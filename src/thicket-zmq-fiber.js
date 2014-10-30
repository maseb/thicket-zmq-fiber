/*global require: false, module: false */
"use strict";

var mod = function(
  _,
  Promise,
  M,
  zmq,
  Options,
  InMemoryFiber,
  Channel,
  CompositeChannel,
  Logger,
  Periodic,
  StateGuard,
  CountdownLatch
  ) {

  var ZmqFiber = function() {
    this.initialize.apply(this, arguments);
  };

  var ZMQ_DEFAULT_TOPIC = "",
      ZMQ_MONITOR_INTERVAL = 250;
  var Log = Logger.create("ZmqFiber");

  _.extend(ZmqFiber.prototype, InMemoryFiber.prototype, {
    initialize: function(opts) {
      InMemoryFiber.prototype.initialize.apply(this, arguments);

      opts = Options.fromObject(opts);
      this._zmqTopic             = ZMQ_DEFAULT_TOPIC;
      this._pubAddress           = opts.getOrError("pubAddress");
      this._currentAddresses     = opts.getOrError("addresses");
      this._nextAddresses        = this._currentAddresses;
      this._refreshAddressesFn   = opts.getOrError("refreshAddressesFn");
      this._refreshInterval      = opts.getOrError("refreshInterval");
      this._scheduler            = opts.getOrError("scheduler");
      this._serde                = opts.getOrElseFn("serde", function() {
        return new JSONSerDe();
      });

      this._zmqMonitorTimeout = opts.getOrElse("zmqMonitorInterval", ZMQ_MONITOR_INTERVAL)

      this._statusChannel  = new Channel({ sentinel: this });
      this._egressChannel  = new Channel({ sentinel: this });
      this._localDispatch  = new Channel({ sentinel: this });
      this._ingressChannel = new CompositeChannel({
        sentinel: this,
        listen:   this._localDispatch
      });

      this._fetchAddressPeriodic  = new Periodic({
        task:      this._refreshAddressesFn,
        interval:  this._refreshInterval,
        scheduler: this._scheduler
      });

      this._pubSubGuard = new StateGuard(["pubReady", "subReady"]);

      _.bindAll(this, "_resolveAddressChanges");

      this._localDispatch.subscribe(this._receive);
      this._ingressChannel.subscribe(this._receive);

      this._fetchAddressPeriodic.egressChannel().subscribe(_.bind(function(msg) {
        if (msg.err) {
          Log.error("Error fetching list of addresses", msg.err);
          return;
        }

        if (msg.result) {
          this._nextAddresses = msg.result;
          this._scheduler.get().runSoon(this._resolveAddressChanges);
        }
      }, this));
    },

    _dispose: function() {
      InMemoryFiber.prototype._dispose.apply(this, arguments);

      if (this._localDispatch) {
        this._localDispatch.dispose();
        this._localDispatch = null;
      }

      if (this._ingressChannel) {
        this._ingressChannel.dispose();
        this._ingressChannel = null;
      }

      if (this._egressChannel) {
        this._egressChannel.dispose();
        this._egressChannel = null;
      }

      if (this._fetchAddressPeriodic) {
        this._fetchAddressPeriodic.dispose();
        this._fetchAddressPeriodic = null;
      }

      if(this._pub) {
        this._pub.close();
        this._pub = null;
      }

      if (this._sub) {
        this._sub.close();
        this._sub = null;
      }
    },


    start: Promise.method(function() {
      return Promise
        .bind(this)
        .then(function() {
          return this._cyclePub();
        })
        .then(function() {
          return this._cycleSub();
        })
        .then(function() {
          return this._bindPub();
        })
        .then(function() {
          return this._connectSubs();
        });
    }),


    stop: Promise.method(function() {
      this._teardownPub();
      this._teardownSub();
    }),


    send: Promise.method(function(opts) {
      this._stateGuard.deny("disposed");

      opts = Options.fromObject(opts);

      var from   = opts.getOrError("from"),
          to     = opts.getOrError("to"),
          body   = opts.getOrError("body"),
          msgId  = opts.getOrError("msgId"),
          mT     = opts.getOrElse("mT"),
          rMsgId = opts.getOrElse("rMsgId"),
          chan   = null;

      var msg = {
        from:   from,
        to:     to,
        body:   body,
        msgId:  msgId,
        mT:     mT,
        rMsgId: rMsgId,
        oFib:   this._identity
      };

      // Local dispatch, skip the round-trip
      if (M.get(this._entities, to)) {
        chan = this._localDispatch;
      } else {
        chan = this._egressChannel;
      }

      _.defer(_.bind(function() {
        chan.publish(this, msg);
      }, this));

      return msgId;
    }),

    statusChannel: function() {
      return this._statusChannel
    },

    toString: function() {
      return "ZmqFiber[id="+this.id()+", pubAddress="+this._pubAddress+"]";
    },

    _teardownPub: function() {
      if (this._pub) {
        Log.debug("Closing pub connection", this.toString());
        this._pub.close();
        this._pub = null;
      }
      this._pubSubGuard.unapply("pubReady");
      this._egressChannel.reset();
    },


    _cyclePub: Promise.method(function() {
      Log.debug("Cycling pub", this.toString());
      this._teardownPub();

      this._pub = zmq.socket("pub");

      this._pub.on("error", _.bind(function(err) {
        Log.error("Pub error", this.toString(), err);
      }, this));

      this._egressChannel.subscribe(_.bind(function(msg) {
        Log.debug("Egress subscriber got message", this.toString(), msg);
        if (this._pubSubGuard.applied("pubReady")) {
          Log.debug("Sending", "from", msg.from, "to", msg.to, this.toString());
          this._pub.send(this._serde.serialize(msg));
        }
      }, this));
    }),


    _teardownSub: function() {
      if (this._sub) {
        Log.debug("Closing sub connection", this.toString());
        this._sub.close();
        this._sub = null;
      }
      this._pubSubGuard.unapply("subReady");
    },


    _cycleSub: Promise.method(function() {
      Log.debug("Cycling sub", this.toString());
      this._teardownSub();
      this._sub = zmq.socket("sub");

      this._sub.on("error", function(err) {
        Log.error("Sub error", this.toString(), err);
      });


      this._sub.on("message", _.bind(function(msg) {
        Log.debug("Sub got message", this.toString(), msg.toString());
        if (this._pubSubGuard.applied("subReady")){
          this._ingressChannel.publish(this, this._serde.deserialize(msg.toString()));
        }
      }, this));
    }),


    _bindPub: Promise.method(function() {
      Log.debug("Binding pub", this.toString());
      var bindAsync = Promise.promisify(this._pub.bind, this._pub);
      return bindAsync(this._pubAddress)
        .bind(this)
        .then(function(any) {
          this._pubSubGuard.apply("pubReady");
          return any;
        });
    }),


    _connectSubs: Promise.method(function() {
      var others = _.without(this._currentAddresses, this._pubAddress);

      Log.debug("Connecting sub", this.toString(), others);


      this._sub.monitor(this._zmqMonitorTimeout);

      var connectLatch = new CountdownLatch(others.length, _.bind(function(err) {
        this._sub.unmonitor();
        if (err) {
          Log.error("Error checking for connection success", err);
          return;
        }

        this._statusChannel.publish(this, {
          mT: "subsConnected",
          addresses: others
        });
      }, this));

      this._sub.on("connect", _.bind(function(val, endpoint) {
        Log.debug("Sub connect event", this.toString(), val, endpoint);
        if (_.contains(others, endpoint)) {
          connectLatch.step();
        }
      }, this));

      _.each(others, function(address) {
        this._sub.connect(address);
      }, this);
      this._sub.subscribe(this._zmqTopic);
      this._pubSubGuard.apply("subReady");
    }),


    _resolveAddressChanges: function() {
      this._fetchAddressPeriodic.stop();

      var changes = _.difference(this._currentAddresses, this._nextAddresses);
      if (changes.length) {
        this._currentAddresses = this._nextAddresses;
        return Promise
          .bind(this)
          .then(function() {
            return this._cycleSub();
          })
          .then(function() {
            return this._connectSubs();
          })
          .caught(function(err) {
            Log.fatal("Error cycling addresses", err);
          })
          .lastly(function() {
            this._fetchAddressPeriodic.start();
          })
      }
    }
  });

  var JSONSerDe = function() {
    return this.initialize.apply(this, arguments);
  };

  _.extend(JSONSerDe.prototype, {
    initialize: function() {},
    serialize: function(msg) {
      return JSON.stringify(msg);
    },
    deserialize: function(msg) {
      return JSON.parse(msg);
    }
  });

  return ZmqFiber;
};

module.exports = mod(
  require("underscore"),
  require("bluebird"),
  require("mori"),
  require("zmq"),

  require("thicket").c("options"),
  require("thicket").c("messaging/fibers/in-memory"),

  require("thicket").c("channel"),
  require("thicket").c("composite-channel"),
  require("thicket").c("logger"),
  require("thicket").c("periodic"),
  require("thicket").c("state-guard"),
  require("thicket").c("countdown-latch")
);