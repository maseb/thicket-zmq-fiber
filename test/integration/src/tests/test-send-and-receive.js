/*global require: false, module: false */
"use strict";

var mod = function(
  _,
  Promise,
  zmq,
  ZmqFiber,
  Options,
  Exchange,
  Runtime,
  Logger,
  CountdownLatch
  ) {

  var TestSendAndReceive = function() {
    this.initialize.apply(this, arguments);
  };

  var Log = Logger.create("TestSendAndReceive");

  _.extend(TestSendAndReceive.prototype, {
    initialize: function(opts) {
      opts = Options.fromObject(opts);
      this._pubAddress1 = opts.getOrError("pub1");
      this._pubAddress2 = opts.getOrError("pub2");
    },
    run: Promise.method(function() {

      var pubAddress1 = this._pubAddress1,
          pubAddress2 = this._pubAddress2,
          addresses   = [pubAddress1, pubAddress2],
          fn = function() {
            return Promise.resolve(addresses);
          },
          refreshInterval = 10000,
          runtime = new Runtime(),
          scheduler = runtime.scheduler(),
          fiber1 = new ZmqFiber({
            pubAddress: pubAddress1,
            addresses: addresses,
            refreshAddressesFn: fn,
            refreshInterval: refreshInterval,
            scheduler: scheduler,

            // Override for testing.
            zmqMonitorInterval: 3
          }),
          fiber2 = new ZmqFiber({
            pubAddress: pubAddress2,
            addresses: addresses,
            refreshAddressesFn: fn,
            refreshInterval: refreshInterval,
            scheduler: scheduler,
            // Override for testing.
            zmqMonitorInterval: 3
          }),
          exchange1 = new Exchange({
            fiber: fiber1,
            runtime: runtime
          }),
          exchange2 = new Exchange({
            fiber: fiber2,
            runtime: runtime
          }),
          mail1 = exchange1.mailbox("one"),
          mail2 = exchange2.mailbox("two"),
          mail11 = exchange1.mailbox("oneone"),
          mail22 = exchange2.mailbox("twotwo");

      mail2.requestReplyChannel().subscribe(function(env) {
        mail2.reply(env.msgId, {
          to: env.from,
          body: { bar: "bar" }
        });
      });


      var defer = function(){
        var res, rej;
        var p = new Promise(function() {
          res = arguments[0];
          rej = arguments[1];
        });

        return {
          promise: p,
          resolve: res,
          reject: rej
        };
      };

      var allConnected = defer(),
          allConnectedLatch = new CountdownLatch(2, allConnected.resolve);

      fiber1.statusChannel().subscribe(function(msg) {
        Log.debug("Fiber1 status message", msg);
        allConnectedLatch.step();
      });

      fiber2.statusChannel().subscribe(function(msg) {
        Log.debug("Fiber2 status message", msg);
        allConnectedLatch.step();
      });



      return Promise
        .bind(this)
        .then(function() {
          Log.debug("Starting fiber 1");
          return fiber1
            .start()
            .then(function() {
              Log.debug("Starting fiber 2");
              return fiber2.start();
            });
        })
        .then(function() {
          return allConnected.promise;
        })
        .then(function() {
          Log.debug("sendAndReceive");
          return mail1
            .sendAndReceive({
              to: "two",
              body: { foo: "foo" }
            })
            .then(function(res) {
              Log.debug("Got response back", res);
            });
        })
    })
  });

  return TestSendAndReceive;
};

module.exports = mod(
  require("underscore"),
  require("bluebird"),
  require("zmq"),
  require("../../../../src/thicket-zmq-fiber"),
  require("thicket").c("options"),
  require("thicket").c("messaging/exchange"),
  require("thicket").c("runtime"),
  require("thicket").c("logger"),
  require("thicket").c("countdown-latch")
);
