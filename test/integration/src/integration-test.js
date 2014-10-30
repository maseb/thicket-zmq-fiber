/*global require: false, module: false */
"use strict";

var mod = function(
  _,
  Promise,
  App,


  TestSendAndReceive
  ) {

  var IntegrationTest = function() {
    this.initialize.apply(this, arguments);
  };

  _.extend(IntegrationTest.prototype, App.prototype, {
    initialize: function() {
      App.prototype.initialize.apply(this, arguments);
    },
    up: Promise.method(function() {
      return this._runTests();
    }),



    _runTests: Promise.method(function() {
      return Promise
        .bind(this)
        .then(function() {
          return (new TestSendAndReceive(this.config("sendAndReceive"))).run();
        });
    })
  });

  return IntegrationTest;
};

module.exports = mod(
  require("underscore"),
  require("bluebird"),
  require("thicket").c("app"),
  require("./tests/test-send-and-receive")
);
