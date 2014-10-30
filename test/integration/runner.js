var thicket         = require("thicket"),
    Bootstrapper    = thicket.c("bootstrapper"),
    Logger          = thicket.c("logger"),
    CLA             = thicket.c("appenders/console-log"),
    IntegrationTest = require("./src/integration-test");

Logger.root().addAppender(new CLA());
Logger.root().setLogLevel(Logger.Level.Debug);

var Log = Logger.create("TestRunner");

var b = new Bootstrapper({ applicationConstructor: IntegrationTest });

Log.info("Starting test.");

b
  .bootstrap()
  .then(function(app) {
    return app
      .start()
      .then(function() {
        Log.info("End to end test successful");
      })
      .caught(function(err) {
        Log.error("End to end test failed.");
        return app.stop(err);
      })
      .then(function() {
        Log.info("Test complete.")
      })
      .then(function() {
        return app.stop();
      });
  });

