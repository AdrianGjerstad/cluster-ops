const clusterOps = require("../lib/cluster-ops.js");

const c = clusterOps.createCluster({
  workers: 1,
  file: "./worker.js",
  args: []
});

c.on("death", (worker, code, signal) => {
  console.log("onDeath: " + worker.process.pid + ", code " + code + " signal " + signal);
});

c.on("message-test", (worker, msg) => {
  console.log(msg);
});

c.start();

