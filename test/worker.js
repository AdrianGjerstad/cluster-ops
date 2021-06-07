const clusterOps = require("../lib/cluster-ops.js");

let w = new clusterOps.Worker();

w.send({ hello: "world!" }, "test");

process.exit();

