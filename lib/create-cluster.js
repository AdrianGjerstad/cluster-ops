const Cluster = require("./cluster.js");

module.exports = function createCluster(opts) {
  return new Cluster(opts);
}

