const cluster = require("cluster");
const EventEmitter = require("events");

const syscalls = require("./syscalls.js");

class Worker extends EventEmitter {
  static #has_been_created;

  constructor() {
    super();

    if (Worker.#has_been_created) {
      throw new Error("[cluster-ops] Worker instance has already been created in this process");
    }

    Worker.#has_been_created = true;

    process.on("message", this.#onMessage.bind(this));

    // Create reliable one-time exit hooks for this worker on signals
    for (let i = 0; i < Worker.CATCH_SIGNALS.length; ++i) {
      process.on(`SIG${Worker.CATCH_SIGNALS[i]}`, this.#onExit.bind(this, null, {
        signal: `SIG${Worker.CATCH_SIGNALS[i]}`
      }, true));
    }

    // If we lose the IPC channel, this worker is as good as dead.
    process.on("disconnect", this.#onExit.bind(this, null, {
      disconnected: true
    }, true));

    process.on("uncaughtExceptionMonitor", this.#onExit.bind(this, null, {
      error: true
    }, false));
  }

  send(message, type=undefined) {
    process.send({ type, message });
  }

  #onMessage(msg) {
    if (msg.type === null) {
      this.#onSystemMessage(msg);
    } else if (msg.type === undefined) {
      this.emit("message", msg);
    } else {
      this.emit("message-" + msg.type, msg.message);
    }
  }
  
  #onExit(msg, opts, exit, ...args) {
    if (exit) {
      this.once("exit", () => {
        process.exit(0);
      });
    }
    
    this.emit.apply(this, ["exit", opts, ...args]);
  }

  #onSystemMessage(msg) {
    if (msg.call === syscalls.EXIT) {
      this.#onExit(msg, {
        requested: true
      }, true);
    }
  }
}

Worker.CATCH_SIGNALS = [
  "HUP", "INT", "QUIT", "TERM", "ABRT", "BREAK"
];

module.exports = Worker;

