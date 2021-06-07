const path = require("path");
const cluster = require("cluster");
const EventEmitter = require("events");

const syscalls = require("./syscalls.js");

if (cluster.isPrimary === undefined) {
  // Node < 16.0.0
  cluster.isPrimary = cluster.isMaster;
  cluster.setupPrimary = cluster.setupMaster;
}

class Cluster extends EventEmitter {
  #target_workers;
  #exec;
  #args;
  #env;
  #restart_on_error;
  #restart_on_signal;
  #restart_on_normal_exit;
  #min_restart_age;
  #uid;
  #gid;
  #cwd;

  #workers;

  constructor(opts) {
    super();

    opts = {
      workers: 1,
      file: null,
      args: process.argv.slice(2),
      env: {},
      cwd: process.cwd(),
      restart: {},
      uid: process.geteuid(),
      gid: process.getegid(),
      ...opts
    };

    opts.restart = {
      error: true,
      signal: true,
      normal_exit: false,
      min_age: 500,
      ...opts.restart
    };

    let trace = new Error().stack.split("\n")[3].match(/\(([^:]+):[0-9]+:[0-9]+\)/)[1];

    if (opts.file === null) {
      opts.file = trace;
    } else {
      opts.file = path.resolve(trace.substring(0, trace.lastIndexOf("/")), opts.file);
    }
    
    this.#target_workers = opts.workers;
    this.#exec = opts.file;
    this.#args = opts.args;
    this.#env = opts.env;
    this.#restart_on_error = opts.restart.error;
    this.#restart_on_signal = opts.restart.signal;
    this.#restart_on_normal_exit = opts.restart.normal_exit;
    this.#min_restart_age = opts.restart.min_age;
    this.#uid = opts.uid;
    this.#gid = opts.gid;
    this.#cwd = opts.cwd;

    this.#workers = {};
  }

  /**
   * Get the target number of workers in this cluster.
   *
   * This may be lower than it is supposed to depending on the value of restart
   * options and how long the application has been running.
   */
  get target_workers() {
    return this.#target_workers;
  }

  /**
   * Get the file that new workers spawn in.
   *
   * This value will always be an absolute path, regardless of if it was passed
   * in as such or not.
   */
  get file() {
    return this.#exec;
  }

  /**
   * Set the file that new workers spawn in.
   *
   * NOTE: This does not affect currently running workers.
   */
  set file(f) {
    let trace = new Error().stack.split("\n")[2].match(/\(([^:]+):[0-9]+:[0-9]+\)/)[1];
   
    return this.#exec = path.resolve(trace.substring(0, trace.lastIndexOf("/")), f);
  }

  /**
   * Get the list of command line arguments that each worker will spawn with.
   */
  get args() {
    return this.#args;
  }

  /**
   * Set the list of command line arguments that each worker will spawn with.
   *
   * NOTE: This does not affect already-running processes.
   */
  set args(a) {
    this.#args = a;
  }

  /**
   * Get a dictionary of environment variables that the worker will spawn with.
   */
  get env() {
    return this.#env;
  }

  /**
   * Set the environment variables that the worker will spawn with.
   *
   * NOTE: Some keys may be overridden, such as PWD.
   */
  set env(e) {
    this.#env = e;
  }

  /**
   * Get the starting working directory of workers if they were spun up at the
   * current time.
   */
  get cwd() {
    return this.#cwd;
  }

  /**
   * Set the starting working directory of workers, knowing that they may change
   * it at any time.
   */
  set cwd(d) {
    return this.#cwd = path.resolve(d);
  }

  /**
   * Get whether or not workers will be restarted if they die due to errors.
   */
  get restart_on_error() {
    return this.#restart_on_error;
  }

  /**
   * Set whether or not workers will be restarted if they die due to errors.
   */
  set restart_on_error(b) {
    this.#restart_on_error = b;
  }

  /**
   * Get whether or not workers will be restarted if they terminate due to a
   * signal.
   */
  get restart_on_signal() {
    return this.#restart_on_signal;
  }

  /**
   * Set whether or not workers will be restarted if they terminate due to a
   * signal.
   */
  set restart_on_signal(b) {
    this.#restart_on_signal = b;
  }

  /**
   * Get whether or not workers will be restarted if they exit with code 0.
   */
  get restart_on_normal_exit() {
    return this.#restart_on_normal_exit;
  }

  /**
   * Set whether or not workers will be restarted if they exit with code 0.
   */
  set restart_on_normal_exit(b) {
    this.#restart_on_normal_exit = b;
  }

  /**
   * Get the minimum age a worker must have lived before it may be restarted.
   * This is to prevent spamming of instantly dying worker processes.
   */
  get min_restart_age() {
    return this.#min_restart_age;
  }

  /**
   * Set the minimum age a worker must have lived before it may be restarted.
   */
  set min_restart_age(a) {
    this.#min_restart_age = a;
  }
  
  /**
   * Get the effective user identifier of newly spun-up workers.
   */
  get uid() {
    return this.#uid;
  }

  /**
   *  Set the effective user identifier of newly spun-up workers.
   *
   *  NOTE: This does NOT change the execution privileges of already-running
   *  workers. Those must be restarted in order to take on the change.
   */
  set uid(n) {
    this.#uid = n;
  }

  /**
   * Get the effective group identifier of newly spun-up workers.
   */
  get gid() {
    return this.#gid;
  }

  /**
   * Set the effective group identifier of newly spun-up workers.
   *
   * NOTE: This does not change access privileges of already-running workers.
   * Those must be restarted in order to take on the change.
   */
  set gid(n) {
    this.#gid = n;
  }

  /**
   * Get the number of currently running workers.
   */
  get running_workers() {
    return Object.keys(this.#workers).length;
  }
  
  /**
   * Start this cluster by spawning and initializing all workers.
   */
  start() {
    for (let i = 0; i < this.#target_workers; ++i) {
      this.#spawnWorker();
    }
  }

  /**
   * Stop the cluster.
   */
  stop() {
    let keys = Object.keys(this.#workers);

    for (let i = 0; i < keys.length; ++i) {
      this.#terminateWorker(keys[i]);
    }

    return new Promise((resolve, reject) => {
      process.nextTick(() => {
        if (Object.keys(this.#workers).length === 0) {
          resolve();
        }
      });
    });
  }

  /**
   * Spin up x more workers.
   */
  spinUp(x) {
    this.#target_workers += x;

    for (let i = 0; i < x; ++i) {
      this.#spawnWorker();
    }
  }

  /**
   * Spin down, or remove, x workers from the cluster.
   *
   * Use of `force` is not recommended, since it can cut off connections
   * prematurely if using a bad worker implementation.
   */
  spinDown(x, force=false) {
    this.#target_workers -= x;

    if (force) {
      let keys = Object.keys(this.#workers);

      for (let i = 0; i < x; ++i) {
        this.#terminateWorker(keys[i]);
      }
    }
  }

  /**
   * Refreshes the number of workers and spins more up if it is dwindling at
   * all.
   */
  refreshWorkers() {
    // Find how many more processes must be spun up
    let more_processes = this.#target_workers - this.running_workers;
    
    for (let i = 0; i < more_processes; ++i) {
      this.#spawnWorker();
    }
  }
  
  /**
   * Terminate a worker process by communicating ending conditions. This will
   * allow the process to perform any clean up that is necessary without just
   * sending it a SIGKILL.
   */
  #terminateWorker(WORKER_ID) {
    let worker = this.#workers[WORKER_ID];
    worker.removeAllListeners("exit");  // Don't try to restart it.

    worker.send({
      type: null,
      system: syscalls.EXIT,
      WORKER_ID
    });

    worker.on("exit", (code, signal) => {
      if (signal || code !== 0) {
        // Worker did not die normally
        this.emit("death", worker, code, signal);
      }
      
      delete this.#workers[WORKER_ID];
    });
  }

  #spawnWorker() {
    cluster.setupPrimary({
      exec: this.#exec,
      args: this.#args,
      cwd: this.#cwd,
      serialization: "advanced",
      silent: true,
      uid: this.#uid,
      gid: this.#gid
    });

    let WORKER_ID = Math.floor(new Date().getTime()/1000).toString(16).padStart(8, '0');
    for (let i = 0; i < 8; ++i) {
      WORKER_ID += Math.floor(Math.random()*0x100).toString(16).padStart(2, '0');
    }

    let worker = cluster.fork({
      WORKER_ID,
      ...this.#env
    });
    
    worker.WORKER_ID = WORKER_ID;
    worker.BIRTH_TIME = new Date();
    this.#workers[WORKER_ID] = worker;

    worker.on("message", this.#onMessage.bind(this, worker));
    worker.on("exit", this.#onExit.bind(this, worker));

    return WORKER_ID;
  }
  
  /**
   * Called whenever this cluster receives a message from a worker.
   *
   * @private
   */
  #onMessage(worker, msg) {
    if (msg.type === null) {
      this.#onSystemMessage(worker, msg);
    } else if (msg.type === undefined) {
      this.emit("message", worker, msg);
    } else {
      this.emit("message-" + msg.type, worker, msg.message);
    }
  }

  /**
   * Called whenever this cluster receives a system message from a worker.
   *
   * @private
   */
  #onSystemMessage(worker, msg) {
    if (msg.call === syscalls.SPIN_UP) {
      this.spinUp(msg.count);
    } else if (msg.call === syscalls.SPIN_DOWN) {
      this.spinDown(msg.count);
    }
  }

  /**
   * Called whenever a worker dies in this cluster.
   *
   * @private
   */
  #onExit(worker, code, signal) {
    worker.DEATH_TIME = new Date();
    delete this.#workers[worker.WORKER_ID];
    this.emit("death", worker, code, signal);
    
    let age = worker.DEATH_TIME - worker.BIRTH_TIME;
    
    // Do we actually want to bother starting up a new worker?
    if (Object.keys(this.#workers).length < this.#target_workers) {
      if (signal) {
        if (age >= this.#min_restart_age && this.#restart_on_signal) {
          let new_WORKER_ID = this.#spawnWorker();
          this.emit("restart", worker, this.#workers[new_WORKER_ID], code, signal);
        }
      } else if (code !== 0) {
        if (age >= this.#min_restart_age && this.#restart_on_error) {
          let new_WORKER_ID = this.#spawnWorker();
          this.emit("restart", worker, this.#workers[new_WORKER_ID], code, signal);
        }
      } else {
        if (age >= this.#min_restart_age && this.#restart_on_normal_exit) {
          let new_WORKER_ID = this.#spawnWorker();
          this.emit("restart", worker, this.#workers[new_WORKER_ID], code, signal);
        }
      }
    }
  }
}

module.exports = Cluster;

