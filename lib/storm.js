'use strict';
var util = require('util'),
  fs = require('fs');

var debug = require('./log');

var Storm = function() {};

Storm.prototype = {
  msgCount: 0,
  stormConfig: {},
  topologyContext: {},
  input: process.stdin,
  output: process.stdout,

  /**
   * Override this in a subclass to process a tuple.
   * The log statement below should never be run.
   * @param  {Object} tuple Tuple to process.
   */
  tupleCallback: function(tuple) {
    this.log("Error: No `tupleCallback` defined in this nodejs multilang bolt. Tuple: " + tuple.tuple);
  },

  /**
   * Ack a tuple.
   * @param  {Object} tuple Tuple we're acking.
   */
  ack: function(tuple) {
    this.sendCommand({
      command: 'ack',
      id: tuple.id
    });
  },

  /**
   * Mark a tuple as failed.
   * @param  {Object} tuple Tuple we've failed.
   */
  fail: function(tuple) {
    this.sendCommand({
      command: 'fail',
      id: tuple.id
    });
  },

  /**
   * Send a log message.
   * @param  {String} msg Log message.
   */
  log: function(msg) {
    this.sendCommand({
      command: 'log',
      msg: msg
    });
  },

  /**
   * Given a message, write it to stdout so storm can catch it.
   * Automatically appends '\nend\n'.
   * @param  {String} msg Message to write.
   */
  sendToParent: function(msg) {
    var str = msg + "\nend\n";
    this.output.write(str);
  },

  /**
   * Send a raw command.
   * @param  {Object} command Command to send.
   */
  sendCommand: function(command) {
    debug('sending: ' + JSON.stringify(command));
    this.sendToParent(JSON.stringify(command));
  },

  /**
   * Proxy to `emitTuple` with options syntax.
   * @param  {Tuple}  [anchor] Anchoring tuple.
   * @param  {Array}  tuple Tuple values to emit.
   * @param  {Object} [options] Emit options (stream, anchors, directTask)
   */
  emit: function(anchor, tuple, options) {
    // Anchoring tuple arity
    if (anchor && (anchor.id !== undefined && anchor.id !== null) && anchor.tuple instanceof Array) {
      options = options || {};
      options.anchors = [anchor];
    }
    // values, options arity
    else if (anchor instanceof Array) {
      options = tuple || {};
      tuple = anchor;
    }
    // Bad arity
    else {
      var msg = "Incorrect arguments to `emit`: " + JSON.stringify([].slice.call(arguments));
      this.log(msg);
      throw new Error(msg);
    }
    this.emitTuple(tuple, options.stream, options.anchors, options.directTask);
  },

  /**
   * Helper for compatibility with other libs such as the Python multilang wrapper.
   * @param  {String}  task    Task to send the tuple to.
   * @param  {Array}   tuple   Tuple values to emit.
   * @param  {options} options Emit options (see Storm#emit).
   */
  emitDirect: function(task, tuple, options) {
    options = options || {};
    this.emitTuple(tuple, options.stream, options.anchors, task);
  },

  /**
   * Emit a tuple. More bare-metal function if you need more advanced functionality.
   * @param  {Array}  tuple      Tuple values to emit.
   * @param  {String} stream     The id of the stream this tuple was emitted to.
   * @param  {Array}  anchors    Anchoring tuples. `emitTuple` will loop through them and send their ids.
   * @param  {String} directTask If doing a direct emit, indicate the task to send the tuple to.
   */
  emitTuple: function(tuple, stream, anchors, directTask) {
    var command = {
      command: 'emit'
    };

    if (stream != null) {
      command.stream = stream;
    }

    if (anchors != null && anchors.length && this.mode === 'bolt') {
      command.anchors = anchors.map(function(a) {
        return a.id;
      });
    }

    if (directTask != null) {
      command.task = directTask;
    }

    command.tuple = tuple;

    this.sendCommand(command);
  },

  /**
   * Storm has requested our pid - put a file in the directory chosen by Storm
   * and send it our pid back.
   * @param  {String} dir Directory to store pidfile.
   */
  sendPid: function(dir) {
    var self = this;
    try {
      try{
        fs.mkdirSync(dir);
      } catch(e) {
        // dir might already exist
      }
      fs.writeFileSync(dir + '/' + process.pid);
    } catch (e) {
      //do nothing - issue creating pid file
    }

    process.on('uncaughtException', function(e) {
      var msg = "Uncaught Exception: " + e.toString();
      debug(msg);
      self.log(msg);
    });

    process.on('exit', function() {
      self.output.write("Exiting");
    });

    this.sendCommand({
      pid: process.pid
    });
  },

  /**
   * Given a parseable message, fire the appropriate callback for the message.
   * If it is the first message, set the settings given in the message and send our pid back.
   * Otherwise we are most likely processing a tuple.
   * @param  {String} msg Incoming message.
   */
  parseMessage: function(msg) {
    this.msgCount++;

    // Initial message, contains config. Send pid back.
    if (this.msgCount == 1) {
      var data = JSON.parse(msg);
      this.sendPid(data.pidDir);
      this.stormConfig = data.config;
      this.topologyContext = data.context;
      if (typeof this.onConfig === 'function') this.onConfig(this.stormConfig);
      if (typeof this.onContext === 'function') this.onContext(this.topologyContext);
    } else {
      var cmd = {};

      try {
        cmd = JSON.parse(msg);
      } catch (err) {
        debug("Malformed STDIN: " + msg);
        this.log('MALFORMED' + msg);
        //malformed stdin - error check here
      }

      debug('received:' + msg);

      if (this.mode == 'spout') {
        this.spoutCallback(cmd);
      } else {
        if ('tuple' in cmd) {
          this.tupleCallback(cmd);
        }
      }
    }

  },

  /**
   * Begin reading messages from stdin.
   */
  readMessages: function() {
    var self = this;
    this.input.setEncoding("utf8");

    // Reset internal message state
    this._msgs = [];

    this.input.on('data', function(chunk) {
      var msgs = self._readMessage(chunk);
      msgs.forEach(self.parseMessage.bind(self));
    });

    this.input.resume();
  },

  /**
   * Given a message chunk, check for 'end' delimiters and return an array of parseable strings.
   * @param  {String} chunk Message chunk from stdin.
   * @return {Array}        Array of strings that can be fed to JSON.parse()
   */
  _readMessage: function(chunk) {
    var chunks = chunk.split("\n");
    var out = [];
    var msgs = this._msgs || [], last_end = 0;

    msgs = msgs.concat(chunks);

    // Remove empty messages.
    msgs = msgs.filter(function(msg) {
      return !!msg;
    });

    // Find the 'end' delimiter and prepare to return everything that was sent up to it.
    for (var i in msgs) {
      if (msgs[i] == "end") {
        out.push(msgs.slice(last_end, i).join("\n"));
        last_end = parseInt(i) + 1;
      }
    }

    // Remove the messages that were sent.
    msgs.splice(0, last_end);

    // Return messages that we read.
    return out;
  },

  /**
   * Begin reading messages from stdin. Call this when the bolt is ready to start.
   */
  run: function() {
    this.readMessages();
  }
};

module.exports = Storm;
