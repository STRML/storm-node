'use strict';
var mocha = require('mocha');
var spawn = require('child_process').spawn;
var path = require('path');
var assert = require('assert');
var fs = require('fs');

var handshakeMsg = {
  "conf": {
      "topology.message.timeout.secs": 3
  },
  "context": {
      "task->component": {
          "1": "example-spout",
          "2": "__acker",
          "3": "example-bolt"
      },
      "taskid": 3
  },
  "pidDir": "pids"
};

var testInput = [
  {
    id: "0",
    tuple: ['This sentence should be split']
  },
  {
    id: "1",
    tuple: ['and so should this.']
  },
  // 0.9.3
  {
    "id": "-6955786537413359385",
    "comp": "1",
    "stream": "__heartbeat",
    "task": -1,
    "tuple": []
  }
];

describe('Basic splitSentenceBolt tests' , function() {

  var ssb;
  it('Should spawn successfully', function(done) {
    ssb = spawn(
      'node',
      ['splitsentence'],
      {
        cwd: path.resolve(process.cwd(), "examples"),
        stdio: 'pipe'
      }
    );

    // Ensure we can see errors coming out of this for debugging.
    ssb.stderr.pipe(process.stdout);

    // Expect a pid msg
    ssb.stdout.once('data', function(data) {
      data = parseMessage(data)[0];
      assert.equal(data.pid, ssb.pid, "PID should be in message and equal to " + ssb.pid);
      var pidPath = path.resolve(process.cwd(), 'examples/' + handshakeMsg.pidDir + '/' + ssb.pid);
      assert(fs.existsSync(pidPath),
        "Bolt should have written a pidfile to " + pidPath);
      fs.unlinkSync(pidPath);
      done();
    });

    ssb.stdin.write(JSON.stringify(handshakeMsg) + "\nend\n");
  });

  it('Should split sentences', function(done) {

    // Setup listener on bolt stdout.
    var index = 0;
    var splitWords = Array.prototype.concat.apply([], testInput.map(function(msg){
      return msg.tuple[0] ? msg.tuple[0].split(' ') : [];
    }));
    var ackSent = false;
    var syncSent = false;
    ssb.stdout.on('data', function(data) {
      data = parseMessage(data);

      // For each messages that comes through, check it.
      data.forEach(function(datum){

        if (datum.command === 'emit') {
          // Verify word is correct.
          assert.equal(datum.tuple[0], splitWords[index], "Output tuple correctly contains a split sentence.");
          // Verify anchors are properly set.
          assert.equal(datum.anchors[0], (index > 4 ? 1 : 0));
          index++;
        }

        else if (datum.command === 'ack') {
          // Ensure an ack was sent at some point.
          assert(datum.id === "0" || datum.id === "1");
          ackSent = true;
        }

        else if (datum.command === 'sync') {
          syncSent = true;
          finish();
        }

        else {
          throw new Error("Bolt threw an unexpected message: " + JSON.stringify(datum));
        }
      });
    });

    // Send input down to the bolt.
    testInput.forEach(function(msg) {
      ssb.stdin.write(JSON.stringify(msg) + "\nend\n");
    });

    function finish() {
      assert(ackSent);
      assert(syncSent);
      assert(index === splitWords.length);
      done();
    }

  });
});


function parseMessage(msg) {
  var msgs = require('../lib/index').Storm.prototype._readMessage(msg.toString());
  return msgs.map(JSON.parse);
}

