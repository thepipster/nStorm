# nStorm

A simple real-time processing system inspired by [Storm](http://storm.incubator.apache.org/) and [SimpleStorm](https://www.npmjs.org/package/simplestorm).

## Overview

The motivation for this work is to enable the easy creation of processing blocks that can be chained together in flexibile ways, coupled with the ability to scale by adding additional servers (workers).

The high-level objectives are;

1. Ability to create simple processing blocks that can be easily chained together (create topologies).
2. Easily scale by adding new servers/workers, ideally automatically.
3. Automatic replay of messages. If a process dies, then re-send message to another worker.
4. Robust. This is achieved through using Node's [cluster](http://nodejs.org/api/cluster.html) module and running workers in child processes that are restarted upon failure.

### Dependencies

To enable simple cross-server communications and replay of messages I made use of Redis, which is used for both a message store and also a pub-sub mechanism.

## Installation

```js
npm install nstorm
```

## Usage

Include nStorm by

```js
var nStorm = require('nstorm');
```

In Twitter Storm, you can create two types of blocks;

1. *Spouts* are data sources, and must contain a start function that `emits` data.
2. *bolts* Are chainable processing blocks, that must contain a process function.

nStorm uses the same concept, except we only have one type of block and they can have a `process` *and* a `start` method, thereby allowing hybrid blocks.

The `start` method can be used to emit data, i.e. a data source, or can be used for any initialization code a block needs.

### Creating a Data source

Data source blocks are fairly easy to create, using the `BaseBolt` base class. The only restriction is that the block must contain a `start()` function that returns a promise. To output any messages, call the `this.emit(data)`.

The data you pass should be an Array or Object.

The following block demonstrates a coin toss, it randomly 'emits' a head or a tails, and emits that as an object.

```js
/**
 * Demo spout that randomly creates 'coins', either heads or tails
 */
class CoinSpout extends BaseBolt {

    async start() {

        Logger.info("Starting...");

        for (var i=0; i<1000; i++){

            var test = getRandomInt(0,100);

            var toss = 'tails';
            if (test < 50){
                toss = 'heads';
            }

            var row = {coin: toss, count: i, time: Date.now(), deltaSpout: getDelta()}

            Logger.debug("Emitting", row);

            // Use the utility delay function to wait 3 seconds
            await this.delay(3000)

            // Send the data to any block that is consuming it
            this.emit(row);
            
        }

        return

    }


}
```

### Creating a Process Block

Creating a processing block is also pretty straight forward, and also extends from the `BaseBolt` class. The only restriction is that they must contain a `process` function. This function is passed a message and a `done` function that is called when the message has been processed. The message will be whatever data was emitted by a block that this block is connected to. So for example, if this was listening to the spout we just created then the message would be the result of the coin toss.

The block can also send data using the `this.emit()` function, which allows you to pass data to other blocks. 

```js
class HeadsBolt extends BaseBolt {

    async process(message, done) {

        message.deltaHeads = Date.now() - message.time
        //Logger.info(`Heads >>>>> ${message.coin} - message.count = ${message.count}`);

        // Pass data along
        this.emit(message);

        // Acknowledge
        done()

    }

}
```

### Queue Providers

The default implementation uses the awesome [kue](https://github.com/Automattic/kue) under the hood. This creates a very robust system, but with some performance trade-offs. If you have a relatively simple implenetation with small jobs, then you can use the in memory 'async' implementation (uses `async.queue`).

Support for [featureless-job-queue](https://github.com/Neamar/featureless-job-queue) is coming soon.

### Setting up a toplogy

Once you have created your blocks, now you can wire them up!

Here is an example based on the bolt and spout we just created

```js
const Logger = require('nstorm').Logger
const nStorm = require('nstorm').nStorm
const BaseBolt = require('nstorm').BaseBolt

var opts = {
    queue:'simple', 
    debug:true
	redis: {
			port: 6379,
			host: '127.0.0.1',
			prefix: 'nstorm-pubsub:'
		}
}

async function test(){

    // Spout and Bolt implementation
    var coinTossSpout = new CoinSpout();
    var headsBolt = new HeadsBolt();
    var tailsBolt = new TailsBolt();
    var resultsBolt = new ResultsBolt();

    var cloud = new nStorm(opts);

    // Setting up topology using the topology builder
    cloud.addBlock("coindTossSpout", coinTossSpout);
    cloud.addBlock("tailsBolt", tailsBolt, 10).input("coindTossSpout", {filter:{coin: "tails"}});
    cloud.addBlock("headsBolt", headsBolt, 5).input("coindTossSpout", {filter:{coin: "heads"}});
    cloud.addBlock("resultsBolt", resultsBolt, 6).input("tailsBolt").input("headsBolt");

    // Setup cluster, and run topology...
    await cloud.setupTopology()
    await cloud.reset()
    await cloud.start()

}

test().then().catch(err=>Logger.error(err))
```

### nStorm Options

When you create a nStorm cloud using `var cloud = new nStorm(<options>);` you can pass in the following options;

Option | Default | Description
--- | --- | ---
cloudName | "stormcloud" | Specify the topology name, used if you plan to run more than one topology.
queue | 'kue' | The queue engine to use, options are 'kue', 'async' and 'featureless'
redis | {port: 6379, host: '127.0.0.1'} | Redis connection object
debug | false | Turns on logging
replayLimit | 3 | The number of times a message is replayed before its considered bad and deleted, i.e. if the same message causes an exception 3 times stop replaying the message!
replayTime | 300 | Time (ms) between checking for failed messages
(deprecated) reset | false | Deprecated, use `nStorm.reset()` instead. This gives you more control of how and when to reset.


#### Parallelism

By default, every time a block gets a message its processing method is called. However, you can restrict the number of instances *per worker* by specifying a limit, e.g.

```js
builder.addBlock("headsBolt", headsBolt, 2).input("coinTossSpout");
```

Here a maximum of 2 instances of the headsBolt will run. Any messages sent to that block will be buffered (using redis) until the block is ready.

Note, for this to work you must call `context.ack(message)` so nStorm knows you've finished processing the message.

#### Chaining inputs

Inputs can be chained, e.g.;

```js
builder.addBlock("resultsBolt", resultsBolt, 1).input("tailsBolt").input("headsBolt");
```

So, here a 'resultsBot' is getting inputs from both a `tailsBolt` and a `headsBolt`.

#### Filtering

Inputs can be filted on a specific key and value, e.g

```js
builder.addBlock("headsBolt", headsBolt, 1).input("coinTossSpout", {coin: "heads"});
```

Here the 'headsBolt' will reject any messages that do not have a "coin" key which equals "heads", i.e. it only accepts coin tosses of heads!

### Adding remote workers

This is in the works, but because the system is built on top of redis this should be basically already possible......

### Monitoring

A web based graphical monitor is in the works, and coming soon. Feel free to bug me if you'd like to see an early version.

## Suggestions

Feel free to contact me at mike@arsenicsoup.com if you want to help or have suggestions.

## Contribution

Contributions are welcome!

Please feel free to [file issues](https://github.com/thepipster/nStorm/issues) and submit [pull requests](https://github.com/thepipster/nStorm/pulls).

## Donate

If you like this, and use it, please consider donating to help support future development.

<a class="coinbase-button" data-code="1f955f58582ddd191e84a8bb8fcd7a77" data-button-style="donation_small" href="https://coinbase.com/checkouts/1f955f58582ddd191e84a8bb8fcd7a77">Donate Bitcoins</a><script src="https://coinbase.com/assets/button.js" type="text/javascript"></script>

## License

The MIT License (MIT)

Copyright (c) <year> <copyright holders>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
