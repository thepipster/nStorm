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

Data source blocks are fairly easy to create, the only restriction is that the block must contain a `start(context)` function. To output any messages, call the `emit(data)` function of the `context` object passed into the start function. 

The data you pass should be an Array or Object.

The following block demonstrates a coin toss, it randomly 'emits' a head or a tails, and emits that as an object.

```js
/**
 * Demo spout that generates random data
 */
function CoinSpout() {

    this.start = function(context) {

        sendData();

        function sendData(){
            
            var test = Math.floor(Math.random() * 101);

            var toss = 'tails';
            if (test < 50){
                toss = 'heads';
            }

            var row = {coin: toss, time: Date.now()}

            // 'Emit' the data, which is passed to any blocks listening to
            // (subscribed) to this blocks messages
            context.emit(row);

            setTimeout(function(){
                sendData();
            }, 1000);

        }

    }


}
```

### Creating a Process Block

Creating a processing block is also pretty straight forward. The only restriction is that they must contain a process function. This function is passed a message and the context oject. The message will be whatever data was emitted by a block that this block is connected to. So for example, if this was listening to the spout we just created then the message would be the result of the coin toss.

The `context` object contains two functions, an `emit()` which allows you to pass data to other blocks. And a `ack(message)`. This is important, the block must call ack on the message otherwise this message will be replayed (sent back to this block)! 

```js
function HeadsBolt() {

    this.process = function(message, context) {

        Logger.info("Heads ", message.coin);


        // Acknowledge
        context.ack(message);

        // Pass data along
        context.emit(message);

    }

}
```

### Setting up a toplogy

Once you have created your blocks, now you can wire them up! 

Here is an example based on the bolt and spout we just created

```js
var Logger = require('arsenic-logger');
var nStorm = require('nstorm');

// Spout and Bolt implementation
var coinTossSpout = new CoinSpout();
var headsBolt = new HeadsBolt();

// Setting up topology using the topology builder
var cloud = new nStorm();

// Setting up topology using the topology builder
cloud.addBlock("coindTossSpout", coinTossSpout);
cloud.addBlock("headsBolt", headsBolt).input("coindTossSpout");

// Setup cluster, and run topology...
cloud.start();
```

### nStorm Options

When you create a nStorm cloud using `var cloud = new nStorm(<options>);` you can pass in the following options;

Option | Default | Description
--- | --- | ---
userCluser | true | Flag to indicate if nSTorm should use Node.js cluster and place each worker in its own child process. When a child worker dies, it is respawned.
redis | {port: 6379, host: '127.0.0.1'} | Redis connection object
debug | false | Turns on logging 
replay | true | Globally turns off replaying messages
replayLimit | 3 | The number of times a message is replayed before its considered bad and deleted, i.e. if the same message causes an exception 3 times stop replaying the message!
replayTime | 300 | Time (ms) between checking for failed messages


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

