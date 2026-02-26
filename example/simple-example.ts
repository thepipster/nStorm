import { Logger, nStorm, BaseBolt } from '../src';

/**
 * Demo spout that generates random data
 */
class CoinSpout extends BaseBolt {
  async start(): Promise<void> {
    const sendData = (): void => {
      const test = Math.floor(Math.random() * 101);
      const toss = test < 50 ? 'heads' : 'tails';
      const row = { coin: toss, time: Date.now() };

      this.emit(row);

      setTimeout(() => {
        sendData();
      }, 1000);
    };

    sendData();
  }
}

class HeadsBolt extends BaseBolt {
  async process(
    message: Record<string, unknown>,
    done: (err?: Error | null) => void
  ): Promise<void> {
    Logger.info('Heads ', message.coin as string);

    // Pass data along
    this.emit(message);

    // Acknowledge
    done();
  }
}

// Spout and Bolt implementation
const coinTossSpout = new CoinSpout();
const headsBolt = new HeadsBolt();

const cloud = new nStorm({
  debug: false,
  redis: {
    port: 6379,
    host: '127.0.0.1',
  },
});

async function main(): Promise<void> {
  // Setting up topology using the topology builder
  cloud.addBlock('coindTossSpout', coinTossSpout);
  cloud.addBlock('headsBolt', headsBolt).input('coindTossSpout');

  // Setup cluster, and run topology...
  await cloud.setupTopology();
  await cloud.start();
}

main().catch((err) => Logger.error(err));
