/**
 * A larger topology on the in-memory "async" provider - no Redis needed.
 * Run with `npx tsx example/test.ts`, stop with Ctrl-C.
 */
import { Logger, nStorm, BaseBolt } from "../src";

const startTime = new Date();

function getRandomInt(min: number, max: number): number {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

function getDelta(): number {
    return new Date().getTime() - startTime.getTime();
}

/**
 * Demo spout that randomly creates 'coins', either heads or tails
 */
class CoinSpout extends BaseBolt {
    private stopped = false;

    async stop(): Promise<void> {
        this.stopped = true;
    }

    async start(): Promise<void> {
        Logger.info("Starting...");

        for (let i = 0; i < 100; i++) {
            const test = getRandomInt(0, 100);
            const toss = test < 50 ? "heads" : "tails";
            const row = {
                coin: toss,
                count: i,
                time: Date.now(),
                deltaSpout: getDelta(),
            };

            await this.delay(1000);
            if (this.stopped) return;

            await this.emit(row);
        }
    }
}

class HeadsBolt extends BaseBolt {
    async process(
        message: Record<string, unknown>,
        done: (err?: Error | null) => void,
    ): Promise<void> {
        (message as Record<string, unknown>).deltaHeads =
            Date.now() - (message.time as number);
        Logger.info(
            `Heads >>>>> ${message.coin} - message.count = ${message.count}. Delta = ${message.deltaHeads}`,
        );

        await this.emit(message);

        await this.delay(10000);

        done();
    }
}

class TailsBolt extends BaseBolt {
    async process(
        message: Record<string, unknown>,
        done: (err?: Error | null) => void,
    ): Promise<void> {
        (message as Record<string, unknown>).deltaTails =
            Date.now() - (message.time as number);
        Logger.info(
            `Tails >>>>> ${message.coin} - message.count = ${message.count}`,
        );

        await this.emit(message);

        done();
    }
}

class ResultsBolt extends BaseBolt {
    private count = 0;

    async process(
        message: Record<string, unknown>,
        done: (err?: Error | null) => void,
    ): Promise<void> {
        (message as Record<string, unknown>).deltaResults =
            Date.now() - (message.time as number);

        this.count++;
        Logger.info(
            `Results >>>>> ${this.count} coins - message.count = ${message.count}`,
            message,
        );

        await this.emit(message);

        done();
    }
}

async function test(): Promise<void> {
    const coinTossSpout = new CoinSpout();
    const headsBolt = new HeadsBolt();
    const tailsBolt = new TailsBolt();
    const resultsBolt = new ResultsBolt();

    const cloud = new nStorm({ queue: "async", debug: true });

    // Setting up topology using the topology builder
    cloud.addBlock("coindTossSpout", coinTossSpout);
    cloud.addBlock("tailsBolt", tailsBolt, 10).input("coindTossSpout", {
        filter: { coin: "tails" },
    });
    cloud.addBlock("headsBolt", headsBolt, 5).input("coindTossSpout", {
        filter: { coin: "heads" },
    });
    cloud
        .addBlock("resultsBolt", resultsBolt, 6)
        .input("tailsBolt")
        .input("headsBolt");

    process.once("SIGINT", () => {
        cloud
            .stop()
            .catch((err) => Logger.error(err))
            .finally(() => process.exit(0));
    });

    // Setup cluster, clear out anything left over, and run topology...
    await cloud.setupTopology();
    await cloud.reset();
    await cloud.start();
}

test().catch((err) => Logger.error(err));
