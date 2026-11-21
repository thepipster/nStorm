/**
 * The example from the README: a coin-toss spout feeding a bolt that only
 * receives heads. Uses the default BullMQ provider, so it needs Redis on
 * 127.0.0.1:6379. Run with `npm run example`, stop with Ctrl-C.
 */
import { Logger, nStorm, BaseBolt } from "../src";

/**
 * Demo spout that randomly creates 'coins', either heads or tails
 */
class CoinSpout extends BaseBolt {
    private timer?: NodeJS.Timeout;

    async start(): Promise<void> {
        this.timer = setInterval(() => {
            const toss = Math.random() < 0.5 ? "heads" : "tails";
            void this.emit({ coin: toss, time: Date.now() });
        }, 1000);
    }

    async stop(): Promise<void> {
        clearInterval(this.timer);
    }
}

class HeadsBolt extends BaseBolt {
    async process(
        message: Record<string, unknown>,
        done: (err?: Error | null) => void,
    ): Promise<void> {
        Logger.info("Heads ", message.coin);

        // Pass data along to any subscribers
        await this.emit(message);

        // Acknowledge
        done();
    }
}

const cloud = new nStorm({
    debug: false,
    redis: {
        port: 6379,
        host: "127.0.0.1",
    },
});

async function main(): Promise<void> {
    cloud.addBlock("coinTossSpout", new CoinSpout());
    cloud
        .addBlock("headsBolt", new HeadsBolt())
        .input("coinTossSpout", { filter: { coin: "heads" } });

    await cloud.start();
}

// nStorm leaves process signals to you
for (const signal of ["SIGINT", "SIGTERM"] as const) {
    process.once(signal, () => {
        cloud
            .stop()
            .catch((err) => Logger.error(err))
            .finally(() => process.exit(0));
    });
}

main().catch((err) => {
    Logger.error(err);
    process.exit(1);
});
