import Logger from "./Logger";
import { BaseBolt, NStormSettings } from "./BaseBolt";
import { BaseQueue } from "./queue-providers/BaseQueue";
import AsyncQueue from "./queue-providers/AsyncQueue";
import BullMQQueue from "./queue-providers/BullMQQueue";
import SimpleQueue from "./queue-providers/SimpleQueue";

type DeepPartial<T> = {
    [P in keyof T]?: T[P] extends object ? DeepPartial<T[P]> : T[P];
};

/** Drop keys explicitly set to undefined so they cannot mask a default */
function defined<T extends object>(obj: T | undefined): Partial<T> {
    const out: Partial<T> = {};
    for (const key of Object.keys(obj ?? {}) as Array<keyof T>) {
        const value = (obj as T)[key];
        if (value !== undefined) out[key] = value;
    }
    return out;
}

export class nStorm {
    public settings: NStormSettings;
    public blocks: BaseBolt[] = [];
    public queue: BaseQueue;

    private isSetup = false;
    private isStarted = false;

    constructor(options?: DeepPartial<NStormSettings>) {
        const defaults: NStormSettings = {
            cloudName: "stormcloud",
            queue: "bullmq",
            debug: false,
            redis: {
                port: 6379,
                host: "127.0.0.1",
            },
            replay: true,
            replayLimit: 3,
            replayTime: 300,
        };

        this.settings = {
            ...defaults,
            ...defined(options),
            redis: { ...defaults.redis, ...defined(options?.redis) },
        } as NStormSettings;

        if (this.settings.debug) {
            Logger.setLevel("debug");
        }

        switch (this.settings.queue) {
            case "simple":
                this.queue = new SimpleQueue(this.settings);
                break;
            case "bullmq":
                this.queue = new BullMQQueue(this.settings);
                break;
            case "async":
                this.queue = new AsyncQueue(this.settings);
                break;
            default:
                throw new Error(
                    `Unknown queue provider "${String(this.settings.queue)}". Use "bullmq", "simple" or "async"`,
                );
        }
    }

    /**
     * Wire the blocks together and register them with the queue provider.
     * Call once, after every block has been added. start() calls this for you
     * if you have not.
     */
    async setupTopology(): Promise<void> {
        if (this.isSetup) {
            throw new Error("setupTopology() has already been called");
        }

        // Validate every input before touching anything, so a bad topology
        // leaves no half-wired state behind
        const links: Array<{ source: BaseBolt; target: BaseBolt }> = [];

        for (const block of this.blocks) {
            for (const input of block.inputs) {
                const source = this._findBlock(input.name);

                if (!source) {
                    throw new Error(
                        `For block ${block.name}, could not find its source block ${input.name}`,
                    );
                }

                links.push({ source, target: block });
            }
        }

        this.isSetup = true;

        // Setup subscribers...
        for (const block of this.blocks) block.subscribers = [];

        for (const { source, target } of links) {
            const input = target.inputs.find((i) => i.name === source.name);
            source.subscribers.push({
                name: target.name,
                options: input?.options,
            });
        }

        // Register every queue before any block starts consuming, so a block
        // can always emit to its subscribers
        for (const block of this.blocks) {
            await this.queue.registerBlock(block);
        }

        for (const block of this.blocks) {
            block._init();
        }
    }

    /**
     * Start the topology. Every block's start() is invoked together, so a
     * source block whose start() runs for a long time does not hold up the
     * others. Resolves when they have all returned.
     */
    async start(): Promise<void> {
        if (this.isStarted) {
            throw new Error("start() has already been called");
        }

        if (!this.isSetup) {
            await this.setupTopology();
        }

        this.isStarted = true;

        if (this.settings.debug) {
            Logger.debug("Starting cloud");
        }

        // Call the queue provider's start hook
        await this.queue.start();

        // Now call the start methods of all the blocks
        await Promise.all(this.blocks.map(async (block) => block.start()));

        if (this.settings.debug) {
            Logger.debug("Cloud started");
        }
    }

    /**
     * Stop the topology: calls every block's stop() hook, then stops consuming
     * and closes every connection the queue provider opened. nStorm installs
     * no signal handlers - call this from your own SIGINT/SIGTERM handler.
     */
    async stop(): Promise<void> {
        if (this.settings.debug) {
            Logger.debug("Stopping cloud");
        }

        const results = await Promise.allSettled(
            this.blocks.map(async (block) => block.stop()),
        );

        for (const result of results) {
            if (result.status === "rejected") {
                Logger.error("Error stopping block; ", result.reason);
            }
        }

        await this.queue.close();
    }

    /**
     * Delete any messages still queued for this topology. Call after
     * setupTopology(), which is when the queues become known.
     */
    async reset(): Promise<void> {
        await this.queue.clear();
    }

    /**
     * Add a block to the topology
     *
     * @param name The name of this block, use a unique name
     * @param block The implementation of the block
     * @param limit maximum number of messages the block processes at one time
     * @returns The block (for chaining .input() calls)
     */
    addBlock(name: string, block: BaseBolt, limit?: number): BaseBolt {
        if (this.isSetup) {
            throw new Error(
                `Cannot add block "${name}": the topology has already been set up`,
            );
        }

        const existing = this._findBlock(name);
        if (existing) {
            throw new Error(
                "A block already exists with this name. Block names must be unique",
            );
        }

        block.setup({ name, limit, debug: this.settings.debug }, this);

        this.blocks.push(block);

        return block;
    }

    _findBlock(name: string): BaseBolt | null {
        return this.blocks.find((block) => block.name === name) ?? null;
    }
}

export default nStorm;
