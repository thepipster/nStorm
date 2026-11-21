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
    private stopping: Promise<void> | null = null;

    // @spec TOPO-CONFIG-001, TOPO-CONFIG-002, TOPO-CONFIG-003, TOPO-CONFIG-004
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
            processTimeout: 0,
        };

        this.settings = {
            ...defaults,
            ...defined(options),
            redis: { ...defaults.redis, ...defined(options?.redis) },
        } as NStormSettings;

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
    // @spec TOPO-SETUP-001, TOPO-SETUP-002, TOPO-SETUP-003
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
    // @spec TOPO-START-001, TOPO-START-002, TOPO-START-003, TOPO-START-004
    async start(): Promise<void> {
        if (this.stopping) {
            throw new Error("This topology has been stopped");
        }

        if (this.isStarted) {
            throw new Error("start() has already been called");
        }

        if (!this.isSetup) {
            await this.setupTopology();
        }

        this.isStarted = true;

        if (this.settings.debug) {
            Logger.info("Starting cloud");
        }

        // Call the queue provider's start hook
        await this.queue.start();

        // Now call the start methods of all the blocks. If any of them
        // fails, do not leave the rest running half-started.
        try {
            await Promise.all(this.blocks.map(async (block) => block.start()));
        } catch (err) {
            await this.stop().catch((stopErr) => {
                Logger.error("Error stopping after a failed start; ", stopErr);
            });
            throw err;
        }

        if (this.settings.debug) {
            Logger.info("Cloud started");
        }
    }

    /**
     * Stop the topology: call every block's stop() hook, stop taking
     * messages, wait up to `timeout` milliseconds (default 30 seconds) for
     * the ones in progress to finish, then close every connection the queue
     * provider opened (ADR-0004).
     *
     * Safe to call at any point and more than once; later calls return the
     * first call's promise. A stopped topology cannot be started again.
     *
     * nStorm installs no signal handlers - call this from your own
     * SIGINT/SIGTERM handler (ADR-0001).
     */
    // @spec TOPO-STOP-001, TOPO-STOP-002, TOPO-STOP-003, TOPO-STOP-004
    stop(options?: { timeout?: number }): Promise<void> {
        if (!this.stopping) this.stopping = this.shutdown(options?.timeout);
        return this.stopping;
    }

    private async shutdown(timeout?: number): Promise<void> {
        if (this.settings.debug) {
            Logger.info("Stopping cloud");
        }

        const results = await Promise.allSettled(
            this.blocks.map(async (block) => block.stop()),
        );

        for (const result of results) {
            if (result.status === "rejected") {
                Logger.error("Error stopping block; ", result.reason);
            }
        }

        await this.queue.close(timeout);
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
    // @spec TOPO-SETUP-004, TOPO-SETUP-005
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

    /** @internal */
    _findBlock(name: string): BaseBolt | null {
        return this.blocks.find((block) => block.name === name) ?? null;
    }
}

export default nStorm;
