import Logger from "./Logger";
import { BaseBolt, NStormSettings } from "./BaseBolt";
import { BaseQueue } from "./queue-providers/BaseQueue";
import AsyncQueue from "./queue-providers/AsyncQueue";
import BullMQQueue from "./queue-providers/BullMQQueue";
import SimpleQueue from "./queue-providers/SimpleQueue";

type DeepPartial<T> = {
    [P in keyof T]?: T[P] extends object ? DeepPartial<T[P]> : T[P];
};

export class nStorm {
    public settings: NStormSettings;
    public blocks: BaseBolt[] = [];
    public queue: BaseQueue;

    constructor(options?: DeepPartial<NStormSettings>) {
        const defaults: NStormSettings = {
            cloudName: "stormcloud",
            queue: "bullmq",
            metaLogging: true,
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
            ...options,
            redis: { ...defaults.redis, ...(options?.redis ?? {}) },
        } as NStormSettings;

        switch (this.settings.queue) {
            case "simple":
                this.queue = new SimpleQueue(this.settings);
                break;
            case "bullmq":
                this.queue = new BullMQQueue(this.settings);
                break;
            default:
                this.queue = new AsyncQueue(this.settings);
                break;
        }
    }

    async setupTopology(): Promise<void> {
        // Setup subscribers...
        for (let i = 0; i < this.blocks.length; i += 1) {
            for (let j = 0; j < this.blocks[i].inputs.length; j += 1) {
                const inputBlock = this._findBlock(
                    this.blocks[i].inputs[j].name,
                );

                if (!inputBlock) {
                    throw new Error(
                        `For block ${this.blocks[i].name}, could not find its source block ${this.blocks[i].inputs[j].name}`,
                    );
                }

                inputBlock.subscribers.push({
                    name: this.blocks[i].name,
                    options: this.blocks[i].inputs[j].options,
                });
            }
        }

        // Initialize the blocks and register with the queue provider
        for (let i = 0; i < this.blocks.length; i += 1) {
            await this.queue.registerBlock(this.blocks[i]);
            this.blocks[i]._init();
        }
    }

    async start(): Promise<void> {
        if (this.settings.debug) {
            Logger.debug("Starting cloud");
        }

        // Call the queue provider's start hook
        await this.queue.start();

        // Now call the start methods of all the blocks
        for (let i = 0; i < this.blocks.length; i += 1) {
            await this.blocks[i].start();
        }

        if (this.settings.debug) {
            Logger.debug("Cloud started");
        }
    }

    async reset(): Promise<void> {
        await this.queue.clear();
    }

    /**
     * Add a block to the topology
     *
     * @param name The name of this block, use a unique name
     * @param block The implementation of the block
     * @param limit maximum number of block functions that can run at one time
     * @returns The block (for chaining .input() calls)
     */
    addBlock(name: string, block: BaseBolt, limit?: number): BaseBolt {
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
        for (let i = 0; i < this.blocks.length; i += 1) {
            if (this.blocks[i].name === name) {
                return this.blocks[i];
            }
        }
        return null;
    }
}

export default nStorm;
