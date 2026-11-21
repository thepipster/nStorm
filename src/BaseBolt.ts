import Logger from "./Logger";
import type nStorm from "./nStorm";
import { DoneCallback, once } from "./queue-providers/BaseQueue";

export interface Subscriber {
    name: string;
    options?: InputOptions;
}

export interface InputOptions {
    /**
     * Only deliver messages where every key listed here is present on the
     * message and strictly equal to the given value.
     */
    filter?: Record<string, unknown>;
}

export interface InputEntry {
    name: string;
    options?: InputOptions;
}

export interface BlockSetupOpts {
    name: string;
    limit?: number;
    debug?: boolean;
}

export interface NStormSettings {
    /** Namespace for this topology's queues */
    cloudName: string;
    /** Queue provider */
    queue: "bullmq" | "async" | "simple";
    /** Log nStorm's internals */
    debug: boolean;
    redis: {
        port: number;
        host: string;
        password?: string;
        /** Redis ACL username */
        user?: string;
        /** Prefix for every Redis key nStorm writes */
        prefix?: string;
    };
    /** Redeliver a message whose block failed */
    replay: boolean;
    /** Total number of times a message is delivered before it is dropped */
    replayLimit: number;
    /** Milliseconds to wait before redelivering a failed message */
    replayTime: number;
    /**
     * Fail a message whose block has not finished it within this many
     * milliseconds. 0 (the default) waits forever.
     */
    processTimeout: number;
}

/**
 * The base class for a bolt / spout
 */
export class BaseBolt {
    public inputs: InputEntry[] = [];
    public subscribers: Subscriber[] = [];
    public limit: number = 1;
    public context: nStorm | null = null;
    public debug: boolean = false;
    public name: string = "";

    setup(opts: BlockSetupOpts, context: nStorm): void {
        this.name = opts?.name ?? "";
        this.context = context;
        this.limit = opts?.limit ?? 1;
        this.debug = opts?.debug ? true : false;
    }

    /**
     * Register this block's process() method as the consumer of its queue.
     * A message fails (and is replayed, if the provider supports it) when
     * process() calls done(err), throws, or returns a promise that rejects.
     */
    // @spec BOLT-PROCESS-001, BOLT-PROCESS-002, BOLT-PROCESS-004
    _init(): void {
        if (!this.context) {
            throw new Error(
                `Block "${this.name}" has not been added to a topology`,
            );
        }

        const timeoutMs = this.context.settings.processTimeout;

        this.context.queue.process(
            this.name,
            this.limit,
            (job: unknown, done: DoneCallback) => {
                let timer: NodeJS.Timeout | undefined;

                const finish = once((err) => {
                    clearTimeout(timer);
                    done(err);
                });

                const fail = (e: unknown): void => {
                    Logger.error(`Error in block "${this.name}"; `, e);
                    finish(e instanceof Error ? e : new Error(String(e)));
                };

                // We cannot cancel the block's work, only stop waiting for
                // it: the message fails and its slot is freed, and whatever
                // process() does afterwards is ignored.
                if (timeoutMs > 0) {
                    timer = setTimeout(() => {
                        fail(
                            new Error(
                                `Block "${this.name}" did not finish a message within ${timeoutMs}ms`,
                            ),
                        );
                    }, timeoutMs);
                }

                try {
                    Promise.resolve(this.process(job, finish)).catch(fail);
                } catch (e) {
                    fail(e);
                }
            },
        );
    }

    delay(ms: number): Promise<string> {
        return new Promise((resolve) => {
            setTimeout(() => {
                return resolve("done");
            }, ms);
        });
    }

    /**
     * Does this message pass the given filter? True when there is no filter,
     * or when every key in the filter is present on the message with a
     * strictly equal value.
     */
    // @spec BOLT-FILTER-001, BOLT-FILTER-002
    _isFiltered(
        msg: Record<string, unknown>,
        filter?: Record<string, unknown>,
    ): boolean {
        if (!filter) return true;

        return Object.keys(filter).every(
            (key) =>
                Object.prototype.hasOwnProperty.call(msg, key) &&
                msg[key] === filter[key],
        );
    }

    /**
     * Send a message to every block subscribed to this one.
     *
     * The returned promise resolves once every subscriber's queue has accepted
     * the message, and rejects if any of them could not. Awaiting it is
     * optional: a failure is always logged, and ignoring the promise will not
     * cause an unhandled rejection (ADR-0003).
     */
    // @spec BOLT-EMIT-001, BOLT-EMIT-002, BOLT-EMIT-003, BOLT-EMIT-004
    emit(message: Record<string, unknown>): Promise<void> {
        const result = this._emit(message);

        result.catch((err) => {
            Logger.error(`Block "${this.name}" failed to emit; `, err);
        });

        return result;
    }

    private async _emit(message: Record<string, unknown>): Promise<void> {
        const context = this.context;

        if (!context) {
            throw new Error(
                `Block "${this.name}" has not been added to a topology`,
            );
        }

        const targets = this.subscribers.filter((sub) =>
            this._isFiltered(message, sub.options?.filter),
        );

        await Promise.all(
            targets.map(async (sub) => context.queue.emit(sub.name, message)),
        );
    }

    /**
     * Handle a message. Override this in your block, and call done() exactly
     * once when finished - done(err) to mark the message as failed.
     */
    // @spec BOLT-PROCESS-003
    async process(_message: unknown, done: DoneCallback): Promise<void> {
        // Override in subclass. The default just acknowledges the message so
        // that source-only blocks never hold a queue slot.
        done();
    }

    /**
     * Called when the topology starts. Override to begin emitting data or to
     * do any initialisation.
     */
    async start(): Promise<void> {
        // Override in subclass
    }

    /**
     * Called by nStorm.stop(). Override to clear timers, close connections
     * and stop emitting.
     */
    async stop(): Promise<void> {
        // Override in subclass
    }

    /**
     * This function sets up the input for a block.
     */
    // @spec BOLT-INPUT-001
    input(sourceName: string, options?: InputOptions): this {
        this.inputs.push({
            name: sourceName,
            options,
        });

        // Deduplicate by name
        const seen = new Set<string>();
        this.inputs = this.inputs.filter((inp) => {
            if (seen.has(inp.name)) return false;
            seen.add(inp.name);
            return true;
        });

        return this;
    }
}

export default BaseBolt;
