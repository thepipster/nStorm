import { NStormSettings } from "../BaseBolt";

/** How long close() waits for messages in progress, unless told otherwise */
export const DEFAULT_CLOSE_TIMEOUT = 30_000;

export type DoneCallback = (err?: Error | null) => void;

export type QueueCallback = (job: unknown, done: DoneCallback) => void;

/**
 * Wrap a callback so that only its first invocation has any effect. Block
 * authors may call `done` late, twice or from both a callback and a rejected
 * promise; providers must only ever see it once.
 */
// @spec QUEUE-DONE-001
export function once(fn: DoneCallback): DoneCallback {
    let called = false;
    return (err?: Error | null) => {
        if (called) return;
        called = true;
        fn(err);
    };
}

/**
 * Base class for a queue provider
 */
export class BaseQueue {
    protected settings: NStormSettings;

    // @spec QUEUE-CONNECT-001
    constructor(opts: NStormSettings) {
        this.settings = opts;
    }

    /**
     * The name of the queue feeding a block. Always includes the cloud name so
     * that topologies sharing a backend cannot see each other's messages.
     */
    // @spec QUEUE-NAME-001
    protected queueName(blockName: string): string {
        return `${this.settings.cloudName}-${blockName}`;
    }

    /**
     * How many times a message may be delivered in total, i.e. 1 when replay
     * is turned off.
     */
    // @spec QUEUE-REPLAY-001, QUEUE-REPLAY-002
    protected get maxAttempts(): number {
        if (!this.settings.replay) return 1;
        return Math.max(1, this.settings.replayLimit);
    }

    /**
     * Hook called by nStorm when a block is registered in the topology
     */
    async registerBlock(_block: { name: string }): Promise<void> {
        // Override in subclass
    }

    /**
     * Hook called by nStorm when the topology is setup but before the start() method
     */
    async start(): Promise<void> {
        // Override in subclass
    }

    /**
     * Called by a Spout/Bolt internally to send a message to a specific block.
     * Resolves once the message has been accepted by the queue (not once it
     * has been processed) and rejects if it could not be enqueued.
     */
    // @spec QUEUE-EMIT-001, QUEUE-EMIT-002
    async emit(
        _blockName: string,
        _message: Record<string, unknown>,
    ): Promise<void> {
        // Override in subclass
    }

    /**
     * Called internally by a block to register its message handler
     */
    process(
        _blockName: string,
        _concurrency: number,
        _callback?: QueueCallback,
    ): void {
        // Override in subclass
    }

    /**
     * Called internally by nStorm to clear out any residual queue data
     */
    // @spec QUEUE-CLEAR-001
    async clear(): Promise<void> {
        return;
    }

    /**
     * Called by nStorm.stop(). Stop taking messages, give the ones in
     * progress up to `timeoutMs` to finish, then release every connection,
     * worker and timer this provider opened (ADR-0004). Safe to call more
     * than once. Providers never handle process signals themselves (ADR-0001).
     */
    // @spec QUEUE-CLOSE-001, QUEUE-CLOSE-002, QUEUE-CLOSE-003, QUEUE-SIGNAL-001
    async close(_timeoutMs: number = DEFAULT_CLOSE_TIMEOUT): Promise<void> {
        return;
    }

    /**
     * Resolve true as soon as `isDone()` holds, or false once `timeoutMs` has
     * passed without it holding.
     */
    protected async waitUntil(
        isDone: () => boolean,
        timeoutMs: number,
    ): Promise<boolean> {
        const deadline = Date.now() + timeoutMs;

        while (!isDone()) {
            if (Date.now() >= deadline) return false;
            await new Promise((resolve) => setTimeout(resolve, 10));
        }

        return true;
    }
}

export default BaseQueue;
