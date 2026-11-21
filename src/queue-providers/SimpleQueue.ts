import Logger from "../Logger";
import {
    BaseQueue,
    QueueCallback,
    once,
    DEFAULT_CLOSE_TIMEOUT,
} from "./BaseQueue";
import IORedis from "ioredis";

interface Envelope {
    data: Record<string, unknown>;
    attempt: number;
}

interface Poller {
    active: boolean;
    /** Wakes the poll loop when it is waiting for a free concurrency slot */
    wake: (() => void) | null;
    /** Settles when the poll loop has exited */
    finished: Promise<void>;
}

interface PendingReplay {
    redisKey: string;
    envelope: Envelope;
}

/** Seconds a blocking pop waits before the loop re-checks whether to stop */
const POP_TIMEOUT_SECONDS = 1;

/**
 * Lightweight Redis-backed queue using RPUSH/BLPOP. Lower overhead than
 * BullMQ, with weaker guarantees:
 *
 * - A message is removed from Redis when a worker picks it up, so a message
 *   that is being processed (or waiting out its `replayTime` before a retry)
 *   when the process dies is lost. Use BullMQ if that matters (ADR-0002).
 * - A message whose block calls `done(err)`, throws or rejects is put back on
 *   the queue after `replayTime`, up to `replayLimit` deliveries in total
 *   (`replay: false` disables this).
 * - A clean close() loses nothing that it can still reach: messages waiting
 *   to be replayed go straight back to Redis, and messages in progress are
 *   given time to finish (ADR-0004).
 *
 * Honours `redis.host/port/password/user/prefix`. No connection is opened
 * until the first message is emitted or a block starts consuming.
 */
export class SimpleQueue extends BaseQueue {
    private client: IORedis | null = null;
    private blockingClients: IORedis[] = [];
    private queues: Record<string, { redisKey: string }> = {};
    private pollers: Poller[] = [];
    private retryTimers = new Map<NodeJS.Timeout, PendingReplay>();
    private activeJobs = 0;
    private closing: Promise<void> | null = null;
    private closed = false;

    // @spec SIMPLE-REDIS-001
    private connect(): IORedis {
        const { host, port, password, user } = this.settings.redis;

        return new IORedis({
            host,
            port,
            password,
            username: user,
            maxRetriesPerRequest: null,
        });
    }

    // @spec QUEUE-CONNECT-001
    private getClient(): IORedis {
        // Reconnecting after close() would leave a connection nobody closes
        if (this.closed) throw new Error("SimpleQueue has been closed");
        if (!this.client) this.client = this.connect();
        return this.client;
    }

    // @spec SIMPLE-KEY-001
    private redisKey(blockName: string): string {
        const prefix = this.settings.redis.prefix;
        return `${prefix ? prefix + ":" : ""}${this.queueName(blockName)}:sq-jobs`;
    }

    async registerBlock(block: { name: string }): Promise<void> {
        this.queues[this.queueName(block.name)] = {
            redisKey: this.redisKey(block.name),
        };
    }

    private getQueue(blockName: string): { redisKey: string } {
        const q = this.queues[this.queueName(blockName)];

        if (!q) {
            throw new Error(
                `Queue ${this.queueName(blockName)} is not defined!`,
            );
        }

        return q;
    }

    // @spec SIMPLE-ENVELOPE-001
    async emit(
        blockName: string,
        message: Record<string, unknown>,
    ): Promise<void> {
        const q = this.getQueue(blockName);
        await this.push(q.redisKey, { data: message, attempt: 1 });
    }

    private async push(redisKey: string, envelope: Envelope): Promise<void> {
        await this.getClient().rpush(redisKey, JSON.stringify(envelope));
    }

    // @spec SIMPLE-ENVELOPE-002
    private parse(raw: string): Envelope | null {
        try {
            const parsed = JSON.parse(raw);

            if (
                parsed &&
                typeof parsed === "object" &&
                typeof parsed.attempt === "number" &&
                parsed.data &&
                typeof parsed.data === "object"
            ) {
                return parsed as Envelope;
            }

            Logger.error("SimpleQueue dropped a malformed message; ", raw);
        } catch (e) {
            Logger.error("SimpleQueue dropped an unparseable message; ", e);
        }

        return null;
    }

    private replay(redisKey: string, envelope: Envelope): void {
        this.push(redisKey, {
            ...envelope,
            attempt: envelope.attempt + 1,
        }).catch((pushErr) => {
            Logger.error("SimpleQueue replay error:", pushErr);
        });
    }

    // @spec SIMPLE-REPLAY-001, SIMPLE-CLOSE-001
    private handleFailure(
        redisKey: string,
        envelope: Envelope,
        err: Error,
    ): void {
        if (envelope.attempt >= this.maxAttempts) {
            Logger.error(
                `${redisKey}: message dropped after ${envelope.attempt} attempt(s); `,
                err,
            );
            return;
        }

        // While closing there is no time to wait: put it straight back so
        // that another worker, or the next run, picks it up
        if (this.closing) {
            this.replay(redisKey, envelope);
            return;
        }

        const timer = setTimeout(() => {
            this.retryTimers.delete(timer);
            this.replay(redisKey, envelope);
        }, this.settings.replayTime);
        this.retryTimers.set(timer, { redisKey, envelope });
    }

    // @spec SIMPLE-CONCURRENCY-001, SIMPLE-CLOSE-002
    process(
        blockName: string,
        concurrency: number,
        callback?: QueueCallback,
    ): void {
        const q = this.getQueue(blockName);

        // Blocking reads need a dedicated connection
        const blockingClient = this.connect();
        this.blockingClients.push(blockingClient);

        let activeJobs = 0;

        const run = (envelope: Envelope): void => {
            activeJobs++;
            this.activeJobs++;

            const finish = once((err) => {
                activeJobs--;
                this.activeJobs--;
                poller.wake?.();
                if (err) this.handleFailure(q.redisKey, envelope, err);
            });

            if (!callback) {
                finish();
                return;
            }

            try {
                callback(envelope.data, finish);
            } catch (e) {
                finish(e as Error);
            }
        };

        const poll = async (): Promise<void> => {
            while (poller.active) {
                if (activeJobs >= concurrency) {
                    await new Promise<void>((resolve) => {
                        poller.wake = resolve;
                    });
                    poller.wake = null;
                    continue;
                }

                try {
                    const result = await blockingClient.blpop(
                        q.redisKey,
                        POP_TIMEOUT_SECONDS,
                    );
                    if (!result) continue;

                    if (!poller.active) {
                        // Arrived after close() began: hand it back, at the
                        // front, rather than start work we may not finish
                        await this.getClient().lpush(q.redisKey, result[1]);
                        break;
                    }

                    const envelope = this.parse(result[1]);
                    if (envelope) run(envelope);
                } catch (err) {
                    if (poller.active) {
                        Logger.error("SimpleQueue poll error:", err);
                        await new Promise((r) => setTimeout(r, 1000));
                    } else {
                        Logger.error("SimpleQueue error while closing:", err);
                    }
                }
            }
        };

        const poller: Poller = {
            active: true,
            wake: null,
            finished: Promise.resolve(),
        };
        this.pollers.push(poller);
        poller.finished = poll();
    }

    /**
     * Delete the queued messages for this topology's blocks only.
     */
    // @spec SIMPLE-CLEAR-001
    async clear(): Promise<void> {
        const keys = Object.values(this.queues).map((q) => q.redisKey);

        if (this.settings.debug) {
            Logger.info(`Clearing ${keys.length} SimpleQueue queue(s)`);
        }

        if (keys.length > 0) {
            await this.getClient().del(...keys);
        }
    }

    close(timeoutMs: number = DEFAULT_CLOSE_TIMEOUT): Promise<void> {
        if (!this.closing) this.closing = this.shutdown(timeoutMs);
        return this.closing;
    }

    private async shutdown(timeoutMs: number): Promise<void> {
        // 1. Stop taking messages. A blocking pop already in flight is left
        //    to return (at most POP_TIMEOUT_SECONDS) so its message, if it
        //    gets one, can be handed back instead of lost with the socket.
        for (const poller of this.pollers) {
            poller.active = false;
            poller.wake?.();
        }
        await Promise.all(this.pollers.map((p) => p.finished));
        this.pollers = [];

        // 2. Messages waiting out their replay delay go back now
        const pending = [...this.retryTimers.entries()];
        this.retryTimers.clear();
        await Promise.all(
            pending.map(async ([timer, { redisKey, envelope }]) => {
                clearTimeout(timer);
                await this.push(redisKey, {
                    ...envelope,
                    attempt: envelope.attempt + 1,
                }).catch((err) => {
                    Logger.error("SimpleQueue replay error:", err);
                });
            }),
        );

        // 3. Let messages in progress finish
        if (!(await this.waitUntil(() => this.activeJobs === 0, timeoutMs))) {
            Logger.warn(
                `SimpleQueue closed with ${this.activeJobs} message(s) still in progress; they are lost`,
            );
        }

        for (const client of this.blockingClients) client.disconnect();
        this.blockingClients = [];

        this.client?.disconnect();
        this.client = null;
        this.closed = true;

        if (this.settings.debug) {
            Logger.info("SimpleQueue closed");
        }
    }
}

export default SimpleQueue;
