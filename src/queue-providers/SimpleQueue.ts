import Logger from "../Logger";
import { BaseQueue, QueueCallback, once } from "./BaseQueue";
import IORedis from "ioredis";

interface Envelope {
    data: Record<string, unknown>;
    attempt: number;
}

interface Poller {
    active: boolean;
    /** Wakes the poll loop when it is waiting for a free concurrency slot */
    wake: (() => void) | null;
}

/** Seconds a blocking pop waits before the loop re-checks whether to stop */
const POP_TIMEOUT_SECONDS = 1;

/**
 * Lightweight Redis-backed queue using RPUSH/BLPOP. Lower overhead than
 * BullMQ, with weaker guarantees:
 *
 * - A message is removed from Redis when a worker picks it up, so a message
 *   that is being processed (or waiting out its `replayTime` before a retry)
 *   when the process dies is lost. Use BullMQ if that matters.
 * - A message whose block calls `done(err)`, throws or rejects is put back on
 *   the queue after `replayTime`, up to `replayLimit` deliveries in total
 *   (`replay: false` disables this).
 *
 * Honours `redis.host/port/password/user/prefix`. No connection is opened
 * until the first message is emitted or a block starts consuming.
 */
export class SimpleQueue extends BaseQueue {
    private client: IORedis | null = null;
    private blockingClients: IORedis[] = [];
    private queues: Record<string, { redisKey: string }> = {};
    private pollers: Poller[] = [];
    private retryTimers = new Set<NodeJS.Timeout>();

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

    private getClient(): IORedis {
        if (!this.client) this.client = this.connect();
        return this.client;
    }

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

    private parse(raw: string): Envelope | null {
        try {
            const parsed = JSON.parse(raw);

            if (
                parsed &&
                typeof parsed === "object" &&
                typeof parsed.attempt === "number" &&
                "data" in parsed
            ) {
                return parsed as Envelope;
            }

            // A bare message, as written by nStorm 4.0 and earlier
            return { data: parsed, attempt: 1 };
        } catch (e) {
            Logger.error("SimpleQueue dropped an unparseable message; ", e);
            return null;
        }
    }

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

        const timer = setTimeout(() => {
            this.retryTimers.delete(timer);
            this.push(redisKey, {
                ...envelope,
                attempt: envelope.attempt + 1,
            }).catch((pushErr) => {
                Logger.error("SimpleQueue replay error:", pushErr);
            });
        }, this.settings.replayTime);
        this.retryTimers.add(timer);
    }

    process(
        blockName: string,
        concurrency: number,
        callback?: QueueCallback,
    ): void {
        const q = this.getQueue(blockName);

        // Blocking reads need a dedicated connection
        const blockingClient = this.connect();
        this.blockingClients.push(blockingClient);

        const poller: Poller = { active: true, wake: null };
        this.pollers.push(poller);

        let activeJobs = 0;

        const run = (envelope: Envelope): void => {
            activeJobs++;

            const finish = once((err) => {
                activeJobs--;
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

                    const envelope = this.parse(result[1]);
                    if (envelope) run(envelope);
                } catch (err) {
                    if (poller.active) {
                        Logger.error("SimpleQueue poll error:", err);
                        await new Promise((r) => setTimeout(r, 1000));
                    }
                }
            }
        };

        void poll();
    }

    /**
     * Delete the queued messages for this topology's blocks only.
     */
    async clear(): Promise<void> {
        const keys = Object.values(this.queues).map((q) => q.redisKey);

        if (this.settings.debug) {
            Logger.debug(`Clearing ${keys.length} SimpleQueue queue(s)`);
        }

        if (keys.length > 0) {
            await this.getClient().del(...keys);
        }
    }

    async close(): Promise<void> {
        for (const poller of this.pollers) {
            poller.active = false;
            poller.wake?.();
        }
        this.pollers = [];

        for (const timer of this.retryTimers) clearTimeout(timer);
        this.retryTimers.clear();

        // Disconnecting aborts any BLPOP in flight, which ends the poll loops
        for (const client of this.blockingClients) client.disconnect();
        this.blockingClients = [];

        this.client?.disconnect();
        this.client = null;

        if (this.settings.debug) {
            Logger.debug("SimpleQueue closed");
        }
    }
}

export default SimpleQueue;
