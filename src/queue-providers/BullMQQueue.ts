import Logger from "../Logger";
import { Queue, Worker, QueueEvents } from "bullmq";
import { BaseQueue, QueueCallback, DEFAULT_CLOSE_TIMEOUT } from "./BaseQueue";

/**
 * Redis-backed provider built on BullMQ. This is the default and the only
 * provider with durable, at-least-once delivery: a message stays in Redis
 * until its block calls `done()`, is retried on failure (`replay`,
 * `replayLimit`, `replayTime`) and is redelivered if the worker dies.
 *
 * No Redis connection is opened until the topology is set up.
 */
export class BullMQQueue extends BaseQueue {
    private queues: Record<string, Queue> = {};
    private workers: Worker[] = [];
    private queueEvents: QueueEvents[] = [];

    /**
     * Options shared by every Queue / Worker / QueueEvents. BullMQ opens its
     * own connections from these, so we never hold one ourselves.
     */
    // @spec BULLMQ-REDIS-001, QUEUE-CONNECT-001
    private bullOptions() {
        const { host, port, password, user, prefix } = this.settings.redis;

        return {
            connection: {
                host,
                port,
                password,
                username: user,
                maxRetriesPerRequest: null,
            },
            ...(prefix ? { prefix } : {}),
        };
    }

    // @spec BULLMQ-DELIVERY-001
    async registerBlock(block: { name: string }): Promise<void> {
        const queueName = this.queueName(block.name);

        if (this.settings.debug) {
            Logger.info(`Registering BullMQ queue ${queueName}`);
        }

        this.queues[queueName] = new Queue(queueName, {
            ...this.bullOptions(),
            defaultJobOptions: {
                attempts: this.maxAttempts,
                backoff: {
                    type: "fixed",
                    delay: this.settings.replayTime,
                },
                removeOnComplete: true,
                removeOnFail: 100,
            },
        });
    }

    async emit(
        blockName: string,
        message: Record<string, unknown>,
    ): Promise<void> {
        const queueName = this.queueName(blockName);
        const queue = this.queues[queueName];

        if (!queue) {
            throw new Error(`Queue ${queueName} is not defined!`);
        }

        await queue.add(blockName, message);
    }

    // @spec BULLMQ-WORKER-001, BULLMQ-DELIVERY-002, BULLMQ-DELIVERY-003, BULLMQ-DEBUG-001
    process(
        blockName: string,
        concurrency: number,
        callback?: QueueCallback,
    ): void {
        const queueName = this.queueName(blockName);

        const worker = new Worker(
            queueName,
            (job) => {
                return new Promise<void>((resolve, reject) => {
                    if (!callback) {
                        resolve();
                        return;
                    }

                    try {
                        callback(job.data, (err?: Error | null) => {
                            if (err) {
                                reject(err);
                            } else {
                                resolve();
                            }
                        });
                    } catch (e) {
                        reject(e);
                    }
                });
            },
            {
                ...this.bullOptions(),
                concurrency,
            },
        );

        worker.on("error", (err) => {
            Logger.error(err);
        });

        this.workers.push(worker);

        if (this.settings.debug) {
            const events = new QueueEvents(queueName, this.bullOptions());

            events.on("failed", ({ jobId, failedReason }) => {
                Logger.error(`Job ${jobId} failed: ${failedReason}`);
            });

            events.on("completed", ({ jobId }) => {
                Logger.info(`Job ${jobId} completed`);
            });

            this.queueEvents.push(events);
        }
    }

    async clear(): Promise<void> {
        for (const queue of Object.values(this.queues)) {
            await queue.obliterate({ force: true });
        }

        if (this.settings.debug) {
            Logger.info("Cleared all BullMQ queues");
        }
    }

    // @spec BULLMQ-CLOSE-001
    async close(timeoutMs: number = DEFAULT_CLOSE_TIMEOUT): Promise<void> {
        const workers = this.workers;
        const events = this.queueEvents;
        const queues = Object.values(this.queues);

        this.workers = [];
        this.queueEvents = [];
        this.queues = {};

        // Workers first, so in-flight jobs finish before the queues go away.
        // worker.close() waits for them indefinitely, so bound it ourselves.
        let timer: NodeJS.Timeout | undefined;
        const graceful = Promise.all(workers.map((w) => w.close())).then(
            () => true,
        );
        const timedOut = new Promise<boolean>((resolve) => {
            timer = setTimeout(() => resolve(false), timeoutMs);
        });

        if (!(await Promise.race([graceful, timedOut]))) {
            Logger.warn(
                "BullMQ workers still busy at the close timeout; forcing them closed. Their jobs will be redelivered",
            );
            await Promise.all(workers.map((w) => w.close(true)));
        }
        clearTimeout(timer);

        await Promise.all(events.map((e) => e.close()));
        await Promise.all(queues.map((q) => q.close()));

        if (this.settings.debug) {
            Logger.info("BullMQ closed");
        }
    }
}

export default BullMQQueue;
