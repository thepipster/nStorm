import Logger from "../Logger";
import async from "async";
import { BaseQueue, QueueCallback, once } from "./BaseQueue";

interface Task {
    message: Record<string, unknown>;
    attempt: number;
}

/**
 * In-process queue built on `async.queue`. Nothing leaves the process, so no
 * Redis is needed, messages cannot be shared between workers, and anything
 * still queued is lost when the process exits.
 *
 * Failed messages are replayed in-process, honouring `replay`, `replayLimit`
 * and `replayTime`. The `redis` settings are ignored.
 */
export class AsyncQueue extends BaseQueue {
    private queues: Record<string, async.QueueObject<Task>> = {};
    private retryTimers = new Set<NodeJS.Timeout>();

    /**
     * Called by a Spout/Bolt internally to send a message to a specific block
     */
    // @spec ASYNC-DELIVERY-001
    async emit(
        blockName: string,
        message: Record<string, unknown>,
    ): Promise<void> {
        const queueName = this.queueName(blockName);

        if (!this.queues[queueName]) {
            throw new Error(`Queue ${queueName} is not defined!`);
        }

        this.push(queueName, { message, attempt: 1 });
    }

    private push(queueName: string, task: Task): void {
        const q = this.queues[queueName];
        if (!q) return; // closed while a retry was pending

        q.push(task, (err) => {
            if (!err) return;

            if (task.attempt >= this.maxAttempts) {
                Logger.error(
                    `${queueName}: message dropped after ${task.attempt} attempt(s); `,
                    err,
                );
                return;
            }

            const timer = setTimeout(() => {
                this.retryTimers.delete(timer);
                this.push(queueName, { ...task, attempt: task.attempt + 1 });
            }, this.settings.replayTime);
            this.retryTimers.add(timer);
        });
    }

    /**
     * Called internally by a block to register its message handler
     */
    // @spec ASYNC-CONCURRENCY-001, ASYNC-DELIVERY-002
    process(
        blockName: string,
        concurrency: number,
        callback?: QueueCallback,
    ): void {
        const queueName = this.queueName(blockName);

        this.queues[queueName] = async.queue<Task, Error>((task, done) => {
            const finish = once(done);

            if (!callback) {
                finish();
                return;
            }

            try {
                callback(task.message, finish);
            } catch (e) {
                finish(e as Error);
            }
        }, concurrency);

        if (this.settings.debug) {
            Logger.debug(`${queueName} setup!`);
        }
    }

    async clear(): Promise<void> {
        for (const timer of this.retryTimers) clearTimeout(timer);
        this.retryTimers.clear();

        for (const q of Object.values(this.queues)) q.remove(() => true);
    }

    // @spec ASYNC-CLOSE-001
    async close(): Promise<void> {
        for (const timer of this.retryTimers) clearTimeout(timer);
        this.retryTimers.clear();

        for (const q of Object.values(this.queues)) q.kill();
        this.queues = {};
    }
}

export default AsyncQueue;
