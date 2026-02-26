import Logger from '../Logger';
import { Queue, Worker, QueueEvents } from 'bullmq';
import IORedis from 'ioredis';
import { BaseQueue, QueueCallback } from './BaseQueue';
import { NStormSettings } from '../BaseBolt';

const TTL = 10 * 60 * 1000;

export class BullMQQueue extends BaseQueue {
  private connection: IORedis;
  private queues: Record<string, Queue> = {};
  private workers: Worker[] = [];
  private queueEvents: QueueEvents[] = [];

  constructor(opts: NStormSettings) {
    super(opts);

    if (this.settings.debug) {
      Logger.info('Using BullMQ');
    }

    this.connection = new IORedis({
      host: opts.redis.host,
      port: opts.redis.port,
      password: opts.redis.password,
      maxRetriesPerRequest: null,
    });
  }

  async registerBlock(block: { name: string }): Promise<void> {
    const queueName = `${this.settings.cloudName}-${block.name}`;
    this.queues[queueName] = new Queue(queueName, {
      connection: {
        host: this.settings.redis.host,
        port: this.settings.redis.port,
        password: this.settings.redis.password,
        maxRetriesPerRequest: null,
      },
      defaultJobOptions: {
        attempts: this.settings.replayLimit,
        backoff: {
          type: 'fixed',
          delay: this.settings.replayTime,
        },
        removeOnComplete: true,
        removeOnFail: 100,
      },
    });
  }

  async start(): Promise<void> {
    // Setup graceful shutdown
    const shutdown = async () => {
      if (this.settings.debug) {
        Logger.warn('BullMQ shutdown');
      }
      await this.close();
      process.exit(0);
    };

    process.once('SIGTERM', shutdown);
    process.once('SIGINT', shutdown);
  }

  emit(
    blockName: string,
    message: Record<string, unknown>,
    _opts: NStormSettings
  ): void {
    const queueName = `${this.settings.cloudName}-${blockName}`;
    const queue = this.queues[queueName];

    if (!queue) {
      throw new Error(`Queue ${queueName} is not defined!`);
    }

    queue
      .add(blockName, message, {
        attempts: this.settings.replayLimit,
        backoff: {
          type: 'fixed',
          delay: this.settings.replayTime,
        },
      })
      .catch((err) => {
        Logger.error(err);
      });
  }

  process(
    blockName: string,
    concurrency: number,
    callback?: QueueCallback
  ): void {
    const queueName = `${this.settings.cloudName}-${blockName}`;

    const worker = new Worker(
      queueName,
      async (job) => {
        return new Promise<void>((resolve, reject) => {
          try {
            callback?.(job.data, (err?: Error | null) => {
              if (err) {
                reject(err);
              } else {
                resolve();
              }
            });
          } catch (e) {
            Logger.error('Error in process block; ', e);
            reject(e);
          }
        });
      },
      {
        connection: {
          host: this.settings.redis.host,
          port: this.settings.redis.port,
          password: this.settings.redis.password,
          maxRetriesPerRequest: null,
        },
        concurrency,
      }
    );

    if (this.settings.debug) {
      const events = new QueueEvents(queueName, {
        connection: {
          host: this.settings.redis.host,
          port: this.settings.redis.port,
          password: this.settings.redis.password,
          maxRetriesPerRequest: null,
        },
      });

      events.on('failed', ({ jobId, failedReason }) => {
        Logger.error(`Job ${jobId} failed: ${failedReason}`);
      });

      events.on('completed', ({ jobId }) => {
        if (this.settings.debug) {
          Logger.info(`Job ${jobId} completed`);
        }
      });

      this.queueEvents.push(events);
    }

    worker.on('error', (err) => {
      Logger.error(err);
    });

    this.workers.push(worker);
  }

  async clear(): Promise<void> {
    for (const queueName of Object.keys(this.queues)) {
      const queue = this.queues[queueName];
      await queue.obliterate({ force: true });
    }

    if (this.settings.debug) {
      Logger.debug('Cleared all BullMQ queues');
    }
  }

  async close(): Promise<void> {
    for (const worker of this.workers) {
      await worker.close();
    }
    for (const events of this.queueEvents) {
      await events.close();
    }
    for (const queueName of Object.keys(this.queues)) {
      await this.queues[queueName].close();
    }
    this.connection.disconnect();
  }
}

export default BullMQQueue;
