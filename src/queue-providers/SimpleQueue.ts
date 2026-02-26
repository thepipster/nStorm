import Logger from '../Logger';
import { BaseQueue, QueueCallback } from './BaseQueue';
import { NStormSettings } from '../BaseBolt';
import IORedis from 'ioredis';

/**
 * Simple Redis-backed queue using RPUSH/BLPOP.
 * A lightweight replacement for featureless-job-queue.
 */
export class SimpleQueue extends BaseQueue {
  private prefix: string;
  private client: IORedis;
  private queues: Record<string, { redisKey: string }> = {};
  private pollers: Array<{ active: boolean }> = [];

  constructor(opts: NStormSettings) {
    super(opts);
    this.prefix = opts.cloudName;

    this.client = new IORedis({
      host: opts.redis.host,
      port: opts.redis.port,
      password: opts.redis.password,
      maxRetriesPerRequest: null,
    });
  }

  async registerBlock(block: { name: string }): Promise<void> {
    this.queues[this.prefix + '-' + block.name] = {
      redisKey: block.name + ':sq-jobs',
    };
  }

  async start(): Promise<void> {
    // Setup a graceful shutdown
    process.once('SIGTERM', async () => {
      if (this.settings.debug) {
        Logger.warn('SimpleQueue shutdown');
      }
      this.pollers.forEach((p) => (p.active = false));
      this.client.disconnect();
      process.exit(0);
    });
  }

  emit(
    blockName: string,
    message: Record<string, unknown>,
    _opts: NStormSettings
  ): void {
    const q = this.queues[this.prefix + '-' + blockName];

    if (!q) {
      throw new Error(
        `Queue ${this.prefix + '-' + blockName} is not defined!`
      );
    }

    this.client.rpush(q.redisKey, JSON.stringify(message)).catch((err) => {
      Logger.error('SimpleQueue emit error:', err);
    });
  }

  process(
    blockName: string,
    concurrency: number,
    callback?: QueueCallback
  ): void {
    const q = this.queues[this.prefix + '-' + blockName];

    if (!q) {
      throw new Error(
        `Queue ${this.prefix + '-' + blockName} is not defined!`
      );
    }

    // Create a dedicated connection for blocking reads
    const blockingClient = new IORedis({
      host: this.settings.redis.host,
      port: this.settings.redis.port,
      password: this.settings.redis.password,
      maxRetriesPerRequest: null,
    });

    const poller = { active: true };
    this.pollers.push(poller);

    let activeJobs = 0;

    const poll = async () => {
      while (poller.active) {
        if (activeJobs >= concurrency) {
          await new Promise((r) => setTimeout(r, 50));
          continue;
        }

        try {
          const result = await blockingClient.blpop(q.redisKey, 0);
          if (!result) continue;

          const jobData = JSON.parse(result[1]);
          activeJobs++;

          try {
            callback?.(jobData, (err?: Error | null) => {
              activeJobs--;
              if (err) {
                Logger.error('SimpleQueue job error:', err);
              }
            });
          } catch (e) {
            activeJobs--;
            Logger.error('SimpleQueue process error:', e);
          }
        } catch (err) {
          if (poller.active) {
            Logger.error('SimpleQueue poll error:', err);
            await new Promise((r) => setTimeout(r, 1000));
          }
        }
      }
    };

    poll();
  }

  async clear(): Promise<void> {
    const keys = await this.client.keys('*:sq-jobs');

    if (this.settings.debug) {
      Logger.debug('Clearing queue. Deleting ' + keys.length);
    }

    for (const key of keys) {
      Logger.info('Deleting ' + key);
      await this.client.del(key);
    }

    if (this.settings.debug) {
      Logger.debug('Done clearing queue');
    }
  }
}

export default SimpleQueue;
