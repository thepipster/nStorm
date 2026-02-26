import Logger from '../Logger';
import async from 'async';
import { BaseQueue, QueueCallback } from './BaseQueue';
import { NStormSettings } from '../BaseBolt';

export class AsyncQueue extends BaseQueue {
  private prefix: string;
  private queues: Record<string, async.QueueObject<unknown>> = {};

  constructor(opts: NStormSettings) {
    super(opts);
    this.prefix = opts.cloudName;
  }

  /**
   * Called by a Spout/Bolt internally to send a message to a specific block
   */
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

    q.push(message, (_err) => {
      // finished processing
    });
  }

  /**
   * Called internally by a block whenever a message is received
   */
  process(
    blockName: string,
    concurrency: number,
    callback?: QueueCallback
  ): void {
    this.queues[this.prefix + '-' + blockName] = async.queue(
      (job: unknown, done: async.ErrorCallback) => {
        try {
          callback?.(job, done);
        } catch (e) {
          done(e as Error);
        }
      },
      concurrency
    );
    Logger.debug(this.prefix + '-' + blockName + ' setup!');
  }
}

export default AsyncQueue;
