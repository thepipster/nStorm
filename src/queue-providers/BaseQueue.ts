import { NStormSettings } from '../BaseBolt';

export type QueueCallback = (
  job: unknown,
  done: (err?: Error | null) => void
) => void;

/**
 * Base class for a queue provider
 */
export class BaseQueue {
  protected settings: NStormSettings;

  constructor(opts: NStormSettings) {
    this.settings = opts;
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
   * Called by a Spout/Bolt internally to send a message to a specific block
   */
  emit(
    _blockName: string,
    _message: Record<string, unknown>,
    _opts: NStormSettings
  ): void {
    // Override in subclass
  }

  /**
   * Called internally by a block whenever a message is received
   */
  process(
    _blockName: string,
    _concurrency: number,
    _callback?: QueueCallback
  ): void {
    // Override in subclass
  }

  /**
   * Called internally by nStorm to clear out any residual queue data
   */
  async clear(): Promise<void> {
    return;
  }
}

export default BaseQueue;
