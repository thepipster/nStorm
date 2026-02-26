import Logger from './Logger';

export interface Subscriber {
  name: string;
  options?: InputOptions;
}

export interface InputOptions {
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
  cloudName: string;
  queue: 'bullmq' | 'async' | 'simple';
  metaLogging: boolean;
  debug: boolean;
  redis: {
    port: number;
    host: string;
    password?: string;
    user?: string;
    prefix?: string;
  };
  replay: boolean;
  replayLimit: number;
  replayTime: number;
}

/**
 * The base class for a bolt / spout
 */
export class BaseBolt {
  public inputs: InputEntry[] = [];
  public subscribers: Subscriber[] = [];
  public limit: number = 1;
  public threads: unknown[] = [];
  public context: import('./nStorm').default | null = null;
  public debug: boolean = false;
  public name: string = '';

  setup(opts: BlockSetupOpts, context: import('./nStorm').default): void {
    this.name = opts?.name ?? '';
    this.context = context;
    this.limit = opts?.limit ?? 1;
    this.debug = opts?.debug ? true : false;
  }

  _init(): void {
    this.context!.queue.process(
      this.name,
      this.limit,
      (job: unknown, done: (err?: Error | null) => void) => {
        try {
          this.process(job, done);
        } catch (e) {
          Logger.error('Error in process block; ', e);
          done(e as Error);
        }
      }
    );
  }

  delay(ms: number): Promise<string> {
    return new Promise((resolve) => {
      setTimeout(() => {
        return resolve('done');
      }, ms);
    });
  }

  /**
   * Check to see if any filter options have been passed. If so, check to see
   * if the message has a corresponding key and it is of the correct value.
   */
  _isFiltered(
    msg: Record<string, unknown>,
    filter?: Record<string, unknown>
  ): boolean {
    if (!filter) return true;

    for (const key in filter) {
      for (const msgKey in msg) {
        if (msgKey === key) {
          if (filter[key] === msg[msgKey]) {
            return true;
          } else {
            return false;
          }
        }
      }
    }

    return true;
  }

  /**
   * Send a message to anyone subscribed to this block
   */
  emit(message: Record<string, unknown>): void {
    for (let i = 0; i < this.subscribers.length; i += 1) {
      let skip = false;

      if (this.subscribers[i].options?.filter) {
        if (!this._isFiltered(message, this.subscribers[i].options!.filter)) {
          skip = true;
        }
      }

      if (!skip) {
        this.context!.queue.emit(
          this.subscribers[i].name,
          message,
          this.context!.settings
        );
      }
    }
  }

  async ack(_message: unknown): Promise<void> {
    // Override in subclass
  }

  /**
   * Do some processing, this should be defined on the child bolt
   */
  async process(
    _message: unknown,
    _done: (err?: Error | null) => void
  ): Promise<void> {
    // Override in subclass
  }

  async start(): Promise<void> {
    // Override in subclass
  }

  /**
   * This function sets up the input for a block.
   */
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
