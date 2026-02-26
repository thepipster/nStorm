import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { BaseBolt } from '../src/BaseBolt';
import type { NStormSettings } from '../src/BaseBolt';

// Mock all queue providers so no Redis connection is created
vi.mock('ioredis', () => {
  const MockIORedis = vi.fn(function (this: Record<string, unknown>) {
    this.disconnect = vi.fn();
  });
  return { default: MockIORedis };
});

vi.mock('bullmq', () => {
  const MockQueue = vi.fn(function (this: Record<string, unknown>) {
    this.add = vi.fn().mockResolvedValue({ id: '1' });
    this.obliterate = vi.fn().mockResolvedValue(undefined);
    this.close = vi.fn().mockResolvedValue(undefined);
  });
  const MockWorker = vi.fn(function (this: Record<string, unknown>) {
    this.close = vi.fn().mockResolvedValue(undefined);
    this.on = vi.fn();
  });
  const MockQueueEvents = vi.fn(function (this: Record<string, unknown>) {
    this.close = vi.fn().mockResolvedValue(undefined);
    this.on = vi.fn();
  });
  return { Queue: MockQueue, Worker: MockWorker, QueueEvents: MockQueueEvents };
});

import { nStorm } from '../src/nStorm';

function makeOpts(overrides?: Record<string, unknown>) {
  return {
    queue: 'async' as const,
    debug: false,
    cloudName: 'test-cloud',
    ...overrides,
  };
}

describe('nStorm', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('constructor', () => {
    it('should create with default settings', () => {
      const cloud = new nStorm({ queue: 'async' });
      expect(cloud.settings.cloudName).toBe('stormcloud');
      expect(cloud.settings.queue).toBe('async');
      expect(cloud.settings.debug).toBe(false);
      expect(cloud.settings.redis.port).toBe(6379);
      expect(cloud.settings.redis.host).toBe('127.0.0.1');
      expect(cloud.settings.replayLimit).toBe(3);
    });

    it('should merge custom settings with defaults', () => {
      const cloud = new nStorm({
        queue: 'async',
        cloudName: 'myCloud',
        debug: true,
        redis: { port: 7777 },
      });

      expect(cloud.settings.cloudName).toBe('myCloud');
      expect(cloud.settings.debug).toBe(true);
      expect(cloud.settings.redis.port).toBe(7777);
      expect(cloud.settings.redis.host).toBe('127.0.0.1'); // preserved default
    });

    it('should use AsyncQueue for queue: "async"', () => {
      const cloud = new nStorm({ queue: 'async' });
      expect(cloud.queue.constructor.name).toBe('AsyncQueue');
    });

    it('should use BullMQQueue for queue: "bullmq"', () => {
      const cloud = new nStorm({ queue: 'bullmq' });
      expect(cloud.queue.constructor.name).toBe('BullMQQueue');
    });

    it('should use SimpleQueue for queue: "simple"', () => {
      const cloud = new nStorm({ queue: 'simple' });
      expect(cloud.queue.constructor.name).toBe('SimpleQueue');
    });

    it('should default to AsyncQueue for unknown queue type', () => {
      const cloud = new nStorm({ queue: 'unknown' as any });
      expect(cloud.queue.constructor.name).toBe('AsyncQueue');
    });

    it('should default to bullmq when no options provided', () => {
      const cloud = new nStorm();
      expect(cloud.settings.queue).toBe('bullmq');
    });
  });

  describe('addBlock', () => {
    let cloud: nStorm;

    beforeEach(() => {
      cloud = new nStorm(makeOpts());
    });

    it('should add a block to the topology', () => {
      const bolt = new BaseBolt();
      cloud.addBlock('myBolt', bolt);

      expect(cloud.blocks).toHaveLength(1);
      expect(cloud.blocks[0]).toBe(bolt);
      expect(bolt.name).toBe('myBolt');
    });

    it('should set up the block with name, limit, and debug', () => {
      const bolt = new BaseBolt();
      cloud.addBlock('myBolt', bolt, 5);

      expect(bolt.name).toBe('myBolt');
      expect(bolt.limit).toBe(5);
      expect(bolt.context).toBe(cloud);
    });

    it('should return the block for chaining', () => {
      const bolt = new BaseBolt();
      const result = cloud.addBlock('myBolt', bolt);

      expect(result).toBe(bolt);
    });

    it('should support chaining .input() on the result', () => {
      const spout = new BaseBolt();
      const bolt = new BaseBolt();
      cloud.addBlock('spout', spout);
      cloud.addBlock('bolt', bolt).input('spout');

      expect(bolt.inputs).toEqual([{ name: 'spout', options: undefined }]);
    });

    it('should throw when adding a block with a duplicate name', () => {
      cloud.addBlock('dup', new BaseBolt());

      expect(() => cloud.addBlock('dup', new BaseBolt())).toThrow(
        /Block names must be unique/
      );
    });
  });

  describe('_findBlock', () => {
    it('should return the block with the matching name', () => {
      const cloud = new nStorm(makeOpts());
      const bolt = new BaseBolt();
      cloud.addBlock('target', bolt);

      expect(cloud._findBlock('target')).toBe(bolt);
    });

    it('should return null when no block matches', () => {
      const cloud = new nStorm(makeOpts());
      expect(cloud._findBlock('nonexistent')).toBeNull();
    });
  });

  describe('setupTopology', () => {
    it('should wire up subscribers based on inputs', async () => {
      const cloud = new nStorm(makeOpts());
      const spout = new BaseBolt();
      const bolt = new BaseBolt();

      cloud.addBlock('spout', spout);
      cloud.addBlock('bolt', bolt).input('spout');

      await cloud.setupTopology();

      expect(spout.subscribers).toEqual([
        { name: 'bolt', options: undefined },
      ]);
    });

    it('should wire filter options into subscribers', async () => {
      const cloud = new nStorm(makeOpts());
      const spout = new BaseBolt();
      const headsBolt = new BaseBolt();

      cloud.addBlock('spout', spout);
      cloud.addBlock('heads', headsBolt).input('spout', { filter: { coin: 'heads' } });

      await cloud.setupTopology();

      expect(spout.subscribers).toEqual([
        { name: 'heads', options: { filter: { coin: 'heads' } } },
      ]);
    });

    it('should support multiple inputs on a single block', async () => {
      const cloud = new nStorm(makeOpts());
      const s1 = new BaseBolt();
      const s2 = new BaseBolt();
      const results = new BaseBolt();

      cloud.addBlock('s1', s1);
      cloud.addBlock('s2', s2);
      cloud.addBlock('results', results).input('s1').input('s2');

      await cloud.setupTopology();

      expect(s1.subscribers).toEqual([{ name: 'results', options: undefined }]);
      expect(s2.subscribers).toEqual([{ name: 'results', options: undefined }]);
    });

    it('should throw when an input source block does not exist', async () => {
      const cloud = new nStorm(makeOpts());
      const bolt = new BaseBolt();
      cloud.addBlock('bolt', bolt).input('nonexistent');

      await expect(cloud.setupTopology()).rejects.toThrow(
        /could not find its source block/
      );
    });

    it('should call registerBlock and _init on each block', async () => {
      const cloud = new nStorm(makeOpts());
      const bolt = new BaseBolt();

      const registerSpy = vi.spyOn(cloud.queue, 'registerBlock');
      const initSpy = vi.spyOn(BaseBolt.prototype, '_init').mockImplementation(() => {});

      cloud.addBlock('bolt', bolt);
      await cloud.setupTopology();

      expect(registerSpy).toHaveBeenCalledWith(bolt);
      expect(initSpy).toHaveBeenCalled();

      initSpy.mockRestore();
    });
  });

  describe('start', () => {
    it('should call queue.start and block.start for each block', async () => {
      const cloud = new nStorm(makeOpts());
      const bolt = new BaseBolt();
      cloud.addBlock('bolt', bolt);

      const queueStartSpy = vi.spyOn(cloud.queue, 'start').mockResolvedValue();
      const blockStartSpy = vi.spyOn(bolt, 'start').mockResolvedValue();

      // Need to setup topology first (mock _init so it doesn't need queue.process)
      vi.spyOn(BaseBolt.prototype, '_init').mockImplementation(() => {});
      await cloud.setupTopology();

      await cloud.start();

      expect(queueStartSpy).toHaveBeenCalled();
      expect(blockStartSpy).toHaveBeenCalled();
    });

    it('should call blocks in order', async () => {
      const cloud = new nStorm(makeOpts());
      const order: string[] = [];

      class OrderBolt extends BaseBolt {
        constructor(private label: string) {
          super();
        }
        async start(): Promise<void> {
          order.push(this.label);
        }
      }

      const b1 = new OrderBolt('first');
      const b2 = new OrderBolt('second');

      cloud.addBlock('b1', b1);
      cloud.addBlock('b2', b2);

      vi.spyOn(cloud.queue, 'start').mockResolvedValue();
      vi.spyOn(BaseBolt.prototype, '_init').mockImplementation(() => {});
      await cloud.setupTopology();

      await cloud.start();

      expect(order).toEqual(['first', 'second']);
    });
  });

  describe('reset', () => {
    it('should call queue.clear', async () => {
      const cloud = new nStorm(makeOpts());
      const clearSpy = vi.spyOn(cloud.queue, 'clear').mockResolvedValue();

      await cloud.reset();

      expect(clearSpy).toHaveBeenCalled();
    });
  });

  describe('full topology integration (with AsyncQueue)', () => {
    it('should route messages through a simple topology', async () => {
      const cloud = new nStorm(makeOpts());
      const received: unknown[] = [];

      class Spout extends BaseBolt {
        async start(): Promise<void> {
          this.emit({ coin: 'heads', value: 1 });
          this.emit({ coin: 'tails', value: 2 });
        }
      }

      class Collector extends BaseBolt {
        async process(
          message: unknown,
          done: (err?: Error | null) => void
        ): Promise<void> {
          received.push(message);
          done();
        }
      }

      const spout = new Spout();
      const collector = new Collector();

      cloud.addBlock('spout', spout);
      cloud.addBlock('collector', collector).input('spout');

      // No mocking _init here — let the real AsyncQueue wire everything up
      await cloud.setupTopology();
      await cloud.start();

      // Wait for async queue to flush
      await new Promise((r) => setTimeout(r, 100));

      expect(received).toHaveLength(2);
      expect(received).toEqual(
        expect.arrayContaining([
          { coin: 'heads', value: 1 },
          { coin: 'tails', value: 2 },
        ])
      );
    });

    it('should filter messages based on subscriber options', async () => {
      const cloud = new nStorm(makeOpts());
      const headsReceived: unknown[] = [];
      const tailsReceived: unknown[] = [];

      class Spout extends BaseBolt {
        async start(): Promise<void> {
          this.emit({ coin: 'heads' });
          this.emit({ coin: 'tails' });
          this.emit({ coin: 'heads' });
        }
      }

      class HeadsBolt extends BaseBolt {
        async process(
          message: unknown,
          done: (err?: Error | null) => void
        ): Promise<void> {
          headsReceived.push(message);
          done();
        }
      }

      class TailsBolt extends BaseBolt {
        async process(
          message: unknown,
          done: (err?: Error | null) => void
        ): Promise<void> {
          tailsReceived.push(message);
          done();
        }
      }

      const spout = new Spout();
      const heads = new HeadsBolt();
      const tails = new TailsBolt();

      cloud.addBlock('spout', spout);
      cloud.addBlock('heads', heads).input('spout', { filter: { coin: 'heads' } });
      cloud.addBlock('tails', tails).input('spout', { filter: { coin: 'tails' } });

      // No mocking _init here — let the real AsyncQueue wire everything up
      await cloud.setupTopology();
      await cloud.start();

      await new Promise((r) => setTimeout(r, 100));

      expect(headsReceived).toHaveLength(2);
      expect(tailsReceived).toHaveLength(1);
    });
  });
});
