import { describe, it, expect, vi, beforeEach } from 'vitest';
import { BaseBolt } from '../src/BaseBolt';
import type { NStormSettings } from '../src/BaseBolt';

// Minimal mock for nStorm context
function makeMockContext() {
  const mockEmit = vi.fn();
  const mockProcess = vi.fn();
  return {
    queue: {
      emit: mockEmit,
      process: mockProcess,
      registerBlock: vi.fn(),
      start: vi.fn(),
      clear: vi.fn(),
    },
    settings: makeSettings(),
    blocks: [],
    _findBlock: vi.fn(),
    // expose mocks for assertions
    _mockEmit: mockEmit,
    _mockProcess: mockProcess,
  };
}

function makeSettings(overrides?: Partial<NStormSettings>): NStormSettings {
  return {
    cloudName: 'test-cloud',
    queue: 'async',
    metaLogging: false,
    debug: false,
    redis: { port: 6379, host: '127.0.0.1' },
    replay: false,
    replayLimit: 3,
    replayTime: 300,
    ...overrides,
  };
}

describe('BaseBolt', () => {
  let bolt: BaseBolt;

  beforeEach(() => {
    bolt = new BaseBolt();
  });

  describe('constructor', () => {
    it('should initialize with default values', () => {
      expect(bolt.inputs).toEqual([]);
      expect(bolt.subscribers).toEqual([]);
      expect(bolt.limit).toBe(1);
      expect(bolt.threads).toEqual([]);
      expect(bolt.context).toBeNull();
      expect(bolt.debug).toBe(false);
      expect(bolt.name).toBe('');
    });
  });

  describe('setup', () => {
    it('should set name, context, limit, and debug', () => {
      const ctx = makeMockContext();
      bolt.setup({ name: 'testBolt', limit: 5, debug: true }, ctx as any);

      expect(bolt.name).toBe('testBolt');
      expect(bolt.context).toBe(ctx);
      expect(bolt.limit).toBe(5);
      expect(bolt.debug).toBe(true);
    });

    it('should default limit to 1 when not provided', () => {
      const ctx = makeMockContext();
      bolt.setup({ name: 'noLimit' }, ctx as any);

      expect(bolt.limit).toBe(1);
    });

    it('should default debug to false when not provided', () => {
      const ctx = makeMockContext();
      bolt.setup({ name: 'noDebug' }, ctx as any);

      expect(bolt.debug).toBe(false);
    });
  });

  describe('_init', () => {
    it('should call context.queue.process with name, limit, and a callback', () => {
      const ctx = makeMockContext();
      bolt.setup({ name: 'initBolt', limit: 3 }, ctx as any);
      bolt._init();

      expect(ctx._mockProcess).toHaveBeenCalledWith(
        'initBolt',
        3,
        expect.any(Function)
      );
    });

    it('should forward jobs to the process method', () => {
      const ctx = makeMockContext();
      bolt.setup({ name: 'fwdBolt' }, ctx as any);

      const processSpy = vi.spyOn(bolt, 'process').mockResolvedValue(undefined);
      bolt._init();

      // Get the callback that was registered
      const registeredCallback = ctx._mockProcess.mock.calls[0][2];
      const mockDone = vi.fn();
      registeredCallback({ data: 'test' }, mockDone);

      expect(processSpy).toHaveBeenCalledWith({ data: 'test' }, mockDone);
    });

    it('should catch errors thrown by process and call done with error', () => {
      const ctx = makeMockContext();
      bolt.setup({ name: 'errBolt' }, ctx as any);

      const error = new Error('process failed');
      vi.spyOn(bolt, 'process').mockImplementation(() => {
        throw error;
      });
      bolt._init();

      const registeredCallback = ctx._mockProcess.mock.calls[0][2];
      const mockDone = vi.fn();
      registeredCallback({}, mockDone);

      expect(mockDone).toHaveBeenCalledWith(error);
    });
  });

  describe('delay', () => {
    it('should resolve with "done" after the specified time', async () => {
      vi.useFakeTimers();
      const promise = bolt.delay(1000);

      vi.advanceTimersByTime(1000);
      const result = await promise;

      expect(result).toBe('done');
      vi.useRealTimers();
    });
  });

  describe('_isFiltered', () => {
    it('should return true when no filter is provided', () => {
      expect(bolt._isFiltered({ key: 'val' })).toBe(true);
      expect(bolt._isFiltered({ key: 'val' }, undefined)).toBe(true);
    });

    it('should return true when message matches filter', () => {
      expect(
        bolt._isFiltered({ coin: 'heads' }, { coin: 'heads' })
      ).toBe(true);
    });

    it('should return false when message does not match filter', () => {
      expect(
        bolt._isFiltered({ coin: 'heads' }, { coin: 'tails' })
      ).toBe(false);
    });

    it('should return true when filter key is not in message', () => {
      expect(
        bolt._isFiltered({ other: 'val' }, { coin: 'heads' })
      ).toBe(true);
    });

    it('should check the first matching key only', () => {
      expect(
        bolt._isFiltered(
          { a: 1, b: 2 },
          { a: 1, b: 999 }
        )
      ).toBe(true); // matches on 'a' first
    });
  });

  describe('emit', () => {
    it('should send message to all subscribers via queue.emit', () => {
      const ctx = makeMockContext();
      bolt.setup({ name: 'emitBolt' }, ctx as any);

      bolt.subscribers = [
        { name: 'sub1' },
        { name: 'sub2' },
      ];

      bolt.emit({ data: 42 });

      expect(ctx._mockEmit).toHaveBeenCalledTimes(2);
      expect(ctx._mockEmit).toHaveBeenCalledWith('sub1', { data: 42 }, ctx.settings);
      expect(ctx._mockEmit).toHaveBeenCalledWith('sub2', { data: 42 }, ctx.settings);
    });

    it('should skip subscribers whose filter does not match', () => {
      const ctx = makeMockContext();
      bolt.setup({ name: 'filterBolt' }, ctx as any);

      bolt.subscribers = [
        { name: 'heads', options: { filter: { coin: 'heads' } } },
        { name: 'tails', options: { filter: { coin: 'tails' } } },
      ];

      bolt.emit({ coin: 'heads' });

      expect(ctx._mockEmit).toHaveBeenCalledTimes(1);
      expect(ctx._mockEmit).toHaveBeenCalledWith('heads', { coin: 'heads' }, ctx.settings);
    });

    it('should send to subscribers with no filter', () => {
      const ctx = makeMockContext();
      bolt.setup({ name: 'noFilterBolt' }, ctx as any);

      bolt.subscribers = [
        { name: 'all' },
      ];

      bolt.emit({ anything: true });

      expect(ctx._mockEmit).toHaveBeenCalledTimes(1);
    });

    it('should handle empty subscribers list', () => {
      const ctx = makeMockContext();
      bolt.setup({ name: 'lonely' }, ctx as any);

      bolt.emit({ data: 1 });

      expect(ctx._mockEmit).not.toHaveBeenCalled();
    });
  });

  describe('input', () => {
    it('should add an input source', () => {
      bolt.input('source1');

      expect(bolt.inputs).toEqual([{ name: 'source1', options: undefined }]);
    });

    it('should add an input source with options', () => {
      bolt.input('source1', { filter: { coin: 'heads' } });

      expect(bolt.inputs).toEqual([
        { name: 'source1', options: { filter: { coin: 'heads' } } },
      ]);
    });

    it('should return this for chaining', () => {
      const result = bolt.input('a').input('b');

      expect(result).toBe(bolt);
      expect(bolt.inputs).toHaveLength(2);
    });

    it('should deduplicate inputs by name', () => {
      bolt.input('source1');
      bolt.input('source1');

      expect(bolt.inputs).toHaveLength(1);
    });

    it('should keep first entry when deduplicating', () => {
      bolt.input('source1', { filter: { coin: 'heads' } });
      bolt.input('source1', { filter: { coin: 'tails' } });

      expect(bolt.inputs).toHaveLength(1);
      expect(bolt.inputs[0].options?.filter?.coin).toBe('heads');
    });
  });

  describe('ack', () => {
    it('should resolve without error (base implementation)', async () => {
      await expect(bolt.ack({})).resolves.toBeUndefined();
    });
  });

  describe('process', () => {
    it('should resolve without error (base implementation)', async () => {
      const done = vi.fn();
      await expect(bolt.process({}, done)).resolves.toBeUndefined();
    });
  });

  describe('start', () => {
    it('should resolve without error (base implementation)', async () => {
      await expect(bolt.start()).resolves.toBeUndefined();
    });
  });

  describe('subclassing', () => {
    it('should allow overriding process', async () => {
      class MyBolt extends BaseBolt {
        public received: unknown[] = [];
        async process(message: unknown, done: (err?: Error | null) => void): Promise<void> {
          this.received.push(message);
          done();
        }
      }

      const myBolt = new MyBolt();
      const done = vi.fn();
      await myBolt.process({ hello: 'world' }, done);

      expect(myBolt.received).toEqual([{ hello: 'world' }]);
      expect(done).toHaveBeenCalledWith();
    });

    it('should allow overriding start', async () => {
      let started = false;
      class MySpout extends BaseBolt {
        async start(): Promise<void> {
          started = true;
        }
      }

      const spout = new MySpout();
      await spout.start();
      expect(started).toBe(true);
    });
  });
});
