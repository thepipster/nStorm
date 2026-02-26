import { describe, it, expect, vi, beforeEach } from 'vitest';
import { NStormSettings } from '../../src/BaseBolt';

// ---- Mock ioredis ----
const mockRpush = vi.fn().mockResolvedValue(1);
const mockBlpop = vi.fn().mockResolvedValue(null);
const mockKeys = vi.fn().mockResolvedValue([]);
const mockDel = vi.fn().mockResolvedValue(1);
const mockDisconnect = vi.fn();

vi.mock('ioredis', () => {
  const MockIORedis = vi.fn(function (this: Record<string, unknown>) {
    this.rpush = mockRpush;
    this.blpop = mockBlpop;
    this.keys = mockKeys;
    this.del = mockDel;
    this.disconnect = mockDisconnect;
  });
  return { default: MockIORedis };
});

import { SimpleQueue } from '../../src/queue-providers/SimpleQueue';

function makeSettings(overrides?: Partial<NStormSettings>): NStormSettings {
  return {
    cloudName: 'test-cloud',
    queue: 'simple',
    metaLogging: false,
    debug: false,
    redis: { port: 6379, host: '127.0.0.1' },
    replay: false,
    replayLimit: 3,
    replayTime: 300,
    ...overrides,
  };
}

describe('SimpleQueue', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('should construct with settings', () => {
    const queue = new SimpleQueue(makeSettings());
    expect(queue).toBeInstanceOf(SimpleQueue);
  });

  describe('registerBlock', () => {
    it('should register with a redis key', async () => {
      const queue = new SimpleQueue(makeSettings());
      await queue.registerBlock({ name: 'myBlock' });

      // After registration, emit should not throw
      expect(() =>
        queue.emit('myBlock', { data: 1 }, makeSettings())
      ).not.toThrow();
    });
  });

  describe('start', () => {
    it('should register a SIGTERM handler', async () => {
      const onceSpy = vi
        .spyOn(process, 'once')
        .mockImplementation(() => process);
      const queue = new SimpleQueue(makeSettings());
      await queue.start();

      expect(onceSpy).toHaveBeenCalledWith('SIGTERM', expect.any(Function));
      onceSpy.mockRestore();
    });
  });

  describe('emit', () => {
    it('should throw if block is not registered', () => {
      const queue = new SimpleQueue(makeSettings());
      expect(() =>
        queue.emit('missing', { data: 1 }, makeSettings())
      ).toThrow(/not defined/);
    });

    it('should call rpush with the correct key and serialized message', async () => {
      const queue = new SimpleQueue(makeSettings());
      await queue.registerBlock({ name: 'pushBlock' });

      queue.emit('pushBlock', { val: 42 }, makeSettings());

      expect(mockRpush).toHaveBeenCalledWith(
        'pushBlock:sq-jobs',
        JSON.stringify({ val: 42 })
      );
    });
  });

  describe('process', () => {
    it('should throw if block is not registered', () => {
      const queue = new SimpleQueue(makeSettings());
      const cb = vi.fn();
      expect(() => queue.process('missing', 1, cb)).toThrow(/not defined/);
    });

    it('should create a poller that calls blpop', async () => {
      // Make blpop return one result then hang (return null)
      let callCount = 0;
      mockBlpop.mockImplementation(async () => {
        callCount++;
        if (callCount === 1) {
          return ['key', JSON.stringify({ msg: 'hello' })];
        }
        // Subsequent calls: simulate delay then return null
        await new Promise((r) => setTimeout(r, 100));
        return null;
      });

      const queue = new SimpleQueue(makeSettings());
      await queue.registerBlock({ name: 'pollBlock' });

      const received: unknown[] = [];
      const callback = vi.fn(
        (job: unknown, done: (err?: Error | null) => void) => {
          received.push(job);
          done();
        }
      );

      queue.process('pollBlock', 1, callback);

      // Wait for polling to process the message
      await new Promise((r) => setTimeout(r, 100));

      expect(callback).toHaveBeenCalledTimes(1);
      expect(received[0]).toEqual({ msg: 'hello' });
    });
  });

  describe('clear', () => {
    it('should delete all matching redis keys', async () => {
      mockKeys.mockResolvedValueOnce([
        'block1:sq-jobs',
        'block2:sq-jobs',
      ]);

      const queue = new SimpleQueue(makeSettings());
      await queue.clear();

      expect(mockKeys).toHaveBeenCalledWith('*:sq-jobs');
      expect(mockDel).toHaveBeenCalledTimes(2);
      expect(mockDel).toHaveBeenCalledWith('block1:sq-jobs');
      expect(mockDel).toHaveBeenCalledWith('block2:sq-jobs');
    });

    it('should handle empty keys gracefully', async () => {
      mockKeys.mockResolvedValueOnce([]);

      const queue = new SimpleQueue(makeSettings());
      await queue.clear();

      expect(mockDel).not.toHaveBeenCalled();
    });

    it('should log when debug is true', async () => {
      mockKeys.mockResolvedValueOnce(['x:sq-jobs']);

      const queue = new SimpleQueue(makeSettings({ debug: true }));
      await queue.clear();

      expect(mockDel).toHaveBeenCalledTimes(1);
    });
  });
});
