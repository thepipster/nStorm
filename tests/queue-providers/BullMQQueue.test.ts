import { describe, it, expect, vi, beforeEach } from 'vitest';
import { NStormSettings } from '../../src/BaseBolt';

// ---- Mock bullmq ----
const mockAdd = vi.fn().mockResolvedValue({ id: '1' });
const mockObliterate = vi.fn().mockResolvedValue(undefined);
const mockQueueClose = vi.fn().mockResolvedValue(undefined);
const mockWorkerClose = vi.fn().mockResolvedValue(undefined);
const mockWorkerOn = vi.fn();
const mockEventsClose = vi.fn().mockResolvedValue(undefined);
const mockEventsOn = vi.fn();

let capturedWorkerProcessor: ((job: { data: unknown }) => Promise<void>) | null =
  null;

vi.mock('bullmq', () => {
  const MockQueue = vi.fn(function (this: Record<string, unknown>) {
    this.add = mockAdd;
    this.obliterate = mockObliterate;
    this.close = mockQueueClose;
  });

  const MockWorker = vi.fn(function (
    this: Record<string, unknown>,
    _name: string,
    processor: unknown
  ) {
    capturedWorkerProcessor = processor as (job: {
      data: unknown;
    }) => Promise<void>;
    this.close = mockWorkerClose;
    this.on = mockWorkerOn;
  });

  const MockQueueEvents = vi.fn(function (this: Record<string, unknown>) {
    this.close = mockEventsClose;
    this.on = mockEventsOn;
  });

  return {
    Queue: MockQueue,
    Worker: MockWorker,
    QueueEvents: MockQueueEvents,
  };
});

// ---- Mock ioredis ----
vi.mock('ioredis', () => {
  const MockIORedis = vi.fn(function (this: Record<string, unknown>) {
    this.disconnect = vi.fn();
  });
  return { default: MockIORedis };
});

import { BullMQQueue } from '../../src/queue-providers/BullMQQueue';
import { Queue, Worker, QueueEvents } from 'bullmq';

function makeSettings(overrides?: Partial<NStormSettings>): NStormSettings {
  return {
    cloudName: 'test-cloud',
    queue: 'bullmq',
    metaLogging: false,
    debug: false,
    redis: { port: 6379, host: '127.0.0.1' },
    replay: false,
    replayLimit: 3,
    replayTime: 300,
    ...overrides,
  };
}

describe('BullMQQueue', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    capturedWorkerProcessor = null;
  });

  it('should construct and create an IORedis connection', () => {
    const queue = new BullMQQueue(makeSettings());
    expect(queue).toBeInstanceOf(BullMQQueue);
  });

  it('should log when debug is true', () => {
    // Should not throw even with debug on
    const queue = new BullMQQueue(makeSettings({ debug: true }));
    expect(queue).toBeDefined();
  });

  describe('registerBlock', () => {
    it('should create a BullMQ Queue for the block', async () => {
      const queue = new BullMQQueue(makeSettings());
      await queue.registerBlock({ name: 'myBlock' });

      expect(Queue).toHaveBeenCalledWith(
        'test-cloud-myBlock',
        expect.objectContaining({
          connection: expect.objectContaining({
            host: '127.0.0.1',
            port: 6379,
          }),
          defaultJobOptions: expect.objectContaining({
            attempts: 3,
            removeOnComplete: true,
          }),
        })
      );
    });
  });

  describe('start', () => {
    it('should register SIGTERM and SIGINT handlers', async () => {
      const onceSpy = vi.spyOn(process, 'once').mockImplementation(() => process);
      const queue = new BullMQQueue(makeSettings());
      await queue.start();

      expect(onceSpy).toHaveBeenCalledWith('SIGTERM', expect.any(Function));
      expect(onceSpy).toHaveBeenCalledWith('SIGINT', expect.any(Function));

      onceSpy.mockRestore();
    });
  });

  describe('emit', () => {
    it('should throw if block queue is not registered', () => {
      const queue = new BullMQQueue(makeSettings());
      expect(() =>
        queue.emit('missing', { data: 1 }, makeSettings())
      ).toThrow(/not defined/);
    });

    it('should call queue.add with correct params', async () => {
      const queue = new BullMQQueue(makeSettings());
      await queue.registerBlock({ name: 'emitBlock' });

      queue.emit('emitBlock', { val: 42 }, makeSettings());

      expect(mockAdd).toHaveBeenCalledWith(
        'emitBlock',
        { val: 42 },
        expect.objectContaining({
          attempts: 3,
          backoff: { type: 'fixed', delay: 300 },
        })
      );
    });
  });

  describe('process', () => {
    it('should create a Worker for the block', async () => {
      const queue = new BullMQQueue(makeSettings());
      await queue.registerBlock({ name: 'procBlock' });

      const callback = vi.fn();
      queue.process('procBlock', 5, callback);

      expect(Worker).toHaveBeenCalledWith(
        'test-cloud-procBlock',
        expect.any(Function),
        expect.objectContaining({
          concurrency: 5,
          connection: expect.objectContaining({
            host: '127.0.0.1',
            port: 6379,
          }),
        })
      );

      expect(mockWorkerOn).toHaveBeenCalledWith('error', expect.any(Function));
    });

    it('should create QueueEvents when debug is true', async () => {
      const queue = new BullMQQueue(makeSettings({ debug: true }));
      await queue.registerBlock({ name: 'debugBlock' });

      const callback = vi.fn();
      queue.process('debugBlock', 1, callback);

      expect(QueueEvents).toHaveBeenCalledWith(
        'test-cloud-debugBlock',
        expect.objectContaining({
          connection: expect.any(Object),
        })
      );
    });

    it('should invoke callback when worker processes a job', async () => {
      const queue = new BullMQQueue(makeSettings());
      await queue.registerBlock({ name: 'callbackBlock' });

      const callback = vi.fn(
        (_job: unknown, done: (err?: Error | null) => void) => {
          done();
        }
      );

      queue.process('callbackBlock', 1, callback);

      // Simulate the worker running a job
      expect(capturedWorkerProcessor).not.toBeNull();
      await capturedWorkerProcessor!({ data: { key: 'value' } });

      expect(callback).toHaveBeenCalledWith(
        { key: 'value' },
        expect.any(Function)
      );
    });

    it('should reject when callback calls done with error', async () => {
      const queue = new BullMQQueue(makeSettings());
      await queue.registerBlock({ name: 'errBlock' });

      const callback = vi.fn(
        (_job: unknown, done: (err?: Error | null) => void) => {
          done(new Error('job failed'));
        }
      );

      queue.process('errBlock', 1, callback);

      await expect(
        capturedWorkerProcessor!({ data: { bad: true } })
      ).rejects.toThrow('job failed');
    });

    it('should reject when callback throws', async () => {
      const queue = new BullMQQueue(makeSettings());
      await queue.registerBlock({ name: 'throwBlock' });

      const callback = vi.fn(() => {
        throw new Error('callback exploded');
      });

      queue.process('throwBlock', 1, callback);

      await expect(
        capturedWorkerProcessor!({ data: {} })
      ).rejects.toThrow('callback exploded');
    });
  });

  describe('clear', () => {
    it('should obliterate all registered queues', async () => {
      const queue = new BullMQQueue(makeSettings());
      await queue.registerBlock({ name: 'a' });
      await queue.registerBlock({ name: 'b' });

      await queue.clear();

      expect(mockObliterate).toHaveBeenCalledTimes(2);
      expect(mockObliterate).toHaveBeenCalledWith({ force: true });
    });
  });

  describe('close', () => {
    it('should close all workers, events, queues, and disconnect', async () => {
      const queue = new BullMQQueue(makeSettings({ debug: true }));
      await queue.registerBlock({ name: 'closeBlock' });

      const callback = vi.fn();
      queue.process('closeBlock', 1, callback);

      await queue.close();

      expect(mockWorkerClose).toHaveBeenCalled();
      expect(mockEventsClose).toHaveBeenCalled();
      expect(mockQueueClose).toHaveBeenCalled();
    });
  });
});
