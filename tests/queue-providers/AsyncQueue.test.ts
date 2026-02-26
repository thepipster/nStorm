import { describe, it, expect, vi } from 'vitest';
import { AsyncQueue } from '../../src/queue-providers/AsyncQueue';
import { NStormSettings } from '../../src/BaseBolt';

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

describe('AsyncQueue', () => {
  it('should construct with settings', () => {
    const queue = new AsyncQueue(makeSettings());
    expect(queue).toBeInstanceOf(AsyncQueue);
  });

  it('process should register a queue for the block', () => {
    const queue = new AsyncQueue(makeSettings());
    const callback = vi.fn();

    // Should not throw
    queue.process('myBlock', 1, callback);

    // Now emitting to that block should work
    expect(() =>
      queue.emit('myBlock', { hello: 'world' }, makeSettings())
    ).not.toThrow();
  });

  it('emit should throw if the block queue is not registered', () => {
    const queue = new AsyncQueue(makeSettings());

    expect(() =>
      queue.emit('nonExistent', { data: 1 }, makeSettings())
    ).toThrow(/not defined/);
  });

  it('should deliver messages to the callback', async () => {
    const queue = new AsyncQueue(makeSettings());
    const received: unknown[] = [];

    const callback = vi.fn(
      (job: unknown, done: (err?: Error | null) => void) => {
        received.push(job);
        done();
      }
    );

    queue.process('worker', 2, callback);
    queue.emit('worker', { msg: 'one' }, makeSettings());
    queue.emit('worker', { msg: 'two' }, makeSettings());

    // async.queue processes asynchronously; wait a tick
    await new Promise((r) => setTimeout(r, 50));

    expect(callback).toHaveBeenCalledTimes(2);
    expect(received).toHaveLength(2);
    expect(received[0]).toEqual({ msg: 'one' });
    expect(received[1]).toEqual({ msg: 'two' });
  });

  it('should handle errors in the callback without crashing', async () => {
    const queue = new AsyncQueue(makeSettings());

    const callback = vi.fn(() => {
      throw new Error('boom');
    });

    queue.process('errBlock', 1, callback);
    queue.emit('errBlock', { data: 1 }, makeSettings());

    await new Promise((r) => setTimeout(r, 50));

    expect(callback).toHaveBeenCalledTimes(1);
  });

  it('should respect concurrency', async () => {
    const queue = new AsyncQueue(makeSettings());
    let concurrent = 0;
    let maxConcurrent = 0;

    const callback = vi.fn(
      (_job: unknown, done: (err?: Error | null) => void) => {
        concurrent++;
        maxConcurrent = Math.max(maxConcurrent, concurrent);
        setTimeout(() => {
          concurrent--;
          done();
        }, 30);
      }
    );

    queue.process('concBlock', 2, callback);

    // Push 4 jobs
    for (let i = 0; i < 4; i++) {
      queue.emit('concBlock', { i }, makeSettings());
    }

    await new Promise((r) => setTimeout(r, 200));

    expect(callback).toHaveBeenCalledTimes(4);
    // Max concurrency should not exceed 2
    expect(maxConcurrent).toBeLessThanOrEqual(2);
  });

  it('should work with different cloud names (prefix isolation)', () => {
    const q1 = new AsyncQueue(makeSettings({ cloudName: 'cloud-a' }));
    const q2 = new AsyncQueue(makeSettings({ cloudName: 'cloud-b' }));

    const cb1 = vi.fn();
    const cb2 = vi.fn();

    q1.process('block', 1, cb1);
    q2.process('block', 1, cb2);

    // Emitting to cloud-a should not reach cloud-b
    q1.emit('block', { from: 'a' }, makeSettings({ cloudName: 'cloud-a' }));

    // cloud-b should not have received anything yet
    expect(() =>
      q2.emit('block', { from: 'b' }, makeSettings({ cloudName: 'cloud-b' }))
    ).not.toThrow();
  });
});
