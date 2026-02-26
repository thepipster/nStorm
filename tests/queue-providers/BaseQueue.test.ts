import { describe, it, expect } from 'vitest';
import { BaseQueue } from '../../src/queue-providers/BaseQueue';
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

describe('BaseQueue', () => {
  it('should construct with settings', () => {
    const settings = makeSettings();
    const queue = new BaseQueue(settings);
    expect(queue).toBeInstanceOf(BaseQueue);
  });

  it('registerBlock should resolve without error', async () => {
    const queue = new BaseQueue(makeSettings());
    await expect(queue.registerBlock({ name: 'testBlock' })).resolves.toBeUndefined();
  });

  it('start should resolve without error', async () => {
    const queue = new BaseQueue(makeSettings());
    await expect(queue.start()).resolves.toBeUndefined();
  });

  it('emit should not throw', () => {
    const queue = new BaseQueue(makeSettings());
    expect(() => queue.emit('block', { data: 1 }, makeSettings())).not.toThrow();
  });

  it('process should not throw', () => {
    const queue = new BaseQueue(makeSettings());
    expect(() => queue.process('block', 1)).not.toThrow();
  });

  it('clear should resolve without error', async () => {
    const queue = new BaseQueue(makeSettings());
    await expect(queue.clear()).resolves.toBeUndefined();
  });
});
