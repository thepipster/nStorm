import { describe, it, expect, vi } from "vitest";
import { BaseQueue, once } from "../../src/queue-providers/BaseQueue";
import { NStormSettings } from "../../src/BaseBolt";

function makeSettings(overrides?: Partial<NStormSettings>): NStormSettings {
    return {
        cloudName: "test-cloud",
        queue: "async",
        debug: false,
        redis: { port: 6379, host: "127.0.0.1" },
        replay: false,
        replayLimit: 3,
        replayTime: 300,
        ...overrides,
    };
}

describe("BaseQueue", () => {
    it("should construct with settings", () => {
        const settings = makeSettings();
        const queue = new BaseQueue(settings);
        expect(queue).toBeInstanceOf(BaseQueue);
    });

    it("registerBlock should resolve without error", async () => {
        const queue = new BaseQueue(makeSettings());
        await expect(
            queue.registerBlock({ name: "testBlock" }),
        ).resolves.toBeUndefined();
    });

    it("start should resolve without error", async () => {
        const queue = new BaseQueue(makeSettings());
        await expect(queue.start()).resolves.toBeUndefined();
    });

    it("emit should resolve without error", async () => {
        const queue = new BaseQueue(makeSettings());
        await expect(queue.emit("block", { data: 1 })).resolves.toBeUndefined();
    });

    it("close should resolve without error", async () => {
        const queue = new BaseQueue(makeSettings());
        await expect(queue.close()).resolves.toBeUndefined();
    });

    it("once should only let the first call through", () => {
        const fn = vi.fn();
        const wrapped = once(fn);
        const err = new Error("first");

        wrapped(err);
        wrapped();

        expect(fn).toHaveBeenCalledTimes(1);
        expect(fn).toHaveBeenCalledWith(err);
    });

    it("process should not throw", () => {
        const queue = new BaseQueue(makeSettings());
        expect(() => queue.process("block", 1)).not.toThrow();
    });

    it("clear should resolve without error", async () => {
        const queue = new BaseQueue(makeSettings());
        await expect(queue.clear()).resolves.toBeUndefined();
    });
});
