import { describe, it, expect, vi } from "vitest";
import { AsyncQueue } from "../../src/queue-providers/AsyncQueue";
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
        processTimeout: 0,
        ...overrides,
    };
}

describe("AsyncQueue", () => {
    it("should construct with settings", () => {
        const queue = new AsyncQueue(makeSettings());
        expect(queue).toBeInstanceOf(AsyncQueue);
    });

    it("process should register a queue for the block", async () => {
        const queue = new AsyncQueue(makeSettings());
        const callback = vi.fn();

        // Should not throw
        queue.process("myBlock", 1, callback);

        // Now emitting to that block should work
        await expect(
            queue.emit("myBlock", { hello: "world" }),
        ).resolves.toBeUndefined();
    });

    // @spec QUEUE-EMIT-001
    it("emit should reject if the block queue is not registered", async () => {
        const queue = new AsyncQueue(makeSettings());

        await expect(queue.emit("nonExistent", { data: 1 })).rejects.toThrow(
            /not defined/,
        );
    });

    // @spec ASYNC-DELIVERY-001
    it("should deliver messages to the callback", async () => {
        const queue = new AsyncQueue(makeSettings());
        const received: unknown[] = [];

        const callback = vi.fn(
            (job: unknown, done: (err?: Error | null) => void) => {
                received.push(job);
                done();
            },
        );

        queue.process("worker", 2, callback);
        queue.emit("worker", { msg: "one" });
        queue.emit("worker", { msg: "two" });

        // async.queue processes asynchronously; wait a tick
        await new Promise((r) => setTimeout(r, 50));

        expect(callback).toHaveBeenCalledTimes(2);
        expect(received).toHaveLength(2);
        expect(received[0]).toEqual({ msg: "one" });
        expect(received[1]).toEqual({ msg: "two" });
    });

    // @spec QUEUE-REPLAY-002, ASYNC-DELIVERY-002
    it("should handle errors in the callback without crashing", async () => {
        const queue = new AsyncQueue(makeSettings());

        const callback = vi.fn(() => {
            throw new Error("boom");
        });

        queue.process("errBlock", 1, callback);
        queue.emit("errBlock", { data: 1 });

        await new Promise((r) => setTimeout(r, 50));

        expect(callback).toHaveBeenCalledTimes(1);
    });

    // @spec ASYNC-CONCURRENCY-001
    it("should respect concurrency", async () => {
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
            },
        );

        queue.process("concBlock", 2, callback);

        // Push 4 jobs
        for (let i = 0; i < 4; i++) {
            queue.emit("concBlock", { i });
        }

        await new Promise((r) => setTimeout(r, 200));

        expect(callback).toHaveBeenCalledTimes(4);
        // Max concurrency should not exceed 2
        expect(maxConcurrent).toBeLessThanOrEqual(2);
    });

    // @spec QUEUE-REPLAY-001
    it("should replay a failed message up to replayLimit times", async () => {
        const queue = new AsyncQueue(
            makeSettings({ replay: true, replayLimit: 3, replayTime: 5 }),
        );
        const callback = vi.fn(
            (_job: unknown, done: (err?: Error | null) => void) => {
                done(new Error("always fails"));
            },
        );

        queue.process("retryBlock", 1, callback);
        await queue.emit("retryBlock", { data: 1 });

        await vi.waitFor(() => expect(callback).toHaveBeenCalledTimes(3));
        await new Promise((r) => setTimeout(r, 30));

        expect(callback).toHaveBeenCalledTimes(3);
    });

    // @spec QUEUE-REPLAY-001
    it("should stop replaying once a delivery succeeds", async () => {
        const queue = new AsyncQueue(
            makeSettings({ replay: true, replayLimit: 5, replayTime: 5 }),
        );
        let calls = 0;
        const callback = vi.fn(
            (_job: unknown, done: (err?: Error | null) => void) => {
                calls++;
                done(calls === 1 ? new Error("first time only") : null);
            },
        );

        queue.process("flakyBlock", 1, callback);
        await queue.emit("flakyBlock", { data: 1 });

        await vi.waitFor(() => expect(callback).toHaveBeenCalledTimes(2));
        await new Promise((r) => setTimeout(r, 30));

        expect(callback).toHaveBeenCalledTimes(2);
    });

    // @spec QUEUE-DONE-001
    it("should ignore a second call to done", async () => {
        const queue = new AsyncQueue(makeSettings());
        const callback = vi.fn(
            (_job: unknown, done: (err?: Error | null) => void) => {
                done();
                done();
            },
        );

        queue.process("twiceBlock", 1, callback);
        await queue.emit("twiceBlock", { n: 1 });
        await queue.emit("twiceBlock", { n: 2 });

        await vi.waitFor(() => expect(callback).toHaveBeenCalledTimes(2));
    });

    // @spec QUEUE-CLOSE-001, ASYNC-CLOSE-001
    it("close should cancel pending replays and stop delivering", async () => {
        const queue = new AsyncQueue(
            makeSettings({ replay: true, replayLimit: 3, replayTime: 150 }),
        );
        const callback = vi.fn(
            (_job: unknown, done: (err?: Error | null) => void) => {
                done(new Error("fails"));
            },
        );

        queue.process("closeBlock", 1, callback);
        await queue.emit("closeBlock", { data: 1 });
        await vi.waitFor(() => expect(callback).toHaveBeenCalledTimes(1));

        await queue.close();
        await new Promise((r) => setTimeout(r, 250));

        expect(callback).toHaveBeenCalledTimes(1);
        await expect(queue.emit("closeBlock", { data: 2 })).rejects.toThrow(
            /not defined/,
        );
    });

    // @spec QUEUE-NAME-001
    it("should work with different cloud names (prefix isolation)", async () => {
        const q1 = new AsyncQueue(makeSettings({ cloudName: "cloud-a" }));
        const q2 = new AsyncQueue(makeSettings({ cloudName: "cloud-b" }));

        const cb1 = vi.fn();
        const cb2 = vi.fn();

        q1.process("block", 1, cb1);
        q2.process("block", 1, cb2);

        // Emitting to cloud-a should not reach cloud-b
        await q1.emit("block", { from: "a" });
        await new Promise((r) => setTimeout(r, 20));

        expect(cb1).toHaveBeenCalledTimes(1);
        expect(cb2).not.toHaveBeenCalled();
    });

    describe("close", () => {
        type Done = (err?: Error | null) => void;

        // @spec QUEUE-CLOSE-001
        it("should wait for a message in progress", async () => {
            const queue = new AsyncQueue(makeSettings());
            let finish: Done = () => {};
            queue.process("slow", 1, (_job: unknown, done: Done) => {
                finish = done;
            });
            await queue.emit("slow", { n: 1 });
            await new Promise((r) => setTimeout(r, 10));

            let closed = false;
            const closing = queue.close(1000).then(() => (closed = true));
            await new Promise((r) => setTimeout(r, 40));
            expect(closed).toBe(false);

            finish();
            await closing;
            expect(closed).toBe(true);
        });

        // @spec QUEUE-CLOSE-001
        it("should not start queued messages once closing", async () => {
            const queue = new AsyncQueue(makeSettings());
            let finish: Done = () => {};
            const callback = vi.fn((_job: unknown, done: Done) => {
                finish = done;
            });
            queue.process("busy", 1, callback);
            await queue.emit("busy", { n: 1 });
            await queue.emit("busy", { n: 2 });
            await new Promise((r) => setTimeout(r, 10));

            const closing = queue.close(1000);
            finish();
            await closing;

            expect(callback).toHaveBeenCalledTimes(1);
        });

        // @spec QUEUE-CLOSE-003
        it("should give up waiting at the timeout", async () => {
            const queue = new AsyncQueue(makeSettings());
            queue.process("stuck", 1, () => {
                // never calls done
            });
            await queue.emit("stuck", { n: 1 });
            await new Promise((r) => setTimeout(r, 10));

            const before = Date.now();
            await queue.close(50);

            expect(Date.now() - before).toBeLessThan(500);
        });

        // @spec QUEUE-CLOSE-002
        it("should be safe to call twice", async () => {
            const queue = new AsyncQueue(makeSettings());
            queue.process("a", 1, vi.fn());

            const first = queue.close();
            expect(queue.close()).toBe(first);
            await first;
        });
    });
});
