import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { NStormSettings } from "../../src/BaseBolt";

// ---- Mock ioredis ----
// By default a blocking pop behaves like a timeout on an empty queue: it
// waits a little, then returns null. Tests push raw payloads onto `pending`.
let pending: string[] = [];
const mockRpush = vi.fn();
const mockBlpop = vi.fn();
const mockLpush = vi.fn();
const mockKeys = vi.fn();
const mockDel = vi.fn();
const mockDisconnect = vi.fn();

vi.mock("ioredis", () => {
    const MockIORedis = vi.fn(function (this: Record<string, unknown>) {
        this.rpush = mockRpush;
        this.blpop = mockBlpop;
        this.lpush = mockLpush;
        this.keys = mockKeys;
        this.del = mockDel;
        this.disconnect = mockDisconnect;
    });
    return { default: MockIORedis };
});

import IORedis from "ioredis";
import { SimpleQueue } from "../../src/queue-providers/SimpleQueue";

type Done = (err?: Error | null) => void;

function makeSettings(overrides?: Partial<NStormSettings>): NStormSettings {
    return {
        cloudName: "test-cloud",
        queue: "simple",
        debug: false,
        redis: { port: 6379, host: "127.0.0.1" },
        replay: true,
        replayLimit: 3,
        replayTime: 5,
        processTimeout: 0,
        ...overrides,
    };
}

const envelope = (data: unknown, attempt = 1): string =>
    JSON.stringify({ data, attempt });

const tick = (ms = 20): Promise<void> => new Promise((r) => setTimeout(r, ms));

describe("SimpleQueue", () => {
    let queue: SimpleQueue;

    beforeEach(() => {
        vi.clearAllMocks();
        pending = [];
        mockRpush.mockResolvedValue(1);
        mockDel.mockResolvedValue(1);
        mockLpush.mockResolvedValue(1);
        mockBlpop.mockImplementation(async (key: string) => {
            const next = pending.shift();
            if (next !== undefined) return [key, next];
            await tick(5);
            return null;
        });
    });

    afterEach(async () => {
        await queue?.close();
    });

    // @spec QUEUE-CONNECT-001
    it("should not open a Redis connection until it is needed", async () => {
        queue = new SimpleQueue(makeSettings());
        await queue.registerBlock({ name: "lazy" });

        expect(IORedis).not.toHaveBeenCalled();

        await queue.emit("lazy", { data: 1 });

        expect(IORedis).toHaveBeenCalledTimes(1);
    });

    // @spec SIMPLE-REDIS-001
    it("should pass the redis username and password through", async () => {
        queue = new SimpleQueue(
            makeSettings({
                redis: {
                    host: "redis.local",
                    port: 6380,
                    user: "worker",
                    password: "secret",
                },
            }),
        );
        await queue.registerBlock({ name: "auth" });
        await queue.emit("auth", { data: 1 });

        expect(IORedis).toHaveBeenCalledWith(
            expect.objectContaining({
                host: "redis.local",
                port: 6380,
                username: "worker",
                password: "secret",
            }),
        );
    });

    describe("emit", () => {
        // @spec QUEUE-EMIT-001
        it("should reject if block is not registered", async () => {
            queue = new SimpleQueue(makeSettings());
            await expect(queue.emit("missing", { data: 1 })).rejects.toThrow(
                /not defined/,
            );
        });

        // @spec QUEUE-NAME-001, SIMPLE-ENVELOPE-001
        it("should push to a key namespaced by cloud name", async () => {
            queue = new SimpleQueue(makeSettings());
            await queue.registerBlock({ name: "pushBlock" });

            await queue.emit("pushBlock", { val: 42 });

            expect(mockRpush).toHaveBeenCalledWith(
                "test-cloud-pushBlock:sq-jobs",
                envelope({ val: 42 }),
            );
        });

        // @spec SIMPLE-KEY-001
        it("should apply redis.prefix to the key", async () => {
            queue = new SimpleQueue(
                makeSettings({
                    redis: { host: "127.0.0.1", port: 6379, prefix: "app" },
                }),
            );
            await queue.registerBlock({ name: "b" });
            await queue.emit("b", { val: 1 });

            expect(mockRpush).toHaveBeenCalledWith(
                "app:test-cloud-b:sq-jobs",
                expect.any(String),
            );
        });

        // @spec QUEUE-NAME-001
        it("should keep same-named blocks in different clouds apart", async () => {
            queue = new SimpleQueue(makeSettings({ cloudName: "cloud-a" }));
            const other = new SimpleQueue(
                makeSettings({ cloudName: "cloud-b" }),
            );
            await queue.registerBlock({ name: "block" });
            await other.registerBlock({ name: "block" });

            await queue.emit("block", { from: "a" });
            await other.emit("block", { from: "b" });
            await other.close();

            const keys = mockRpush.mock.calls.map((c) => c[0]);
            expect(keys).toEqual([
                "cloud-a-block:sq-jobs",
                "cloud-b-block:sq-jobs",
            ]);
        });

        // @spec QUEUE-EMIT-002
        it("should reject when Redis refuses the write", async () => {
            mockRpush.mockRejectedValueOnce(new Error("redis down"));
            queue = new SimpleQueue(makeSettings());
            await queue.registerBlock({ name: "b" });

            await expect(queue.emit("b", { val: 1 })).rejects.toThrow(
                "redis down",
            );
        });
    });

    describe("process", () => {
        it("should throw if block is not registered", () => {
            queue = new SimpleQueue(makeSettings());
            expect(() => queue.process("missing", 1, vi.fn())).toThrow(
                /not defined/,
            );
        });

        it("should deliver queued messages to the callback", async () => {
            pending.push(envelope({ msg: "hello" }));
            queue = new SimpleQueue(makeSettings());
            await queue.registerBlock({ name: "pollBlock" });

            const received: unknown[] = [];
            queue.process("pollBlock", 1, (job: unknown, done: Done) => {
                received.push(job);
                done();
            });

            await vi.waitFor(() => expect(received).toHaveLength(1));

            expect(received[0]).toEqual({ msg: "hello" });
            expect(mockBlpop).toHaveBeenCalledWith(
                "test-cloud-pollBlock:sq-jobs",
                expect.any(Number),
            );
        });

        // @spec SIMPLE-ENVELOPE-002
        it("should drop an unparseable message and keep going", async () => {
            pending.push(
                "{not json",
                JSON.stringify({ no: "envelope" }),
                envelope({ ok: true }),
            );
            queue = new SimpleQueue(makeSettings());
            await queue.registerBlock({ name: "junk" });

            const received: unknown[] = [];
            queue.process("junk", 1, (job: unknown, done: Done) => {
                received.push(job);
                done();
            });

            await vi.waitFor(() => expect(received).toEqual([{ ok: true }]));
        });

        // @spec SIMPLE-CONCURRENCY-001
        it("should respect the concurrency limit", async () => {
            for (let i = 0; i < 5; i++) pending.push(envelope({ i }));
            queue = new SimpleQueue(makeSettings());
            await queue.registerBlock({ name: "conc" });

            let concurrent = 0;
            let maxConcurrent = 0;
            let completed = 0;

            queue.process("conc", 2, (_job: unknown, done: Done) => {
                concurrent++;
                maxConcurrent = Math.max(maxConcurrent, concurrent);
                setTimeout(() => {
                    concurrent--;
                    completed++;
                    done();
                }, 10);
            });

            await vi.waitFor(() => expect(completed).toBe(5));
            expect(maxConcurrent).toBe(2);
        });

        // @spec QUEUE-DONE-001
        it("should not exceed the limit when done is called twice", async () => {
            for (let i = 0; i < 4; i++) pending.push(envelope({ i }));
            queue = new SimpleQueue(makeSettings());
            await queue.registerBlock({ name: "twice" });

            let concurrent = 0;
            let maxConcurrent = 0;
            let completed = 0;

            queue.process("twice", 1, (_job: unknown, done: Done) => {
                concurrent++;
                maxConcurrent = Math.max(maxConcurrent, concurrent);
                setTimeout(() => {
                    concurrent--;
                    completed++;
                    done();
                    done();
                }, 10);
            });

            await vi.waitFor(() => expect(completed).toBe(4));
            expect(maxConcurrent).toBe(1);
        });

        // @spec QUEUE-REPLAY-001, SIMPLE-REPLAY-001
        it("should re-queue a failed message with the attempt incremented", async () => {
            pending.push(envelope({ job: 1 }));
            queue = new SimpleQueue(makeSettings());
            await queue.registerBlock({ name: "retry" });

            queue.process("retry", 1, (_job: unknown, done: Done) => {
                done(new Error("failed"));
            });

            await vi.waitFor(() =>
                expect(mockRpush).toHaveBeenCalledWith(
                    "test-cloud-retry:sq-jobs",
                    envelope({ job: 1 }, 2),
                ),
            );
        });

        // @spec QUEUE-REPLAY-001
        it("should re-queue when the callback throws", async () => {
            pending.push(envelope({ job: 1 }));
            queue = new SimpleQueue(makeSettings());
            await queue.registerBlock({ name: "throws" });

            queue.process("throws", 1, () => {
                throw new Error("exploded");
            });

            await vi.waitFor(() => expect(mockRpush).toHaveBeenCalledTimes(1));
        });

        // @spec QUEUE-REPLAY-001
        it("should drop a message that has used up replayLimit", async () => {
            pending.push(envelope({ job: 1 }, 3));
            queue = new SimpleQueue(makeSettings({ replayLimit: 3 }));
            await queue.registerBlock({ name: "spent" });

            const callback = vi.fn((_job: unknown, done: Done) => {
                done(new Error("failed"));
            });
            queue.process("spent", 1, callback);

            await vi.waitFor(() => expect(callback).toHaveBeenCalledTimes(1));
            await tick(30);

            expect(mockRpush).not.toHaveBeenCalled();
        });

        // @spec QUEUE-REPLAY-002
        it("should not replay at all when replay is off", async () => {
            pending.push(envelope({ job: 1 }));
            queue = new SimpleQueue(makeSettings({ replay: false }));
            await queue.registerBlock({ name: "noreplay" });

            const callback = vi.fn((_job: unknown, done: Done) => {
                done(new Error("failed"));
            });
            queue.process("noreplay", 1, callback);

            await vi.waitFor(() => expect(callback).toHaveBeenCalledTimes(1));
            await tick(30);

            expect(mockRpush).not.toHaveBeenCalled();
        });
    });

    describe("clear", () => {
        // @spec QUEUE-CLEAR-001, SIMPLE-CLEAR-001
        it("should delete only this topology's keys, without scanning", async () => {
            queue = new SimpleQueue(makeSettings());
            await queue.registerBlock({ name: "block1" });
            await queue.registerBlock({ name: "block2" });

            await queue.clear();

            expect(mockKeys).not.toHaveBeenCalled();
            expect(mockDel).toHaveBeenCalledTimes(1);
            expect(mockDel).toHaveBeenCalledWith(
                "test-cloud-block1:sq-jobs",
                "test-cloud-block2:sq-jobs",
            );
        });

        it("should do nothing when no blocks are registered", async () => {
            queue = new SimpleQueue(makeSettings());
            await queue.clear();

            expect(mockDel).not.toHaveBeenCalled();
        });
    });

    describe("lifecycle", () => {
        // @spec QUEUE-SIGNAL-001
        it("start should not install signal handlers", async () => {
            const onceSpy = vi.spyOn(process, "once");
            const onSpy = vi.spyOn(process, "on");
            queue = new SimpleQueue(makeSettings());

            await queue.start();

            expect(onceSpy).not.toHaveBeenCalled();
            expect(onSpy).not.toHaveBeenCalled();
            onceSpy.mockRestore();
            onSpy.mockRestore();
        });

        // @spec QUEUE-CLOSE-001
        it("close should stop polling and disconnect every connection", async () => {
            queue = new SimpleQueue(makeSettings());
            await queue.registerBlock({ name: "a" });
            await queue.registerBlock({ name: "b" });
            await queue.emit("a", { x: 1 }); // shared client
            queue.process("a", 1, vi.fn()); // blocking client
            queue.process("b", 1, vi.fn()); // blocking client

            await tick();
            await queue.close();

            expect(mockDisconnect).toHaveBeenCalledTimes(3);

            await tick();
            const callsAfterClose = mockBlpop.mock.calls.length;
            await tick(40);
            expect(mockBlpop.mock.calls.length).toBe(callsAfterClose);
        });
    });

    describe("draining on close", () => {
        // @spec QUEUE-CLOSE-001
        it("should wait for a message in progress before disconnecting", async () => {
            pending.push(envelope({ job: 1 }));
            queue = new SimpleQueue(makeSettings());
            await queue.registerBlock({ name: "slow" });

            let finish: Done = () => {};
            const callback = vi.fn((_job: unknown, done: Done) => {
                finish = done;
            });
            queue.process("slow", 1, callback);
            await vi.waitFor(() => expect(callback).toHaveBeenCalledTimes(1));

            let closed = false;
            const closing = queue.close(2000).then(() => (closed = true));
            await tick(60);
            expect(closed).toBe(false);
            expect(mockDisconnect).not.toHaveBeenCalled();

            finish();
            await closing;
            expect(mockDisconnect).toHaveBeenCalled();
        });

        // @spec QUEUE-CLOSE-003
        it("should give up waiting at the timeout", async () => {
            pending.push(envelope({ job: 1 }));
            queue = new SimpleQueue(makeSettings());
            await queue.registerBlock({ name: "stuck" });

            const callback = vi.fn(() => {
                // never calls done
            });
            queue.process("stuck", 1, callback);
            await vi.waitFor(() => expect(callback).toHaveBeenCalledTimes(1));

            await queue.close(50);

            expect(mockDisconnect).toHaveBeenCalled();
        });

        // @spec SIMPLE-CLOSE-001
        it("should put a message waiting for replay straight back", async () => {
            pending.push(envelope({ job: 1 }));
            queue = new SimpleQueue(makeSettings({ replayTime: 60_000 }));
            await queue.registerBlock({ name: "waiting" });

            const callback = vi.fn((_job: unknown, done: Done) => {
                done(new Error("failed"));
            });
            queue.process("waiting", 1, callback);
            await vi.waitFor(() => expect(callback).toHaveBeenCalledTimes(1));
            expect(mockRpush).not.toHaveBeenCalled();

            await queue.close();

            expect(mockRpush).toHaveBeenCalledWith(
                "test-cloud-waiting:sq-jobs",
                envelope({ job: 1 }, 2),
            );
        });

        // @spec SIMPLE-CLOSE-001
        it("should put back at once a message that fails while closing", async () => {
            pending.push(envelope({ job: 1 }));
            queue = new SimpleQueue(makeSettings({ replayTime: 60_000 }));
            await queue.registerBlock({ name: "failing" });

            let finish: Done = () => {};
            const callback = vi.fn((_job: unknown, done: Done) => {
                finish = done;
            });
            queue.process("failing", 1, callback);
            await vi.waitFor(() => expect(callback).toHaveBeenCalledTimes(1));

            const closing = queue.close(2000);
            await tick(30);
            finish(new Error("failed during shutdown"));
            await closing;

            expect(mockRpush).toHaveBeenCalledWith(
                "test-cloud-failing:sq-jobs",
                envelope({ job: 1 }, 2),
            );
        });

        // @spec SIMPLE-CLOSE-002
        it("should hand back a message that arrives after closing began", async () => {
            const raw = envelope({ late: true });
            mockBlpop.mockImplementation(async (key: string) => {
                await tick(40);
                return [key, raw];
            });
            queue = new SimpleQueue(makeSettings());
            await queue.registerBlock({ name: "late" });

            const callback = vi.fn();
            queue.process("late", 1, callback);
            await tick(10);
            await queue.close();

            expect(callback).not.toHaveBeenCalled();
            expect(mockLpush).toHaveBeenCalledWith(
                "test-cloud-late:sq-jobs",
                raw,
            );
        });

        // @spec QUEUE-CLOSE-001
        it("should not reconnect when used after close", async () => {
            queue = new SimpleQueue(makeSettings());
            await queue.registerBlock({ name: "a" });
            await queue.emit("a", { n: 1 });
            await queue.close();
            const connections = vi.mocked(IORedis).mock.calls.length;

            await expect(queue.emit("a", { n: 2 })).rejects.toThrow(
                /has been closed/,
            );
            await expect(queue.clear()).rejects.toThrow(/has been closed/);

            expect(vi.mocked(IORedis).mock.calls.length).toBe(connections);
        });

        // @spec QUEUE-CLOSE-002
        it("should be safe to call twice", async () => {
            queue = new SimpleQueue(makeSettings());
            await queue.registerBlock({ name: "a" });
            queue.process("a", 1, vi.fn());

            const first = queue.close();
            expect(queue.close()).toBe(first);
            await first;

            expect(mockDisconnect).toHaveBeenCalledTimes(1);
        });
    });
});
