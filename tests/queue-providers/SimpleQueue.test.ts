import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { NStormSettings } from "../../src/BaseBolt";

// ---- Mock ioredis ----
// By default a blocking pop behaves like a timeout on an empty queue: it
// waits a little, then returns null. Tests push raw payloads onto `pending`.
let pending: string[] = [];
const mockRpush = vi.fn();
const mockBlpop = vi.fn();
const mockKeys = vi.fn();
const mockDel = vi.fn();
const mockDisconnect = vi.fn();

vi.mock("ioredis", () => {
    const MockIORedis = vi.fn(function (this: Record<string, unknown>) {
        this.rpush = mockRpush;
        this.blpop = mockBlpop;
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

    it("should not open a Redis connection until it is needed", async () => {
        queue = new SimpleQueue(makeSettings());
        await queue.registerBlock({ name: "lazy" });

        expect(IORedis).not.toHaveBeenCalled();

        await queue.emit("lazy", { data: 1 });

        expect(IORedis).toHaveBeenCalledTimes(1);
    });

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
        it("should reject if block is not registered", async () => {
            queue = new SimpleQueue(makeSettings());
            await expect(queue.emit("missing", { data: 1 })).rejects.toThrow(
                /not defined/,
            );
        });

        it("should push to a key namespaced by cloud name", async () => {
            queue = new SimpleQueue(makeSettings());
            await queue.registerBlock({ name: "pushBlock" });

            await queue.emit("pushBlock", { val: 42 });

            expect(mockRpush).toHaveBeenCalledWith(
                "test-cloud-pushBlock:sq-jobs",
                envelope({ val: 42 }),
            );
        });

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

        it("should accept a bare message written by an older version", async () => {
            pending.push(JSON.stringify({ legacy: true }));
            queue = new SimpleQueue(makeSettings());
            await queue.registerBlock({ name: "old" });

            const received: unknown[] = [];
            queue.process("old", 1, (job: unknown, done: Done) => {
                received.push(job);
                done();
            });

            await vi.waitFor(() =>
                expect(received).toEqual([{ legacy: true }]),
            );
        });

        it("should drop an unparseable message and keep going", async () => {
            pending.push("{not json", envelope({ ok: true }));
            queue = new SimpleQueue(makeSettings());
            await queue.registerBlock({ name: "junk" });

            const received: unknown[] = [];
            queue.process("junk", 1, (job: unknown, done: Done) => {
                received.push(job);
                done();
            });

            await vi.waitFor(() => expect(received).toEqual([{ ok: true }]));
        });

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

        it("should re-queue when the callback throws", async () => {
            pending.push(envelope({ job: 1 }));
            queue = new SimpleQueue(makeSettings());
            await queue.registerBlock({ name: "throws" });

            queue.process("throws", 1, () => {
                throw new Error("exploded");
            });

            await vi.waitFor(() => expect(mockRpush).toHaveBeenCalledTimes(1));
        });

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

        it("close should cancel a pending replay", async () => {
            pending.push(envelope({ job: 1 }));
            queue = new SimpleQueue(makeSettings({ replayTime: 150 }));
            await queue.registerBlock({ name: "pendingRetry" });

            const callback = vi.fn((_job: unknown, done: Done) => {
                done(new Error("failed"));
            });
            queue.process("pendingRetry", 1, callback);
            await vi.waitFor(() => expect(callback).toHaveBeenCalledTimes(1));

            await queue.close();
            await tick(250);

            expect(mockRpush).not.toHaveBeenCalled();
        });
    });
});
