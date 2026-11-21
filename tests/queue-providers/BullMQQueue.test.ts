import { describe, it, expect, vi, beforeEach } from "vitest";
import { NStormSettings } from "../../src/BaseBolt";

// ---- Mock bullmq ----
const mockAdd = vi.fn().mockResolvedValue({ id: "1" });
const mockObliterate = vi.fn().mockResolvedValue(undefined);
const mockQueueClose = vi.fn().mockResolvedValue(undefined);
const mockWorkerClose = vi.fn().mockResolvedValue(undefined);
const mockWorkerOn = vi.fn();
const mockEventsClose = vi.fn().mockResolvedValue(undefined);
const mockEventsOn = vi.fn();

let capturedWorkerProcessor:
    ((job: { data: unknown }) => Promise<void>) | null = null;

vi.mock("bullmq", () => {
    const MockQueue = vi.fn(function (this: Record<string, unknown>) {
        this.add = mockAdd;
        this.obliterate = mockObliterate;
        this.close = mockQueueClose;
    });

    const MockWorker = vi.fn(function (
        this: Record<string, unknown>,
        _name: string,
        processor: unknown,
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
// BullMQQueue must never open a connection of its own; BullMQ does that.
const mockIORedis = vi.fn(function (this: Record<string, unknown>) {
    this.disconnect = vi.fn();
});
vi.mock("ioredis", () => ({ default: mockIORedis }));

import { BullMQQueue } from "../../src/queue-providers/BullMQQueue";
import { Queue, Worker, QueueEvents } from "bullmq";

function makeSettings(overrides?: Partial<NStormSettings>): NStormSettings {
    return {
        cloudName: "test-cloud",
        queue: "bullmq",
        debug: false,
        redis: { port: 6379, host: "127.0.0.1" },
        replay: true,
        replayLimit: 3,
        replayTime: 300,
        ...overrides,
    };
}

describe("BullMQQueue", () => {
    beforeEach(() => {
        vi.clearAllMocks();
        capturedWorkerProcessor = null;
    });

    // @spec QUEUE-CONNECT-001
    it("should construct without opening any Redis connection", () => {
        const queue = new BullMQQueue(makeSettings());
        expect(queue).toBeInstanceOf(BullMQQueue);
        expect(mockIORedis).not.toHaveBeenCalled();
        expect(Queue).not.toHaveBeenCalled();
    });

    it("should log when debug is true", () => {
        // Should not throw even with debug on
        const queue = new BullMQQueue(makeSettings({ debug: true }));
        expect(queue).toBeDefined();
    });

    describe("registerBlock", () => {
        // @spec QUEUE-NAME-001, QUEUE-REPLAY-001, BULLMQ-DELIVERY-001
        it("should create a BullMQ Queue for the block", async () => {
            const queue = new BullMQQueue(makeSettings());
            await queue.registerBlock({ name: "myBlock" });

            expect(Queue).toHaveBeenCalledWith(
                "test-cloud-myBlock",
                expect.objectContaining({
                    connection: expect.objectContaining({
                        host: "127.0.0.1",
                        port: 6379,
                    }),
                    defaultJobOptions: expect.objectContaining({
                        attempts: 3,
                        removeOnComplete: true,
                    }),
                }),
            );
        });

        // @spec QUEUE-REPLAY-002
        it("should make a single attempt when replay is off", async () => {
            const queue = new BullMQQueue(makeSettings({ replay: false }));
            await queue.registerBlock({ name: "noReplay" });

            expect(Queue).toHaveBeenCalledWith(
                "test-cloud-noReplay",
                expect.objectContaining({
                    defaultJobOptions: expect.objectContaining({ attempts: 1 }),
                }),
            );
        });

        // @spec BULLMQ-REDIS-001
        it("should pass redis user and prefix through to BullMQ", async () => {
            const queue = new BullMQQueue(
                makeSettings({
                    redis: {
                        host: "h",
                        port: 1,
                        user: "worker",
                        prefix: "app",
                    },
                }),
            );
            await queue.registerBlock({ name: "auth" });

            expect(Queue).toHaveBeenCalledWith(
                "test-cloud-auth",
                expect.objectContaining({
                    prefix: "app",
                    connection: expect.objectContaining({ username: "worker" }),
                }),
            );
        });
    });

    describe("start", () => {
        // @spec QUEUE-SIGNAL-001
        it("should not install signal handlers", async () => {
            const onceSpy = vi.spyOn(process, "once");
            const onSpy = vi.spyOn(process, "on");
            const queue = new BullMQQueue(makeSettings());
            await queue.start();

            expect(onceSpy).not.toHaveBeenCalled();
            expect(onSpy).not.toHaveBeenCalled();

            onceSpy.mockRestore();
            onSpy.mockRestore();
        });
    });

    describe("emit", () => {
        // @spec QUEUE-EMIT-001
        it("should reject if block queue is not registered", async () => {
            const queue = new BullMQQueue(makeSettings());
            await expect(queue.emit("missing", { data: 1 })).rejects.toThrow(
                /not defined/,
            );
        });

        // @spec QUEUE-EMIT-002
        it("should reject when BullMQ cannot add the job", async () => {
            mockAdd.mockRejectedValueOnce(new Error("redis down"));
            const queue = new BullMQQueue(makeSettings());
            await queue.registerBlock({ name: "failBlock" });

            await expect(queue.emit("failBlock", { val: 1 })).rejects.toThrow(
                "redis down",
            );
        });

        it("should call queue.add with correct params", async () => {
            const queue = new BullMQQueue(makeSettings());
            await queue.registerBlock({ name: "emitBlock" });

            await queue.emit("emitBlock", { val: 42 });

            // Retry options come from the queue's defaultJobOptions
            expect(mockAdd).toHaveBeenCalledWith("emitBlock", { val: 42 });
        });
    });

    describe("process", () => {
        // @spec BULLMQ-WORKER-001
        it("should create a Worker for the block", async () => {
            const queue = new BullMQQueue(makeSettings());
            await queue.registerBlock({ name: "procBlock" });

            const callback = vi.fn();
            queue.process("procBlock", 5, callback);

            expect(Worker).toHaveBeenCalledWith(
                "test-cloud-procBlock",
                expect.any(Function),
                expect.objectContaining({
                    concurrency: 5,
                    connection: expect.objectContaining({
                        host: "127.0.0.1",
                        port: 6379,
                    }),
                }),
            );

            expect(mockWorkerOn).toHaveBeenCalledWith(
                "error",
                expect.any(Function),
            );
        });

        // @spec BULLMQ-DEBUG-001
        it("should create QueueEvents when debug is true", async () => {
            const queue = new BullMQQueue(makeSettings({ debug: true }));
            await queue.registerBlock({ name: "debugBlock" });

            const callback = vi.fn();
            queue.process("debugBlock", 1, callback);

            expect(QueueEvents).toHaveBeenCalledWith(
                "test-cloud-debugBlock",
                expect.objectContaining({
                    connection: expect.any(Object),
                }),
            );
        });

        // @spec BULLMQ-DELIVERY-002
        it("should invoke callback when worker processes a job", async () => {
            const queue = new BullMQQueue(makeSettings());
            await queue.registerBlock({ name: "callbackBlock" });

            const callback = vi.fn(
                (_job: unknown, done: (err?: Error | null) => void) => {
                    done();
                },
            );

            queue.process("callbackBlock", 1, callback);

            // Simulate the worker running a job
            expect(capturedWorkerProcessor).not.toBeNull();
            await capturedWorkerProcessor!({ data: { key: "value" } });

            expect(callback).toHaveBeenCalledWith(
                { key: "value" },
                expect.any(Function),
            );
        });

        // @spec BULLMQ-DELIVERY-003
        it("should reject when callback calls done with error", async () => {
            const queue = new BullMQQueue(makeSettings());
            await queue.registerBlock({ name: "errBlock" });

            const callback = vi.fn(
                (_job: unknown, done: (err?: Error | null) => void) => {
                    done(new Error("job failed"));
                },
            );

            queue.process("errBlock", 1, callback);

            await expect(
                capturedWorkerProcessor!({ data: { bad: true } }),
            ).rejects.toThrow("job failed");
        });

        // @spec BULLMQ-DELIVERY-003
        it("should reject when callback throws", async () => {
            const queue = new BullMQQueue(makeSettings());
            await queue.registerBlock({ name: "throwBlock" });

            const callback = vi.fn(() => {
                throw new Error("callback exploded");
            });

            queue.process("throwBlock", 1, callback);

            await expect(
                capturedWorkerProcessor!({ data: {} }),
            ).rejects.toThrow("callback exploded");
        });
    });

    describe("clear", () => {
        // @spec QUEUE-CLEAR-001
        it("should obliterate all registered queues", async () => {
            const queue = new BullMQQueue(makeSettings());
            await queue.registerBlock({ name: "a" });
            await queue.registerBlock({ name: "b" });

            await queue.clear();

            expect(mockObliterate).toHaveBeenCalledTimes(2);
            expect(mockObliterate).toHaveBeenCalledWith({ force: true });
        });
    });

    describe("close", () => {
        // @spec QUEUE-CLOSE-001
        it("should close all workers, events, queues, and disconnect", async () => {
            const queue = new BullMQQueue(makeSettings({ debug: true }));
            await queue.registerBlock({ name: "closeBlock" });

            const callback = vi.fn();
            queue.process("closeBlock", 1, callback);

            await queue.close();

            expect(mockWorkerClose).toHaveBeenCalled();
            expect(mockEventsClose).toHaveBeenCalled();
            expect(mockQueueClose).toHaveBeenCalled();
        });

        // @spec QUEUE-CLOSE-002
        it("should be safe to call twice", async () => {
            const queue = new BullMQQueue(makeSettings());
            await queue.registerBlock({ name: "twice" });
            queue.process("twice", 1, vi.fn());

            await queue.close();
            await queue.close();

            expect(mockWorkerClose).toHaveBeenCalledTimes(1);
            expect(mockQueueClose).toHaveBeenCalledTimes(1);
        });
    });
});
