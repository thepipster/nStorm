import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { BaseBolt } from "../src/BaseBolt";
import type { NStormSettings } from "../src/BaseBolt";

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

describe("BaseBolt", () => {
    let bolt: BaseBolt;

    beforeEach(() => {
        bolt = new BaseBolt();
    });

    describe("constructor", () => {
        it("should initialize with default values", () => {
            expect(bolt.inputs).toEqual([]);
            expect(bolt.subscribers).toEqual([]);
            expect(bolt.limit).toBe(1);
            expect(bolt.context).toBeNull();
            expect(bolt.debug).toBe(false);
            expect(bolt.name).toBe("");
        });
    });

    describe("setup", () => {
        it("should set name, context, limit, and debug", () => {
            const ctx = makeMockContext();
            bolt.setup({ name: "testBolt", limit: 5, debug: true }, ctx as any);

            expect(bolt.name).toBe("testBolt");
            expect(bolt.context).toBe(ctx);
            expect(bolt.limit).toBe(5);
            expect(bolt.debug).toBe(true);
        });

        it("should default limit to 1 when not provided", () => {
            const ctx = makeMockContext();
            bolt.setup({ name: "noLimit" }, ctx as any);

            expect(bolt.limit).toBe(1);
        });

        it("should default debug to false when not provided", () => {
            const ctx = makeMockContext();
            bolt.setup({ name: "noDebug" }, ctx as any);

            expect(bolt.debug).toBe(false);
        });
    });

    describe("_init", () => {
        it("should call context.queue.process with name, limit, and a callback", () => {
            const ctx = makeMockContext();
            bolt.setup({ name: "initBolt", limit: 3 }, ctx as any);
            bolt._init();

            expect(ctx._mockProcess).toHaveBeenCalledWith(
                "initBolt",
                3,
                expect.any(Function),
            );
        });

        it("should forward jobs to the process method", () => {
            const ctx = makeMockContext();
            bolt.setup({ name: "fwdBolt" }, ctx as any);

            const processSpy = vi
                .spyOn(bolt, "process")
                .mockResolvedValue(undefined);
            bolt._init();

            // Get the callback that was registered
            const registeredCallback = ctx._mockProcess.mock.calls[0][2];
            const mockDone = vi.fn();
            registeredCallback({ data: "test" }, mockDone);

            expect(processSpy).toHaveBeenCalledWith(
                { data: "test" },
                expect.any(Function),
            );
        });

        // @spec BOLT-PROCESS-001
        it("should fail the job when process returns a rejected promise", async () => {
            const ctx = makeMockContext();
            bolt.setup({ name: "rejectBolt" }, ctx as any);

            const error = new Error("async failure");
            vi.spyOn(bolt, "process").mockRejectedValue(error);
            bolt._init();

            const registeredCallback = ctx._mockProcess.mock.calls[0][2];
            const mockDone = vi.fn();
            registeredCallback({}, mockDone);

            await vi.waitFor(() =>
                expect(mockDone).toHaveBeenCalledWith(error),
            );
        });

        // @spec BOLT-PROCESS-002
        it("should only report completion once, however often done is called", async () => {
            const ctx = makeMockContext();
            bolt.setup({ name: "twiceBolt" }, ctx as any);

            vi.spyOn(bolt, "process").mockImplementation(async (_msg, done) => {
                done();
                done();
                throw new Error("after done");
            });
            bolt._init();

            const registeredCallback = ctx._mockProcess.mock.calls[0][2];
            const mockDone = vi.fn();
            registeredCallback({}, mockDone);

            await new Promise((r) => setTimeout(r, 0));

            expect(mockDone).toHaveBeenCalledTimes(1);
            expect(mockDone).toHaveBeenCalledWith(undefined);
        });

        it("should throw a clear error when the block is not in a topology", () => {
            expect(() => bolt._init()).toThrow(/not been added to a topology/);
        });

        // @spec BOLT-PROCESS-001
        it("should catch errors thrown by process and call done with error", () => {
            const ctx = makeMockContext();
            bolt.setup({ name: "errBolt" }, ctx as any);

            const error = new Error("process failed");
            vi.spyOn(bolt, "process").mockImplementation(() => {
                throw error;
            });
            bolt._init();

            const registeredCallback = ctx._mockProcess.mock.calls[0][2];
            const mockDone = vi.fn();
            registeredCallback({}, mockDone);

            expect(mockDone).toHaveBeenCalledWith(error);
        });
    });

    describe("delay", () => {
        it('should resolve with "done" after the specified time', async () => {
            vi.useFakeTimers();
            const promise = bolt.delay(1000);

            vi.advanceTimersByTime(1000);
            const result = await promise;

            expect(result).toBe("done");
            vi.useRealTimers();
        });
    });

    describe("_isFiltered", () => {
        // @spec BOLT-FILTER-002
        it("should return true when no filter is provided", () => {
            expect(bolt._isFiltered({ key: "val" })).toBe(true);
            expect(bolt._isFiltered({ key: "val" }, undefined)).toBe(true);
        });

        // @spec BOLT-FILTER-001
        it("should return true when message matches filter", () => {
            expect(bolt._isFiltered({ coin: "heads" }, { coin: "heads" })).toBe(
                true,
            );
        });

        // @spec BOLT-FILTER-001
        it("should return false when message does not match filter", () => {
            expect(bolt._isFiltered({ coin: "heads" }, { coin: "tails" })).toBe(
                false,
            );
        });

        // @spec BOLT-FILTER-001
        it("should return false when a filter key is missing from the message", () => {
            expect(bolt._isFiltered({ other: "val" }, { coin: "heads" })).toBe(
                false,
            );
        });

        // @spec BOLT-FILTER-001
        it("should require every filter key to match", () => {
            expect(bolt._isFiltered({ a: 1, b: 2 }, { a: 1, b: 999 })).toBe(
                false,
            );
            expect(bolt._isFiltered({ a: 1, b: 2 }, { b: 2, a: 999 })).toBe(
                false,
            );
            expect(bolt._isFiltered({ a: 1, b: 2, c: 3 }, { a: 1, b: 2 })).toBe(
                true,
            );
        });

        // @spec BOLT-FILTER-001
        it("should not match on inherited properties", () => {
            expect(
                bolt._isFiltered({}, { toString: Object.prototype.toString }),
            ).toBe(false);
        });
    });

    describe("emit", () => {
        it("should send message to all subscribers via queue.emit", async () => {
            const ctx = makeMockContext();
            bolt.setup({ name: "emitBolt" }, ctx as any);

            bolt.subscribers = [{ name: "sub1" }, { name: "sub2" }];

            await bolt.emit({ data: 42 });

            expect(ctx._mockEmit).toHaveBeenCalledTimes(2);
            expect(ctx._mockEmit).toHaveBeenCalledWith("sub1", { data: 42 });
            expect(ctx._mockEmit).toHaveBeenCalledWith("sub2", { data: 42 });
        });

        // @spec BOLT-FILTER-001
        it("should skip subscribers whose filter does not match", async () => {
            const ctx = makeMockContext();
            bolt.setup({ name: "filterBolt" }, ctx as any);

            bolt.subscribers = [
                { name: "heads", options: { filter: { coin: "heads" } } },
                { name: "tails", options: { filter: { coin: "tails" } } },
            ];

            await bolt.emit({ coin: "heads" });

            expect(ctx._mockEmit).toHaveBeenCalledTimes(1);
            expect(ctx._mockEmit).toHaveBeenCalledWith("heads", {
                coin: "heads",
            });
        });

        // @spec BOLT-FILTER-002
        it("should send to subscribers with no filter", async () => {
            const ctx = makeMockContext();
            bolt.setup({ name: "noFilterBolt" }, ctx as any);

            bolt.subscribers = [{ name: "all" }];

            await bolt.emit({ anything: true });

            expect(ctx._mockEmit).toHaveBeenCalledTimes(1);
        });

        it("should handle empty subscribers list", async () => {
            const ctx = makeMockContext();
            bolt.setup({ name: "lonely" }, ctx as any);

            await bolt.emit({ data: 1 });

            expect(ctx._mockEmit).not.toHaveBeenCalled();
        });
    });

    describe("emit results", () => {
        // @spec BOLT-EMIT-001
        it("should resolve once every subscriber queue has accepted the message", async () => {
            const ctx = makeMockContext();
            ctx._mockEmit.mockResolvedValue(undefined);
            bolt.setup({ name: "okBolt" }, ctx as any);
            bolt.subscribers = [{ name: "sub1" }];

            await expect(bolt.emit({ data: 1 })).resolves.toBeUndefined();
        });

        // @spec BOLT-EMIT-002
        it("should reject when a queue cannot accept the message", async () => {
            const ctx = makeMockContext();
            ctx._mockEmit.mockRejectedValue(new Error("redis down"));
            bolt.setup({ name: "failBolt" }, ctx as any);
            bolt.subscribers = [{ name: "sub1" }];

            await expect(bolt.emit({ data: 1 })).rejects.toThrow("redis down");
        });

        // @spec BOLT-EMIT-003
        it("should not raise an unhandled rejection when the result is ignored", async () => {
            const unhandled = vi.fn();
            process.on("unhandledRejection", unhandled);

            const ctx = makeMockContext();
            ctx._mockEmit.mockRejectedValue(new Error("redis down"));
            bolt.setup({ name: "ignoredBolt" }, ctx as any);
            bolt.subscribers = [{ name: "sub1" }];

            void bolt.emit({ data: 1 });
            await new Promise((r) => setTimeout(r, 20));

            process.off("unhandledRejection", unhandled);
            expect(unhandled).not.toHaveBeenCalled();
        });

        // @spec BOLT-EMIT-004
        it("should reject when the block is not part of a topology", async () => {
            await expect(bolt.emit({ data: 1 })).rejects.toThrow(
                /not been added to a topology/,
            );
        });
    });

    describe("input", () => {
        it("should add an input source", () => {
            bolt.input("source1");

            expect(bolt.inputs).toEqual([
                { name: "source1", options: undefined },
            ]);
        });

        it("should add an input source with options", () => {
            bolt.input("source1", { filter: { coin: "heads" } });

            expect(bolt.inputs).toEqual([
                { name: "source1", options: { filter: { coin: "heads" } } },
            ]);
        });

        it("should return this for chaining", () => {
            const result = bolt.input("a").input("b");

            expect(result).toBe(bolt);
            expect(bolt.inputs).toHaveLength(2);
        });

        // @spec BOLT-INPUT-001
        it("should deduplicate inputs by name", () => {
            bolt.input("source1");
            bolt.input("source1");

            expect(bolt.inputs).toHaveLength(1);
        });

        // @spec BOLT-INPUT-001
        it("should keep first entry when deduplicating", () => {
            bolt.input("source1", { filter: { coin: "heads" } });
            bolt.input("source1", { filter: { coin: "tails" } });

            expect(bolt.inputs).toHaveLength(1);
            expect(bolt.inputs[0].options?.filter?.coin).toBe("heads");
        });
    });

    describe("process", () => {
        it("should resolve without error (base implementation)", async () => {
            const done = vi.fn();
            await expect(bolt.process({}, done)).resolves.toBeUndefined();
        });

        // @spec BOLT-PROCESS-003
        it("should acknowledge the message by default", async () => {
            const done = vi.fn();
            await bolt.process({}, done);
            expect(done).toHaveBeenCalledWith();
        });
    });

    describe("stop", () => {
        it("should resolve without error (base implementation)", async () => {
            await expect(bolt.stop()).resolves.toBeUndefined();
        });
    });

    describe("start", () => {
        it("should resolve without error (base implementation)", async () => {
            await expect(bolt.start()).resolves.toBeUndefined();
        });
    });

    describe("subclassing", () => {
        it("should allow overriding process", async () => {
            class MyBolt extends BaseBolt {
                public received: unknown[] = [];
                async process(
                    message: unknown,
                    done: (err?: Error | null) => void,
                ): Promise<void> {
                    this.received.push(message);
                    done();
                }
            }

            const myBolt = new MyBolt();
            const done = vi.fn();
            await myBolt.process({ hello: "world" }, done);

            expect(myBolt.received).toEqual([{ hello: "world" }]);
            expect(done).toHaveBeenCalledWith();
        });

        it("should allow overriding start", async () => {
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

    describe("processTimeout", () => {
        function timedContext(processTimeout: number) {
            const ctx = makeMockContext();
            ctx.settings = makeSettings({ processTimeout });
            return ctx;
        }

        afterEach(() => {
            vi.useRealTimers();
        });

        // @spec BOLT-PROCESS-004
        it("should fail a message that is not finished in time", () => {
            vi.useFakeTimers();
            const ctx = timedContext(1000);
            bolt.setup({ name: "slowBolt" }, ctx as any);
            vi.spyOn(bolt, "process").mockImplementation(
                () => new Promise<void>(() => {}),
            );
            bolt._init();

            const mockDone = vi.fn();
            ctx._mockProcess.mock.calls[0][2]({}, mockDone);

            vi.advanceTimersByTime(999);
            expect(mockDone).not.toHaveBeenCalled();

            vi.advanceTimersByTime(1);
            expect(mockDone).toHaveBeenCalledTimes(1);
            expect(mockDone.mock.calls[0][0].message).toMatch(
                /did not finish a message within 1000ms/,
            );
        });

        // @spec BOLT-PROCESS-004, BOLT-PROCESS-002
        it("should ignore a done that arrives after the timeout", () => {
            vi.useFakeTimers();
            const ctx = timedContext(1000);
            bolt.setup({ name: "lateBolt" }, ctx as any);
            let lateDone: (err?: Error | null) => void = () => {};
            vi.spyOn(bolt, "process").mockImplementation(async (_m, done) => {
                lateDone = done;
            });
            bolt._init();

            const mockDone = vi.fn();
            ctx._mockProcess.mock.calls[0][2]({}, mockDone);
            vi.advanceTimersByTime(1000);
            lateDone();

            expect(mockDone).toHaveBeenCalledTimes(1);
            expect(mockDone.mock.calls[0][0]).toBeInstanceOf(Error);
        });

        // @spec BOLT-PROCESS-004
        it("should not fail a message that finishes in time", () => {
            vi.useFakeTimers();
            const ctx = timedContext(1000);
            bolt.setup({ name: "quickBolt" }, ctx as any);
            vi.spyOn(bolt, "process").mockImplementation(async (_m, done) => {
                done();
            });
            bolt._init();

            const mockDone = vi.fn();
            ctx._mockProcess.mock.calls[0][2]({}, mockDone);
            vi.advanceTimersByTime(5000);

            expect(mockDone).toHaveBeenCalledTimes(1);
            expect(mockDone).toHaveBeenCalledWith(undefined);
        });

        // @spec BOLT-PROCESS-004
        it("should wait indefinitely when no timeout is set", () => {
            vi.useFakeTimers();
            const ctx = timedContext(0);
            bolt.setup({ name: "patientBolt" }, ctx as any);
            vi.spyOn(bolt, "process").mockImplementation(
                () => new Promise<void>(() => {}),
            );
            bolt._init();

            const mockDone = vi.fn();
            ctx._mockProcess.mock.calls[0][2]({}, mockDone);
            vi.advanceTimersByTime(24 * 60 * 60 * 1000);

            expect(mockDone).not.toHaveBeenCalled();
        });
    });
});
