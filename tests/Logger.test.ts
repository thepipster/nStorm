import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import Logger from "../src/Logger";

const winstonLogger = (): any => (Logger as any)._logger;

describe("Logger", () => {
    // tests/setup.ts silences the logger for the whole suite; these tests
    // need it live (winston's own output is mocked in each of them)
    beforeEach(() => {
        Logger.silent = false;
    });

    afterEach(() => {
        Logger.silent = true;
        vi.restoreAllMocks();
        Logger.setLevel("info");
        Logger.stackTraces = true;
    });

    // @spec LOG-LEVEL-001
    it("should default to the info level", () => {
        expect(winstonLogger().level).toBe("info");
    });

    // @spec LOG-LEVEL-002
    it("should start at the level named by NSTORM_LOG_LEVEL", async () => {
        vi.stubEnv("NSTORM_LOG_LEVEL", "warn");
        vi.resetModules();

        const fresh = (await import("../src/Logger")).default;

        expect((fresh as any)._logger.level).toBe("warn");
        vi.unstubAllEnvs();
    });

    // @spec LOG-ALIAS-001
    it("log() should write at a level winston knows about", () => {
        const spy = vi
            .spyOn(winstonLogger(), "log")
            .mockImplementation(() => winstonLogger());

        Logger.log("hello");

        expect(spy).toHaveBeenCalledWith(
            "info",
            expect.stringContaining("hello"),
        );
    });

    // @spec LOG-ALIAS-002
    it("fatal() should write at the error level", () => {
        const spy = vi
            .spyOn(winstonLogger(), "log")
            .mockImplementation(() => winstonLogger());

        Logger.fatal("bad");

        expect(spy).toHaveBeenCalledWith(
            "error",
            expect.stringContaining("bad"),
        );
    });

    // @spec LOG-LEVEL-004
    it("should do no work for a message below the current level", () => {
        const logSpy = vi
            .spyOn(winstonLogger(), "log")
            .mockImplementation(() => winstonLogger());
        const inspected = { toJSON: vi.fn(), [Symbol.for("x")]: 1 };
        const serialize = vi.spyOn(Logger as any, "__serialize");

        Logger.debug("hidden", inspected);

        expect(serialize).not.toHaveBeenCalled();
        expect(logSpy).not.toHaveBeenCalled();
    });

    // @spec LOG-LEVEL-003
    it("setLevel should enable debug output", () => {
        const spy = vi
            .spyOn(winstonLogger(), "log")
            .mockImplementation(() => winstonLogger());

        Logger.setLevel("debug");
        Logger.debug("now visible");

        expect(spy).toHaveBeenCalledWith(
            "debug",
            expect.stringContaining("now visible"),
        );
    });

    // @spec LOG-TRACE-001, LOG-TRACE-002
    it("should append the call site, and omit it when stackTraces is off", () => {
        const spy = vi
            .spyOn(winstonLogger(), "log")
            .mockImplementation(() => winstonLogger());

        Logger.info("with trace");
        Logger.stackTraces = false;
        Logger.info("without trace");

        expect(spy.mock.calls[0][1]).toContain("Logger.test.ts");
        expect(spy.mock.calls[1][1]).not.toContain("{from line");
    });

    // @spec LOG-FORMAT-001
    it("should serialise objects and errors", () => {
        const spy = vi
            .spyOn(winstonLogger(), "log")
            .mockImplementation(() => winstonLogger());

        Logger.error("failed;", new Error("boom"), { id: 7 });

        const message = spy.mock.calls[0][1] as string;
        expect(message).toContain("boom");
        expect(message).toContain("id");
    });

    // @spec LOG-SILENT-001
    it("should write nothing while silent", () => {
        const spy = vi
            .spyOn(winstonLogger(), "log")
            .mockImplementation(() => winstonLogger());

        Logger.silent = true;
        Logger.error("hidden");
        Logger.silent = false;
        Logger.error("shown");

        expect(spy).toHaveBeenCalledTimes(1);
        expect(spy.mock.calls[0][1]).toContain("shown");
    });
});
