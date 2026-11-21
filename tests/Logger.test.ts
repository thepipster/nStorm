import { describe, it, expect, vi, afterEach } from "vitest";
import Logger from "../src/Logger";

const winstonLogger = (): any => (Logger as any)._logger;

describe("Logger", () => {
    afterEach(() => {
        vi.restoreAllMocks();
        Logger.setLevel("info");
        Logger.stackTraces = true;
    });

    it("should default to the info level", () => {
        expect(winstonLogger().level).toBe("info");
    });

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

    it("should serialise objects and errors", () => {
        const spy = vi
            .spyOn(winstonLogger(), "log")
            .mockImplementation(() => winstonLogger());

        Logger.error("failed;", new Error("boom"), { id: 7 });

        const message = spy.mock.calls[0][1] as string;
        expect(message).toContain("boom");
        expect(message).toContain("id");
    });
});
