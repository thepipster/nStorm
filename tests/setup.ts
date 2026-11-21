import { beforeEach } from "vitest";
import Logger from "../src/Logger";

// Many tests exercise failure paths on purpose, and nStorm logs every one of
// them. Left alone that fills a passing run with red error output. Silence
// the logger unless asked not to: NSTORM_TEST_LOGS=1 npm test
const silent = !process.env.NSTORM_TEST_LOGS;

Logger.silent = silent;

beforeEach(() => {
    Logger.silent = silent;
});
