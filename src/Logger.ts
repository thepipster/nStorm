import winston from 'winston';
import chalk from 'chalk';
import path from 'path';
import util from 'util';
import callsite from 'callsite';

const format = winston.format;

interface LoggerColors {
  log: chalk.Chalk;
  trace: chalk.Chalk;
  debug: chalk.Chalk;
  input: chalk.Chalk;
  info: chalk.Chalk;
  warn: chalk.Chalk;
  error: chalk.Chalk;
  fatal: chalk.Chalk;
  exception: chalk.Chalk;
  time: chalk.Chalk;
}

class Logger {
  static colors: LoggerColors = {
    log: chalk.white,
    trace: chalk.gray,
    debug: chalk.green,
    input: chalk.blue,
    info: chalk.blue,
    warn: chalk.yellow,
    error: chalk.red,
    fatal: chalk.bold.red,
    exception: chalk.bold.white.bgRed,
    time: chalk.gray,
  };

  static colorize = true;
  static serviceName = '';
  private static _logger: winston.Logger;

  static init(): void {
    const myFormat = format.printf((info) => {
      return (
        Logger.colors.time('[' + info.timestamp + ']') +
        ` ${info.level}: ${info.message}`
      );
    });

    // Get the service name
    try {
      Logger.serviceName = path.basename(
        process.argv[1],
        path.extname(process.argv[1])
      );
    } catch (_e) {
      // ignore
    }

    const transports = [
      new winston.transports.Console({
        level: 'debug',
        format: format.combine(
          format.timestamp({
            format: 'ddd MMM DD h:mm:ss',
          }),
          format.colorize(),
          myFormat
        ),
      }),
    ];

    Logger._logger = winston.createLogger({
      transports,
    });
  }

  private static __getStackTrace(stackObj: callsite.CallSite[]): string {
    let trace_str = '';
    let depth = 1;
    let startDepth = 9999;
    const maxDepth = 6;

    if (stackObj) {
      stackObj.forEach((site) => {
        if (site) {
          const no = site.getLineNumber();
          let fname = site.getFileName();
          fname = fname ? path.basename(fname) : fname;
          let funcname: string = site.getFunctionName() || 'anonymous';

          if (funcname) {
            funcname = ' (' + funcname + ')' || '';
          } else {
            funcname = '';
          }

          if (fname === 'Logger.ts' || fname === 'Logger.js') {
            startDepth = depth + 1;
          }

          if (depth === startDepth) {
            trace_str += ' {from line ' + no + ' of ' + fname + funcname;
          } else if (
            no &&
            depth > startDepth &&
            depth <= startDepth + maxDepth
          ) {
            trace_str += ', line ' + no + ' of ' + fname + funcname;
          }
        }

        depth += 1;
      });
      trace_str += '}';
    }

    if (Logger.colorize) {
      return Logger.colors.trace(trace_str);
    }

    return trace_str;
  }

  private static __serialize(args: IArguments | unknown[]): string {
    let msg = '';

    for (let i = 0; i < args.length; i++) {
      if (typeof args[i] === 'object') {
        msg +=
          '\n' +
          util.inspect(args[i], {
            colors: Logger.colorize,
            compact: false,
            breakLength: 60,
            depth: null,
          });
      } else {
        msg += args[i];
      }

      msg += ' ';
    }

    return msg;
  }

  static setLevel(lvl = 'debug'): void {
    Logger._logger.level = lvl;
  }

  static log(..._args: unknown[]): void {
    const str =
      Logger.__serialize(arguments) + Logger.__getStackTrace(callsite());
    Logger._logger.log('log' as string, str);
  }

  static debug(..._args: unknown[]): void {
    const str =
      Logger.__serialize(arguments) + Logger.__getStackTrace(callsite());
    Logger._logger.log('debug', str);
  }

  static info(..._args: unknown[]): void {
    const str =
      Logger.__serialize(arguments) + Logger.__getStackTrace(callsite());
    Logger._logger.log('info', str);
  }

  static warn(..._args: unknown[]): void {
    const str =
      Logger.__serialize(arguments) + Logger.__getStackTrace(callsite());
    Logger._logger.log('warn', str);
  }

  static error(..._args: unknown[]): void {
    const str =
      Logger.__serialize(arguments) + Logger.__getStackTrace(callsite());
    Logger._logger.log('error', str);
  }

  static fatal(..._args: unknown[]): void {
    const str =
      Logger.__serialize(arguments) + Logger.__getStackTrace(callsite());
    Logger._logger.log('error', str);
  }
}

Logger.init();

export default Logger;
