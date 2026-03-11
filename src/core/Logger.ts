import { LogLevel } from '../types'

const Colors = {
  reset: '\x1b[0m',
  debug: '\x1b[36m', // Cyan
  info: '\x1b[32m',  // Green
  warn: '\x1b[33m',  // Yellow
  error: '\x1b[31m', // Red
  white: '\x1b[37m', // White
  grey: '\x1b[90m',  // Grey
}

export class LoggerManager {
  private level: LogLevel

  constructor(level: LogLevel = LogLevel.Info) {
    this.level = level
  }

  setLevel(level: LogLevel): void {
    this.level = level
  }

  shouldLog(level: LogLevel): boolean {
    if (this.level === LogLevel.None) return false
    return level >= this.level
  }

  create(moduleName: string): Logger {
    return new Logger(this, moduleName)
  }
}

export class Logger {
  constructor(private readonly parent: LoggerManager, private readonly moduleName: string) { }

  debug(message: string, ...args: any[]): void {
    if (this.parent.shouldLog(LogLevel.Debug)) {
      console.debug(`${Colors.debug}[DEBUG] [${this.moduleName}]${Colors.reset} ${Colors.white}${message}${Colors.reset}`, ...args)
    }
  }

  info(message: string, ...args: any[]): void {
    if (this.parent.shouldLog(LogLevel.Info)) {
      console.info(`${Colors.info}[INFO] [${this.moduleName}]${Colors.reset} ${Colors.white}${message}${Colors.reset}`, ...args)
    }
  }

  warn(message: string, ...args: any[]): void {
    if (this.parent.shouldLog(LogLevel.Warning)) {
      console.warn(`${Colors.warn}[WARN] [${this.moduleName}]${Colors.reset} ${Colors.white}${message}${Colors.reset}`, ...args)
    }
  }

  error(message: string, ...args: any[]): void {
    if (this.parent.shouldLog(LogLevel.Error)) {
      console.error(`${Colors.error}[ERROR] [${this.moduleName}]${Colors.reset} ${Colors.white}${message}${Colors.reset}`, ...args)
    }
  }
}
