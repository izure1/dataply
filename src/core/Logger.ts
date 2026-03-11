import { LogLevel } from '../types'

export class Logger {
  private level: LogLevel

  constructor(level: LogLevel = LogLevel.Info) {
    this.level = level
  }

  setLevel(level: LogLevel): void {
    this.level = level
  }

  private shouldLog(level: LogLevel): boolean {
    if (this.level === LogLevel.None) return false
    return level >= this.level
  }

  debug(message: string, ...args: any[]): void {
    if (this.shouldLog(LogLevel.Debug)) {
      console.debug(`[DEBUG] ${message}`, ...args)
    }
  }

  info(message: string, ...args: any[]): void {
    if (this.shouldLog(LogLevel.Info)) {
      console.info(`[INFO] ${message}`, ...args)
    }
  }

  warn(message: string, ...args: any[]): void {
    if (this.shouldLog(LogLevel.Warning)) {
      console.warn(`[WARN] ${message}`, ...args)
    }
  }

  error(message: string, ...args: any[]): void {
    if (this.shouldLog(LogLevel.Error)) {
      console.error(`[ERROR] ${message}`, ...args)
    }
  }
}
