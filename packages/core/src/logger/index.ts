import {SqdLogger, type Logger} from './logger'
import {jsonLinesStderrSink} from './sinks/json'
import {prettyStderrSink} from './sinks/pretty'

export {LogLevel} from './level'
export * from './logger'

// @ts-ignore
const prettyEnabled = process.env.FORCE_PRETTY_LOGGER
    ? // @ts-ignore
      process.env.FORCE_PRETTY_LOGGER !== '0'
    : // @ts-ignore
      process.stderr.isTTY

const ROOT = new SqdLogger(prettyEnabled ? prettyStderrSink : jsonLinesStderrSink, '')

export function createLogger(ns: string, attributes?: object): SqdLogger {
    return ROOT.child(ns, attributes)
}
