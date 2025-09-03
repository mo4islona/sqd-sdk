import type { DataDataMessage, DataForkMessage, DataPassThrough, DataTargetFactoryOptions } from '../core'
import { createTransformer } from './transformer'

export interface DataTrackerConfig<TCursor, TValue> {
    beforeRead?: () => void | Promise<void>
    afterRead?: (message: DataDataMessage<TCursor, TValue>) => void | Promise<void>
    beforeWrite?: (message: DataDataMessage<TCursor, TValue>) => void | Promise<void>
    afterWrite?: (message: DataDataMessage<TCursor, TValue>) => void | Promise<void>
    beforeFork?: (message: DataForkMessage<TCursor>) => void | Promise<void>
    afterFork?: (message: DataForkMessage<TCursor>) => void | Promise<void>
    afterEnd?: () => void | Promise<void>
}

export function createTracker<TCursor, TValue, TQuery>(
    config: DataTrackerConfig<TCursor, TValue>,
): (opts: DataTargetFactoryOptions) => DataPassThrough<TCursor, TValue, TQuery> {
    return createTransformer<TValue, TValue, TQuery, TCursor>((opts) => ({
        cursorUtils: opts.cursorUtils,
        read: async function* (readOpts) {
            await config.beforeRead?.()
            for await (const message of opts.read(readOpts)) {
                switch (message.type) {
                    case 'data':
                        await config.afterRead?.(message)

                        await config.beforeWrite?.(message)
                        yield message
                        await config.afterWrite?.(message)

                        await config.beforeRead?.()
                        break
                    case 'fork':
                        await config.beforeFork?.(message)
                        yield message
                        await config.afterFork?.(message)
                        break
                }
            }

            await config.afterEnd?.()
        },
    }))
}
