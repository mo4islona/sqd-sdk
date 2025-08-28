import {
    createSource,
    createStream,
    type DataBatchMessage,
    type DataForkMessage,
    type DataMessage,
    type DataStream,
} from '../core'
import {createTransformer} from './transformer'

export interface DataTrackerConfig<TCursor, TValue> {
    beforeRead?: (cursor?: TCursor) => void | Promise<void>
    afterRead?: (message: DataBatchMessage<TCursor, TValue>) => void | Promise<void>
    beforeWrite?: (message: DataBatchMessage<TCursor, TValue>) => void | Promise<void>
    afterWrite?: (message: DataBatchMessage<TCursor, TValue>) => void | Promise<void>
    beforeFork?: (message: DataForkMessage<TCursor>) => void | Promise<void>
    afterFork?: (message: DataForkMessage<TCursor>) => void | Promise<void>
}

export function createTracker<TCursor, TValue, TRequest>(config: DataTrackerConfig<TCursor, TValue>) {
    return createTransformer<TCursor, TCursor, TValue, TValue, TRequest, TRequest>((opts) => ({
        cursorUtils: opts.cursorUtils,
        read: async function* (readOpts) {
            await config.beforeRead?.(readOpts.cursor)
            for await (const message of opts.read(readOpts)) {
                switch (message.type) {
                    case 'batch':
                        await config.afterRead?.(message)

                        await config.beforeWrite?.(message)
                        yield message
                        await config.afterWrite?.(message)

                        await config.beforeRead?.(message.cursor)
                        break
                    case 'fork':
                        await config.beforeFork?.(message)
                        yield message
                        await config.afterFork?.(message)
                        break
                }
            }
        },
    }))
}
