import {
    createSource,
    createStream,
    createTarget,
    type DataBatchMessage,
    type DataForkMessage,
    type DataMessage,
    type DataStream,
} from '../core'

export interface DataTrackerConfig<TCursor, TValue> {
    beforeRead?: (cursor?: TCursor) => void | Promise<void>
    afterRead?: (message: DataBatchMessage<TCursor, TValue>) => void | Promise<void>
    beforeWrite?: (message: DataBatchMessage<TCursor, TValue>) => void | Promise<void>
    afterWrite?: (message: DataBatchMessage<TCursor, TValue>) => void | Promise<void>
    beforeFork?: (message: DataForkMessage<TCursor>) => void | Promise<void>
    afterFork?: (message: DataForkMessage<TCursor>) => void | Promise<void>
}

export function createTracker<TCursor, TValue, TRequest>(config: DataTrackerConfig<TCursor, TValue>) {
    return createTarget<TCursor, TValue, TRequest, DataStream<TCursor, TValue, TRequest>>((opts) => ({
        unfinalized: opts.unfinalized,
        write: (writeOpts) => {
            return createStream<TCursor, TValue, TRequest>(
                createSource({
                    unfinalized: opts.unfinalized,
                    cursorUtils: writeOpts.cursorUtils,
                    read: async function* (readOpts) {
                        await config.beforeRead?.(readOpts.cursor)
                        for await (const message of writeOpts.read(readOpts)) {
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
                }),
            )
        },
    }))
}
