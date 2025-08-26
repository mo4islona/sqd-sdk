import {unexpectedCase} from '../internal/misc'
import type {DataCursorUtils} from './cursor'
import {ForkException} from './errors'

export interface DataBatchItem<TCursor, TValue> {
    cursor: TCursor
    value: TValue
}

export interface DataBatchMessage<TCursor, TValue> {
    type: 'batch'
    cursor: TCursor
    head: TCursor
    finalizedHead?: TCursor
    data: DataBatchItem<TCursor, TValue>[]
}

export interface DataForkMessage<TCursor> {
    type: 'fork'
    cursors: TCursor[]
}

export type DataMessage<TCursor, TValue> = DataBatchMessage<TCursor, TValue> | DataForkMessage<TCursor>

export interface ReadOptions<TCursor, TRequest> {
    cursor?: TCursor
    request?: TRequest
}

export interface DataSource<TCursor, TValue, TRequest> {
    unfinalized: boolean
    cursorUtils: DataCursorUtils<TCursor>
    read(options: ReadOptions<TCursor, TRequest>): AsyncIterable<DataMessage<TCursor, TValue>>
}

export interface WriteContext<TCursor, TValue, TRequest> {
    cursorUtils: DataCursorUtils<TCursor>
    read(options: ReadOptions<TCursor, TRequest>): AsyncIterable<DataMessage<TCursor, TValue>>
}

export interface DataTarget<TCursor, TValue, TRequest, TReturn> {
    unfinalized: boolean
    write(context: WriteContext<TCursor, TValue, TRequest>): TReturn
}

export interface DataTargetFactoryOptions {
    unfinalized: boolean
}

export interface PipeOptions {
    validateBatches?: boolean
}

export function createSource<TCursor, TValue, TRequest>(
    source: DataSource<TCursor, TValue, TRequest>,
): DataSource<TCursor, TValue, TRequest> {
    return source
}

export function createTarget<TCursor, TValue, TRequest, TReturn>(
    targetOrFactory:
        | DataTarget<TCursor, TValue, TRequest, TReturn>
        | ((opts: DataTargetFactoryOptions) => DataTarget<TCursor, TValue, TRequest, TReturn>),
): (opts: DataTargetFactoryOptions) => DataTarget<TCursor, TValue, TRequest, TReturn> {
    return (opts: DataTargetFactoryOptions) => {
        if (typeof targetOrFactory === 'function') {
            return (
                targetOrFactory as (opts: DataTargetFactoryOptions) => DataTarget<TCursor, TValue, TRequest, TReturn>
            )(opts)
        }

        return targetOrFactory as DataTarget<TCursor, TValue, TRequest, TReturn>
    }
}

export interface Stream<TCursor, TValue, TRequest> {
    pipe<TReturn>(
        targetFactory: (opts: DataTargetFactoryOptions) => DataTarget<TCursor, TValue, TRequest, TReturn>,
        opts?: PipeOptions,
    ): TReturn

    [Symbol.asyncIterator](opts?: ReadOptions<TCursor, TRequest>): AsyncIterable<TValue>
}

export function stream<TCursor, TValue, TRequest>(
    sourceFactory: () => DataSource<TCursor, TValue, TRequest>,
): Stream<TCursor, TValue, TRequest> {
    return {
        pipe: (targetFactory, opts: PipeOptions = {}) => {
            const source = sourceFactory()
            const target = targetFactory({unfinalized: source.unfinalized})

            return pipe(source, target, opts)
        },
        [Symbol.asyncIterator]: (opts?: ReadOptions<TCursor, TRequest>): AsyncIterable<TValue> => {
            const source = sourceFactory()
            return pipe(
                source,
                {
                    unfinalized: source.unfinalized,
                    write: async function* (streamOpts: WriteContext<TCursor, TValue, TRequest>) {
                        for await (const message of streamOpts.read(
                            (opts ?? {cursor: undefined, request: undefined}) as ReadOptions<TCursor, TRequest>,
                        )) {
                            switch (message.type) {
                                case 'batch':
                                    yield* message.data.map((item) => item.value)
                                    break
                                case 'fork':
                                    throw new ForkException(message.cursors)
                                default:
                                    throw unexpectedCase((message as any).type)
                            }
                        }
                    },
                },
                {},
            )
        },
    }
}

function pipe<TCursor, TValue, TRequest, TReturn>(
    source: DataSource<TCursor, TValue, TRequest>,
    target: DataTarget<TCursor, TValue, TRequest, TReturn>,
    opts: PipeOptions | unknown,
): TReturn {
    if (source.unfinalized && !target.unfinalized) {
        throw new TypeError('Cannot pipe from unfinalized DataSource to finalized DataTarget')
    }

    return target.write({
        cursorUtils: source.cursorUtils,
        read: async function* (
            streamOpts: ReadOptions<TCursor, TRequest>,
        ): AsyncIterable<DataMessage<TCursor, TValue>> {
            let offset = streamOpts.cursor

            for await (const message of source.read(streamOpts)) {
                switch (message.type) {
                    case 'batch': {
                        const batch = message

                        if (!source.unfinalized && !batch.finalizedHead) {
                            throw new TypeError('Finalized source data must have a finalized head')
                        }

                        if ((opts as PipeOptions).validateBatches) {
                            if (offset && !source.cursorUtils.compare(batch.cursor, offset).isGreaterOrEqual) {
                                throw new Error('New offset is below the previous offset')
                            }

                            if (!source.cursorUtils.compare(batch.head, batch.cursor).isGreaterOrEqual) {
                                throw new Error('Head is below the offset')
                            }

                            if (!source.unfinalized) {
                                if (!source.cursorUtils.compare(batch.head, batch.finalizedHead as TCursor).isEqual) {
                                    throw new Error('Head is not equal to the finalized head')
                                }
                            } else if (batch.finalizedHead) {
                                if (!source.cursorUtils.compare(batch.head, batch.finalizedHead).isGreaterOrEqual) {
                                    throw new Error('Head is below the finalized head')
                                }
                            }

                            let lastId = offset
                            for (const item of batch.data) {
                                if (lastId && !source.cursorUtils.compare(item.cursor, lastId).isGreater) {
                                    throw new Error('Item is below or equal to the previous item')
                                }
                                lastId = item.cursor
                            }

                            if (lastId && !source.cursorUtils.compare(batch.cursor, lastId).isGreaterOrEqual) {
                                throw new Error('Offset is below the data')
                            }

                            if (lastId && !source.cursorUtils.compare(batch.head, lastId).isGreaterOrEqual) {
                                throw new Error('Head is below the data')
                            }
                        }

                        offset = batch.cursor

                        break
                    }
                    case 'fork': {
                        if (!source.unfinalized) {
                            throw new TypeError('Got fork message from finalized DataSource')
                        }
                        if (!target.unfinalized) {
                            throw new TypeError('Got fork message for finalized DataTarget')
                        }

                        return
                    }
                    default: {
                        throw unexpectedCase((message as any).type)
                    }
                }

                yield message
            }
        },
    })
}
