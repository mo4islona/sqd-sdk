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

export interface DataReadOptions<TCursor, TRequest> {
    cursor?: TCursor
    request?: TRequest
}

export interface DataSource<TCursor, TValue, TRequest> {
    unfinalized: boolean
    cursorUtils: DataCursorUtils<TCursor>
    read(options: DataReadOptions<TCursor, TRequest>): AsyncIterable<DataMessage<TCursor, TValue>>
}

export interface DataWriteOptions<TCursor, TValue, TRequest> {
    cursorUtils: DataCursorUtils<TCursor>
    read(options: DataReadOptions<TCursor, TRequest>): AsyncIterable<DataMessage<TCursor, TValue>>
}

export interface DataTarget<TCursor, TValue, TRequest, TReturn> {
    unfinalized: boolean
    write(context: DataWriteOptions<TCursor, TValue, TRequest>): TReturn
}

export interface DataTargetFactoryOptions {
    unfinalized: boolean
}

export interface DataPipeOptions {
    validateBatches?: boolean
}

export interface DataSourceConfig<TCursor, TValue, TRequest> {
    unfinalized?: boolean
    cursorUtils: DataCursorUtils<TCursor>
    read(options: DataReadOptions<TCursor, TRequest>): AsyncIterable<DataMessage<TCursor, TValue>>
}

export function createSource<TCursor, TValue, TRequest>(
    source: DataSourceConfig<TCursor, TValue, TRequest>,
): DataSource<TCursor, TValue, TRequest> {
    return {
        unfinalized: source.unfinalized ?? true,
        cursorUtils: source.cursorUtils,
        read: (opts) => source.read(opts),
    }
}

export interface DataTargetConfig<TCursor, TValue, TRequest, TReturn> {
    unfinalized?: boolean
    write(context: DataWriteOptions<TCursor, TValue, TRequest>): TReturn
}

export function createTarget<TCursor, TValue, TRequest, TReturn>(
    config: DataTargetConfig<TCursor, TValue, TRequest, TReturn>,
): DataTarget<TCursor, TValue, TRequest, TReturn>
export function createTarget<TCursor, TValue, TRequest, TReturn>(
    factory: (opts: DataTargetFactoryOptions) => DataTargetConfig<TCursor, TValue, TRequest, TReturn>,
): (opts: DataTargetFactoryOptions) => DataTarget<TCursor, TValue, TRequest, TReturn>
export function createTarget<TCursor, TValue, TRequest, TReturn>(
    configOrFactory:
        | DataTargetConfig<TCursor, TValue, TRequest, TReturn>
        | ((opts: DataTargetFactoryOptions) => DataTargetConfig<TCursor, TValue, TRequest, TReturn>),
):
    | DataTarget<TCursor, TValue, TRequest, TReturn>
    | ((opts: DataTargetFactoryOptions) => DataTarget<TCursor, TValue, TRequest, TReturn>) {
    if (typeof configOrFactory === 'function') {
        return (opts: DataTargetFactoryOptions) => createTarget(configOrFactory(opts))
    }

    return {
        unfinalized: configOrFactory.unfinalized ?? true,
        write: (opts) => configOrFactory.write(opts),
    }
}

export type DataDuplex<TInputCursor, TInputValue, TInputRequest, TOutputCursor, TOutputValue, TOutputRequest> =
    DataTarget<TInputCursor, TInputValue, TInputRequest, DataStream<TOutputCursor, TOutputValue, TOutputRequest>>

export interface DataStream<TCursor, TValue, TRequest> {
    pipe<TReturn>(
        targetOrFactory:
            | DataTarget<TCursor, TValue, TRequest, TReturn>
            | ((opts: DataTargetFactoryOptions) => DataTarget<TCursor, TValue, TRequest, TReturn>),
        opts?: DataPipeOptions,
    ): TReturn

    [Symbol.asyncIterator](opts?: DataReadOptions<TCursor, TRequest>): AsyncIterable<TValue>
}

export function createStream<TCursor, TValue, TRequest>(
    sourceOrFactory: DataSource<TCursor, TValue, TRequest> | (() => DataSource<TCursor, TValue, TRequest>),
): DataStream<TCursor, TValue, TRequest> {
    const source = typeof sourceOrFactory === 'function' ? sourceOrFactory() : sourceOrFactory

    return {
        pipe: (targetOrFactory, opts: DataPipeOptions = {}) => {
            const target =
                typeof targetOrFactory === 'function'
                    ? targetOrFactory({unfinalized: source.unfinalized})
                    : targetOrFactory

            return pipe(source, target, opts)
        },
        [Symbol.asyncIterator]: (opts?: DataReadOptions<TCursor, TRequest>): AsyncIterable<TValue> => {
            return pipe(
                source,
                {
                    unfinalized: source.unfinalized,
                    write: async function* (streamOpts: DataWriteOptions<TCursor, TValue, TRequest>) {
                        for await (const message of streamOpts.read(
                            (opts ?? {cursor: undefined, request: undefined}) as DataReadOptions<TCursor, TRequest>,
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
    opts: DataPipeOptions,
): TReturn {
    if (source.unfinalized && !target.unfinalized) {
        throw new TypeError('Cannot pipe from unfinalized DataSource to finalized DataTarget')
    }

    return target.write({
        cursorUtils: source.cursorUtils,
        read: async function* (
            streamOpts: DataReadOptions<TCursor, TRequest>,
        ): AsyncIterable<DataMessage<TCursor, TValue>> {
            let offset = streamOpts.cursor

            for await (const message of source.read(streamOpts)) {
                switch (message.type) {
                    case 'batch': {
                        const batch = message

                        if (!source.unfinalized && !batch.finalizedHead) {
                            throw new TypeError('Finalized source data must have a finalized head')
                        }

                        if (opts.validateBatches) {
                            if (offset && !source.cursorUtils.compare(batch.cursor, offset).isGreaterOrEqual) {
                                throw new RangeError('New offset is below the previous offset')
                            }

                            if (!source.cursorUtils.compare(batch.head, batch.cursor).isGreaterOrEqual) {
                                throw new RangeError('Head is below the offset')
                            }

                            if (!source.unfinalized) {
                                if (!source.cursorUtils.compare(batch.head, batch.finalizedHead as TCursor).isEqual) {
                                    throw new RangeError('Head is not equal to the finalized head')
                                }
                            } else if (batch.finalizedHead) {
                                if (!source.cursorUtils.compare(batch.head, batch.finalizedHead).isGreaterOrEqual) {
                                    throw new RangeError('Head is below the finalized head')
                                }
                            }

                            let lastId = offset
                            for (const item of batch.data) {
                                if (lastId && !source.cursorUtils.compare(item.cursor, lastId).isGreater) {
                                    throw new RangeError('Item is below or equal to the previous item')
                                }
                                lastId = item.cursor
                            }

                            if (lastId && !source.cursorUtils.compare(batch.cursor, lastId).isGreaterOrEqual) {
                                throw new RangeError('Offset is below the data')
                            }

                            if (lastId && !source.cursorUtils.compare(batch.head, lastId).isGreaterOrEqual) {
                                throw new RangeError('Head is below the data')
                            }
                        }

                        offset = batch.cursor

                        break
                    }
                    case 'fork': {
                        if (!source.unfinalized) {
                            throw new RangeError('Got fork message from finalized DataSource')
                        }
                        if (!target.unfinalized) {
                            throw new RangeError('Got fork message for finalized DataTarget')
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
