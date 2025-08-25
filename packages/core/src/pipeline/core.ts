import {ForkException} from './errors'
import type {Data, DataBatch, DataFork, DataCursorUtils} from './data'
import type {Maybe} from '../internal/types'
import {unexpectedCase} from '../internal/misc'

export interface DataBatchMessage<TData extends Data, TUnfinalized extends boolean>
    extends DataBatch<TData, TUnfinalized> {
    type: 'batch'
}

export interface DataForkMessage<TData extends Data> extends DataFork<TData['cursor']> {
    type: 'fork'
}

export type DataMessage<TData extends Data, TUnfinalized extends boolean> = TUnfinalized extends false
    ? DataBatchMessage<TData, TUnfinalized>
    : DataBatchMessage<TData, TUnfinalized> | DataForkMessage<TData>

export interface DataReadOptions<TData extends Data, TRequest> {
    cursor: Maybe<TData['cursor']>
    request?: TRequest
}

export interface DataReader<TData extends Data, TUnfinalized extends boolean>
    extends AsyncIterator<DataBatch<TData, TUnfinalized>> {}

export interface DataFactoryOptions<TUnfinalized extends boolean> {
    unfinalized: TUnfinalized
}

export interface DataSource<T extends Data, TUnfinalized extends boolean, TRequest> {
    unfinalized: TUnfinalized
    cursorUtils: DataCursorUtils<T['cursor']>
    read: (opts: DataReadOptions<T, TRequest>) => AsyncIterable<DataMessage<T, TUnfinalized>>
}

export type DataSourceFactory<TData extends Data, TUnfinalized extends boolean, TRequest> = () => DataSource<
    TData,
    TUnfinalized,
    TRequest
>

export function createSource<TData extends Data, TUnfinalized extends boolean, TRequest>(
    sourceOrFactory: DataSource<TData, TUnfinalized, TRequest> | DataSourceFactory<TData, TUnfinalized, TRequest>,
): DataSourceFactory<TData, TUnfinalized, TRequest> {
    return () => {
        if (typeof sourceOrFactory === 'function') {
            sourceOrFactory = sourceOrFactory()
        }

        return sourceOrFactory
    }
}

export interface DataWriteOptions<TData extends Data, TUnfinalized extends boolean, TRequest> {
    cursorUtils: DataCursorUtils<TData['cursor']>
    read: (opts: DataReadOptions<TData, TRequest>) => AsyncIterableIterator<DataMessage<TData, TUnfinalized>>
}

export interface DataTarget<TData extends Data, TUnfinalized extends boolean, TRequest, TResult> {
    unfinalized: TUnfinalized
    write: (opts: DataWriteOptions<TData, TUnfinalized, TRequest>) => TResult
}

export type DataTargetFactory<TData extends Data, TUnfinalized extends boolean, TRequest, TResult> = (
    opts: DataFactoryOptions<TUnfinalized>,
) => DataTarget<TData, TUnfinalized, TRequest, TResult>

export function createTarget<TData extends Data, TUnfinalized extends boolean, TRequest, TResult>(
    targetOrFactory:
        | DataTarget<TData, TUnfinalized, TRequest, TResult>
        | DataTargetFactory<TData, TUnfinalized, TRequest, TResult>,
): DataTargetFactory<TData, TUnfinalized, TRequest, TResult> {
    return (opts) => {
        if (typeof targetOrFactory === 'function') {
            return targetOrFactory(opts)
        }

        return targetOrFactory
    }
}

export type DataDuplex<
    TInputData extends Data,
    TOutputData extends Data,
    TInputUnfinalized extends boolean,
    TOutputUnfinalized extends boolean,
    TInputRequest,
    TOutputRequest,
> = DataTarget<
    TInputData,
    TInputUnfinalized,
    TInputRequest,
    DataStream<TOutputData, TOutputUnfinalized, TOutputRequest>
>

export type DataDuplexFactory<
    TInputData extends Data,
    TOutputData extends Data,
    TInputUnfinalized extends boolean,
    TOutputUnfinalized extends boolean,
    TInputRequest,
    TOutputRequest,
> = (
    opts: DataFactoryOptions<TInputUnfinalized>,
) => DataDuplex<TInputData, TOutputData, TInputUnfinalized, TOutputUnfinalized, TInputRequest, TOutputRequest>

export interface DataPipeOptions<TData extends Data, TRequest> {
    validateBatches?: boolean
    stopOnHead?: boolean
    cursor?: TData['cursor']
    request?: TRequest
}

export interface DataStream<TData extends Data, TUnfinalized extends boolean, TRequest = never> {
    pipe<TResult>(
        targetFactory: (
            opts: DataFactoryOptions<TUnfinalized>,
        ) => DataTarget<TData, TUnfinalized extends true ? true : boolean, TRequest, TResult>,
        opts?: DataPipeOptions<TData, TRequest>,
    ): TResult
    [Symbol.asyncIterator](): AsyncIterableIterator<TData['value']>
}

export function stream<TData extends Data, TUnfinalized extends boolean, TRequest = never>(
    sourceFactory: () => DataSource<TData, TUnfinalized, TRequest>,
): DataStream<TData, TUnfinalized, TRequest> {
    return {
        pipe: (targetFactory, opts = {}) => {
            const source = sourceFactory()
            const target = targetFactory({unfinalized: source.unfinalized})

            return pipe(source, target, opts)
        },
        [Symbol.asyncIterator]: (opts?: DataReadOptions<TData, TRequest>) => {
            const source = sourceFactory()
            return pipe(
                source,
                {
                    unfinalized: source.unfinalized,
                    write: async function* (streamOpts) {
                        for await (const message of streamOpts.read(opts ?? {cursor: undefined, request: undefined})) {
                            switch (message.type) {
                                case 'batch':
                                    yield* message.data.map((item) => item.value)
                                    break
                                case 'fork':
                                    throw new ForkException(message)
                                default:
                                    throw unexpectedCase((message as any).type)
                            }
                        }
                    },
                },
                {
                    cursor: opts?.cursor,
                    request: opts?.request,
                },
            )
        },
    }
}

function pipe<TData extends Data, TRequest = never, TResult = unknown>(
    source: DataSource<TData, boolean, TRequest>,
    target: DataTarget<TData, boolean, TRequest, TResult>,
    opts: DataPipeOptions<TData, TRequest>,
): TResult {
    if (source.unfinalized && !target.unfinalized) {
        throw new TypeError('Cannot pipe from unfinalized DataSource to finalized DataTarget')
    }

    return target.write({
        cursorUtils: source.cursorUtils,
        read: async function* (streamOpts) {
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
                                throw new Error('New offset is below the previous offset')
                            }

                            if (!source.cursorUtils.compare(batch.head, batch.cursor).isGreaterOrEqual) {
                                throw new Error('Head is below the offset')
                            }

                            if (!source.unfinalized) {
                                if (!source.cursorUtils.compare(batch.head, batch.finalizedHead).isEqual) {
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

                        // FIXME: should we always force exit on fork?
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
