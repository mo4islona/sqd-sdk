import {ForkException, isForkException} from './errors'
import type {Data, DataBatch, DataFork, DataRef} from './data'
import type {Awaitable, Maybe} from '../internal/types'
import {unexpectedCase} from '../internal/misc'

export interface DataBatchMessage<TData extends Data> {
    type: 'batch'
    value: DataBatch<TData>
}

export interface DataForkMessage<TData extends Data> {
    type: 'fork'
    value: DataFork<TData['id']>
}

export type DataMessage<TData extends Data, TUnfinalized extends boolean> = TUnfinalized extends false
    ? DataBatchMessage<TData>
    : DataBatchMessage<TData> | DataForkMessage<TData>

export interface DataReadOptions<TData extends Data, TRequest> {
    offset: Maybe<TData['id']>
    request?: TRequest
}

export interface DataReader<TData extends Data> extends AsyncIterator<DataBatch<TData>> {}

export interface DataFactoryOptions<TData extends Data, TUnfinalized extends boolean> {
    unfinalized: TUnfinalized
}

export interface DataSource<T extends Data, TUnfinalized extends boolean, TRequest> {
    unfinalized: TUnfinalized
    ref: DataRef<T['id']>
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
            return sourceOrFactory()
        }

        return {
            ...sourceOrFactory,
            // [Symbol.asyncIterator]: (opts: DataReaderOptions<TData>) => {
            //     let reader: DataReader<TData> | undefined
            //     return {
            //         next: async () => {
            //             if (!reader) {
            //                 reader = await source.reader(opts)
            //             }
            //             return reader.next()
            //         },
            //         return: async () => {
            //             const result = await reader?.return?.()
            //             return result ? result : {done: true, value: undefined}
            //         },
            //         throw: async (err: any) => {
            //             await reader?.return?.().catch(() => {})
            //             throw err
            //         },
            //         [Symbol.asyncIterator]() {
            //             return this
            //         },
            //     }
            // },
        }
    }
}

export interface DataWriteOptions<TData extends Data, TUnfinalized extends boolean, TRequest> {
    ref: DataRef<TData['id']>
    read: (opts: DataReadOptions<TData, TRequest>) => AsyncIterableIterator<DataMessage<TData, TUnfinalized>>
}

export interface DataTarget<TData extends Data, TUnfinalized extends boolean, TRequest, TResult> {
    unfinalized: TUnfinalized
    write: (opts: DataWriteOptions<TData, TUnfinalized, TRequest>) => TResult
}

export type DataTargetFactory<TData extends Data, TUnfinalized extends boolean, TRequest, TResult> = (
    opts: DataFactoryOptions<TData, boolean>,
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
    opts: DataFactoryOptions<TInputData, TInputUnfinalized>,
) => DataDuplex<TInputData, TOutputData, TInputUnfinalized, TOutputUnfinalized, TInputRequest, TOutputRequest>

export interface DataPipeOptions<TData extends Data, TRequest> {
    validateBatches?: boolean
    stopOnHead?: boolean
    offset?: TData['id']
    request?: TRequest
}

export interface DataStream<TData extends Data, TUnfinalized extends boolean, TRequest = never> {
    pipe<TResult>(
        targetFactory: (
            opts: DataFactoryOptions<TData, TUnfinalized>,
        ) => DataTarget<TData, TUnfinalized extends true ? true : boolean, TRequest, TResult>,
        opts?: DataPipeOptions<TData, TRequest>,
    ): TResult
    [Symbol.asyncIterator](): AsyncIterableIterator<TData['value']>
}

export function stream<TData extends Data, TUnfinalized extends boolean, TRequest = never>(
    sourceFactory: () => DataSource<TData, TUnfinalized, TRequest>,
): DataStream<TData, TUnfinalized, TRequest> {
    return {
        pipe: (targetFactory) => {
            const source = sourceFactory()
            const target = targetFactory({unfinalized: source.unfinalized})

            return pipe(source, target)
        },
        [Symbol.asyncIterator]: (opts?: DataReadOptions<TData, TRequest>) => {
            const source = sourceFactory()
            return pipe(
                source,
                {
                    unfinalized: source.unfinalized,
                    write: async function* (streamOpts) {
                        for await (const message of streamOpts.read(opts ?? {offset: undefined, request: undefined})) {
                            switch (message.type) {
                                case 'batch':
                                    yield* message.value.data.map((item) => item.value)
                                    break
                                case 'fork':
                                    throw new ForkException(message.value)
                                default:
                                    throw unexpectedCase((message as any).type)
                            }
                        }
                    },
                },
                {
                    offset: opts?.offset,
                    request: opts?.request,
                },
            )
        },
    }
}

function pipe<TData extends Data, TRequest = never, TResult = unknown>(
    source: DataSource<TData, boolean, TRequest>,
    target: DataTarget<TData, boolean, TRequest, TResult>,
    opts: DataPipeOptions<TData, TRequest> = {validateBatches: true},
): TResult {
    if (source.unfinalized && !target.unfinalized) {
        throw new TypeError('Cannot pipe from unfinalized DataSource to finalized DataTarget')
    }

    return target.write({
        ref: source.ref,
        read: async function* (streamOpts) {
            let offset = streamOpts.offset

            for await (const message of source.read(streamOpts)) {
                switch (message.type) {
                    case 'batch': {
                        const batch = message.value
                        if (opts.validateBatches) {
                            if (offset && !source.ref.compare(batch.offset, offset).isGreaterOrEqual) {
                                throw new Error('New offset is below the previous offset')
                            }

                            if (!source.ref.compare(batch.head, batch.offset).isGreaterOrEqual) {
                                throw new Error('Head is below the offset')
                            }

                            if (
                                batch.finalizedHead &&
                                !source.ref.compare(batch.head, batch.finalizedHead).isGreaterOrEqual
                            ) {
                                throw new Error('Head is below the finalized head')
                            }

                            let lastRef = offset
                            for (const item of batch.data) {
                                if (lastRef && !source.ref.compare(item.id, lastRef).isGreater) {
                                    throw new Error('Item is below or equal to the previous item')
                                }
                                lastRef = item.id
                            }

                            if (lastRef && !source.ref.compare(batch.offset, lastRef).isGreaterOrEqual) {
                                throw new Error('Offset is below the data')
                            }

                            if (lastRef && !source.ref.compare(batch.head, lastRef).isGreaterOrEqual) {
                                throw new Error('Head is below the data')
                            }
                        }

                        offset = batch.offset

                        yield {
                            type: 'batch',
                            value: batch,
                        }
                        break
                    }
                    case 'fork': {
                        if (!source.unfinalized) {
                            throw new TypeError('Got fork message from finalized DataSource')
                        }
                        if (!target.unfinalized) {
                            throw new TypeError('Got fork message for finalized DataTarget')
                        }

                        yield {
                            type: 'fork',
                            value: message.value,
                        }
                        break
                    }
                    default: {
                        throw unexpectedCase((message as any).type)
                    }
                }
            }
        },
    })
}

export type DataMessageHandlers<TMessage extends DataMessage<any, any>> = {
    [K in TMessage['type']]: (message: Extract<TMessage, {type: K}>['value']) => any
}

export type DataMessageHandlersReturnType<THandlers extends DataMessageHandlers<any>> = {
    [K in keyof THandlers]: THandlers[K] extends (...args: any) => any ? ReturnType<THandlers[K]> : never
}[keyof THandlers]

export function handleMessage<
    TMessage extends DataMessage<any, any>,
    THandlers extends DataMessageHandlers<TMessage> = DataMessageHandlers<TMessage>,
>(message: TMessage, handlers: THandlers): DataMessageHandlersReturnType<THandlers> {
    switch (message.type) {
        case 'batch': {
            if (!('batch' in handlers)) {
                throw new TypeError('Got batch message but batch handler is not defined')
            }
            if (typeof handlers.batch !== 'function') {
                throw new TypeError('Batch handler is not a function')
            }
            return handlers.batch(message.value)
        }
        case 'fork': {
            if (!('fork' in handlers)) {
                throw new TypeError('Got fork message but fork handler is not defined')
            }
            if (typeof handlers.fork !== 'function') {
                throw new TypeError('Fork handler is not a function')
            }
            return handlers.fork?.(message.value)
        }
        default: {
            throw unexpectedCase((message as any).type)
        }
    }
}
