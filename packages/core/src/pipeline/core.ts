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

export interface DataReadOptions<TData extends Data, TRequest = unknown> {
    offset: Maybe<TData['id']>
    request?: TRequest
}

export interface DataReader<TData extends Data> extends AsyncIterator<DataBatch<TData>> {}

export interface DataFactoryOptions<TData extends Data, TUnfinalized extends boolean> {
    unfinalized: TUnfinalized
    ref: DataRef<TData['id']>
}

export interface DataSource<T extends Data, TUnfinalized extends boolean, TRequest = never> {
    unfinalized: TUnfinalized
    ref: DataRef<T['id']>
    read: (opts: DataReadOptions<T, TRequest>) => AsyncIterableIterator<DataMessage<T, TUnfinalized>>
}

export type DataSourceFactory<TData extends Data, TUnfinalized extends boolean, TRequest = never> = () => Promise<
    DataSource<TData, TUnfinalized, TRequest>
>

export function createSource<TData extends Data, TUnfinalized extends boolean, TRequest = never>(
    sourceOrFactory: DataSource<TData, TUnfinalized, TRequest> | DataSourceFactory<TData, TUnfinalized, TRequest>,
): DataSourceFactory<TData, TUnfinalized, TRequest> {
    return async () => {
        if (typeof sourceOrFactory === 'function') {
            return await sourceOrFactory()
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

export interface DataWriteOptions<TData extends Data, TUnfinalized extends boolean, TRequest = unknown> {
    ref: DataRef<TData['id']>
    read: (opts: DataReadOptions<TData, TRequest>) => AsyncIterableIterator<DataMessage<TData, TUnfinalized>>
}

export interface DataTarget<TData extends Data, TUnfinalized extends boolean, TRequest = never, TResult = unknown> {
    unfinalized: TUnfinalized
    write: (opts: DataWriteOptions<TData, TUnfinalized, TRequest>) => Promise<TResult>
}

export type DataTargetFactory<TData extends Data, TUnfinalized extends boolean, TRequest = never, TResult = unknown> = (
    opts: DataFactoryOptions<TData, TUnfinalized>,
) => Promise<DataTarget<TData, TUnfinalized, TRequest, TResult>>

export function createTarget<TData extends Data, TUnfinalized extends boolean, TRequest = never, TResult = unknown>(
    targetOrFactory:
        | DataTarget<TData, TUnfinalized, TRequest, TResult>
        | DataTargetFactory<TData, TUnfinalized, TRequest, TResult>,
): DataTargetFactory<TData, TUnfinalized, TRequest, TResult> {
    return async (opts) => {
        if (typeof targetOrFactory === 'function') {
            return await targetOrFactory(opts)
        }

        return targetOrFactory
    }
}

export interface DataDuplex<
    TInputData extends Data,
    TOutputData extends Data,
    TInputUnfinalized extends boolean,
    TOutputUnfinalized extends boolean,
    TInputRequest = never,
    TOutputRequest = never,
> {
    target: DataTarget<TInputData, TInputUnfinalized, TInputRequest, unknown>
    source: DataSource<TOutputData, TOutputUnfinalized, TOutputRequest>
}

export type DataDuplexFactory<
    TInputData extends Data,
    TOutputData extends Data,
    TInputUnfinalized extends boolean,
    TOutputUnfinalized extends boolean,
    TInputRequest = never,
    TOutputRequest = never,
> = (
    opts: DataFactoryOptions<TInputData, TInputUnfinalized>,
) => Promise<DataDuplex<TInputData, TOutputData, TInputUnfinalized, TOutputUnfinalized, TInputRequest, TOutputRequest>>

export interface DataPipeOptions<TData extends Data, TRequest> {
    validateBatches?: boolean
    stopOnHead?: boolean
    offset?: TData['id']
    request?: TRequest
}

export interface Pipeline<TData extends Data, TUnfinalized extends boolean, TRequest = never> {
    pipeThrough<UData extends Data, UUnfinalized extends boolean, URequest = never>(
        duplexFactory: (
            opts: DataFactoryOptions<TData, TUnfinalized>,
        ) => Awaitable<
            DataDuplex<TData, UData, TUnfinalized extends true ? true : boolean, UUnfinalized, TRequest, URequest>
        >,
        opts?: DataPipeOptions<TData, TRequest>,
    ): Pipeline<UData, UUnfinalized, URequest>
    pipeTo<TResult>(
        targetFactory: (
            opts: DataFactoryOptions<TData, TUnfinalized>,
        ) => Awaitable<DataTarget<TData, TUnfinalized extends true ? true : boolean, TRequest, TResult>>,
        opts?: DataPipeOptions<TData, TRequest>,
    ): Promise<TResult>
    [Symbol.asyncIterator](): AsyncIterableIterator<DataBatch<TData>>
}

export function pipeline<TData extends Data, TUnfinalized extends boolean, TRequest = never>(
    sourceFactory: () => Awaitable<DataSource<TData, TUnfinalized, TRequest>>,
): Pipeline<TData, TUnfinalized, TRequest> {
    return {
        pipeThrough: (duplexFactory) => {
            return pipeline(
                createSource(async () => {
                    const source = await sourceFactory()
                    const duplex = await duplexFactory({
                        unfinalized: source.unfinalized,
                        ref: source.ref,
                    })

                    return {
                        unfinalized: duplex.source.unfinalized,
                        ref: duplex.source.ref,
                        read: async function* (opts) {
                            const pipePromise = pipe(source, duplex.target).catch((err) => {
                                throw err
                            })
                            yield* duplex.source.read(opts)

                            await pipePromise
                        },
                    }
                }),
            )
        },
        pipeTo: async (targetFactory) => {
            const source = await sourceFactory()
            const target = await targetFactory({
                unfinalized: source.unfinalized,
                ref: source.ref,
            })

            return pipe(source, target)
        },
        [Symbol.asyncIterator](opts?: DataReadOptions<TData, TRequest>) {
            const offset = opts?.offset

            let reader: AsyncIterableIterator<DataMessage<TData, TUnfinalized>> | undefined
            return {
                next: async () => {
                    if (!reader) {
                        const source = await sourceFactory()
                        reader = source.read({offset})
                    }
                    const result = await reader.next()
                    if (result.done) {
                        return {done: true, value: undefined}
                    }

                    switch (result.value.type) {
                        case 'batch':
                            return {value: result.value.value, done: false}
                        case 'fork':
                            throw new ForkException(result.value.value)
                    }
                },
                return: async () => {
                    await reader?.return?.()
                    return {done: true, value: undefined}
                },
                throw: async (err) => {
                    await reader?.throw?.(err)
                    throw err
                },
                [Symbol.asyncIterator]() {
                    return this
                },
            }
        },
    }
}

async function pipe<TData extends Data, TRequest = never, TResult = unknown>(
    source: DataSource<TData, boolean, TRequest>,
    target: DataTarget<TData, boolean, TRequest, TResult>,
    opts: DataPipeOptions<TData, TRequest> = {validateBatches: true},
): Promise<TResult> {
    if (source.unfinalized && !target.unfinalized) {
        throw new TypeError('Cannot pipe from unfinalized DataSource to finalized DataTarget')
    }

    return target.write({
        ref: source.ref,
        read: async function* (streamOpts) {
            let offset = streamOpts.offset

            for await (const message of source.read(streamOpts)) {
                yield handleMessage(message, {
                    batch: (batch): DataBatchMessage<TData> => {
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

                        return {
                            type: 'batch',
                            value: batch,
                        }
                    },
                    fork: (fork): DataForkMessage<TData> => {
                        if (!source.unfinalized) {
                            throw new TypeError('Got fork message from finalized DataSource')
                        }
                        if (!target.unfinalized) {
                            throw new TypeError('Got fork message for finalized DataTarget')
                        }

                        return {
                            type: 'fork',
                            value: fork,
                        }
                    },
                })
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
