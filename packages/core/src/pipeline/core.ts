import {ForkException, isForkException} from './errors'
import type {Data, DataBatch, DataFork, DataRef} from './data'
import type {Awaitable, Maybe} from '../internal/types'

export interface DataBatchMessage<TData extends Data> {
    type: 'batch'
    batch: DataBatch<TData>
}

export interface DataForkMessage<TData extends Data> {
    type: 'fork'
    fork: DataFork<TData['id']>
}

export type DataMessage<TData extends Data, TUnfinalized extends boolean> = TUnfinalized extends true
    ? DataBatchMessage<TData> | DataForkMessage<TData>
    : DataBatchMessage<TData>

export interface DataReaderOptions<TData extends Data, TRequest = unknown> {
    offset: Maybe<TData['id']>
    request?: TRequest
}

export interface DataReader<TData extends Data> extends AsyncIterator<DataBatch<TData>> {}

export interface DataFactoryOptions<TData extends Data, TUnfinalized extends boolean> {
    unfinalized: TUnfinalized
    ref: DataRef<TData['id']>
}

export interface DataSource<T extends Data, TUnfinalized extends boolean> {
    unfinalized: TUnfinalized
    ref: DataRef<T['id']>
    read: (opts: DataReaderOptions<T>) => AsyncIterableIterator<DataMessage<T, TUnfinalized>>
}

export type DataSourceFactory<TData extends Data, TUnfinalized extends boolean> = () => Promise<
    DataSource<TData, TUnfinalized>
>

export function createSource<TData extends Data, TUnfinalized extends boolean>(
    sourceOrFactory: DataSource<TData, TUnfinalized> | DataSourceFactory<TData, TUnfinalized>,
): DataSourceFactory<TData, TUnfinalized> {
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

export interface DataWriterContext<TData extends Data> {
    offset: Maybe<TData['id']>
}

export interface WriterOperationResult<TData extends Data, TRequest = unknown> {
    offset: Maybe<TData['id']>
    request?: TRequest
}

export interface FinalizedDataWriter<TData extends Data, TReturn = unknown> extends WriterOperationResult<TData> {
    next(
        batch: DataBatch<TData>,
        ctx: DataWriterContext<TData>,
    ): Promise<IteratorResult<Maybe<WriterOperationResult<TData>>, TReturn>>
    return?(): Promise<IteratorReturnResult<TReturn>>
    fork?(
        fork: DataFork<TData['id']>,
        ctx: DataWriterContext<TData>,
    ): Promise<IteratorResult<WriterOperationResult<TData>, TReturn>>
    throw?(err: any): Promise<void>
}

export interface UnfinalizedDataWriter<TData extends Data, TReturn = unknown>
    extends FinalizedDataWriter<TData, TReturn> {
    fork(
        fork: DataFork<TData['id']>,
        ctx: DataWriterContext<TData>,
    ): Promise<IteratorResult<WriterOperationResult<TData>, TReturn>>
}

export type DataWriter<TData extends Data, TUnfinalized extends boolean, TResult = unknown> = TUnfinalized extends true
    ? UnfinalizedDataWriter<TData, TResult>
    : FinalizedDataWriter<TData, TResult>

export interface DataTarget<TData extends Data, TUnfinalized extends boolean, TResult = unknown> {
    unfinalized: TUnfinalized
    write: (
        stream: (opts: DataReaderOptions<TData>) => AsyncIterableIterator<DataMessage<TData, TUnfinalized>>,
    ) => Promise<TResult>
}

export type DataTargetFactory<TData extends Data, TUnfinalized extends boolean, TResult = unknown> = (
    opts: DataFactoryOptions<TData, TUnfinalized>,
) => Promise<DataTarget<TData, TUnfinalized, TResult>>

export function createTarget<TData extends Data, TUnfinalized extends boolean>(
    targetOrFactory: DataTarget<TData, TUnfinalized> | DataTargetFactory<TData, TUnfinalized>,
): DataTargetFactory<TData, TUnfinalized> {
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
> {
    target: DataTarget<TInputData, TInputUnfinalized>
    source: DataSource<TOutputData, TOutputUnfinalized>
}

export type DataDuplexFactory<
    TInputData extends Data,
    TOutputData extends Data,
    TInputUnfinalized extends boolean,
    TOutputUnfinalized extends boolean,
> = (
    opts: DataFactoryOptions<TInputData, TInputUnfinalized>,
) => Promise<DataDuplex<TInputData, TOutputData, TInputUnfinalized, TOutputUnfinalized>>

export interface DataPipeOptions<TData extends Data> {
    validateBatches?: boolean
    stopOnHead?: boolean
    offset?: TData['id']
}

export interface Pipeline<TData extends Data, TUnfinalized extends boolean> {
    pipeThrough<UData extends Data, UUnfinalized extends boolean>(
        duplexFactory: (
            opts: DataFactoryOptions<TData, TUnfinalized>,
        ) => Awaitable<DataDuplex<TData, UData, TUnfinalized extends true ? true : boolean, UUnfinalized>>,
        opts?: DataPipeOptions<TData>,
    ): Pipeline<UData, UUnfinalized>
    pipeTo<TResult>(
        targetFactory: (
            opts: DataFactoryOptions<TData, TUnfinalized>,
        ) => Awaitable<DataTarget<TData, TUnfinalized extends true ? true : boolean, TResult>>,
        opts?: DataPipeOptions<TData>,
    ): Promise<TResult>
    [Symbol.asyncIterator](): AsyncIterableIterator<DataBatch<TData>>
}

export function pipeline<TData extends Data, TUnfinalized extends boolean>(
    sourceFactory: () => Awaitable<DataSource<TData, TUnfinalized>>,
): Pipeline<TData, TUnfinalized> {
    return {
        pipeThrough: (duplexFactory) => {
            return pipeline(
                createSource(async () => {
                    const source = await sourceFactory()
                    const duplex = await duplexFactory({
                        unfinalized: source.unfinalized as TUnfinalized,
                        ref: source.ref,
                    })

                    return {
                        unfinalized: duplex.source.unfinalized,
                        ref: duplex.source.ref,
                        read: (opts) => {
                            const pipePromise = pipe(source, duplex.target).catch((err) => {
                                throw err
                            })

                            const reader = duplex.source.read(opts)
                            return {
                                next: async () => {
                                    const result = await reader.next()
                                    if (result.done) {
                                        await pipePromise
                                    }
                                    return result
                                },
                                return: async () => {
                                    const result = await reader.return?.()
                                    await pipePromise
                                    return result ? result : {done: true, value: undefined}
                                },
                                throw: async (err) => {
                                    const result = await reader.throw?.(err)
                                    await pipePromise
                                    return result ? result : {done: true, value: undefined}
                                },
                                [Symbol.asyncIterator]() {
                                    return this
                                },
                            }
                        },
                    }
                }),
            )
        },
        pipeTo: async (targetFactory) => {
            const source = await sourceFactory()
            const target = await targetFactory({
                unfinalized: source.unfinalized as TUnfinalized,
                ref: source.ref,
            })

            return pipe(source, target)
        },
        [Symbol.asyncIterator](opts?: DataReaderOptions<TData>) {
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
                            return {value: result.value.batch, done: false}
                        case 'fork':
                            throw new ForkException(result.value.fork)
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

async function pipe<TData extends Data, TUnfinalized extends boolean, TResult>(
    source: DataSource<TData, TUnfinalized>,
    target: DataTarget<TData, TUnfinalized extends true ? true : boolean, TResult>,
    opts: DataPipeOptions<TData> = {validateBatches: true},
): Promise<TResult> {
    if (source.unfinalized && !target.unfinalized) {
        throw new TypeError('Cannot pipe from unfinalized DataSource to finalized DataTarget')
    }

    const processData = async (
        writer: DataWriter<TData, boolean>,
        opts: DataReaderOptions<TData>,
    ): Promise<TResult> => {
        const reader = await source.reader({
            offset: opts.offset ?? writer.offset,
            request: opts.request ?? writer.request,
        })
        return processStream(reader, writer, opts)
    }

    const processStream = async (
        reader: DataReader<TData>,
        writer: DataWriter<TData, boolean>,
        ctx: DataWriterContext<TData>,
    ): Promise<TResult> => {
        let result: IteratorResult<DataBatch<TData>, TResult> | undefined
        try {
            result = await reader.next()
        } catch (err) {
            if (!isForkException<TData>(err)) {
                throw err
            }
            if (!source.unfinalized) {
                throw new TypeError('Got fork exception from finalized DataSource')
            }
            if (!target.unfinalized) {
                throw new TypeError('Got fork exception for finalized DataTarget')
            }
            if (!writer.fork) {
                throw new TypeError('Missing fork method in unfinalized DataWriter')
            }

            const {value, done} = await writer.fork(err.fork, ctx)
            // FIXME: how to type this?
            if (done) return value as TResult

            return processData(writer, {offset: value})
        }
        if (result.done) {
            const result = await writer.return?.()
            if (result && !result.done) {
                throw new Error('Writer returned a non-done result in return')
            }
            // FIXME: how to type this?
            return result?.value as TResult
        }

        const batch = result.value
        if (opts.validateBatches) {
            if (ctx.offset && !source.ref.compare(batch.offset, ctx.offset).isGreaterOrEqual) {
                throw new Error('New offset is below the previous offset')
            }

            if (!source.ref.compare(batch.head, batch.offset).isGreaterOrEqual) {
                throw new Error('Head is below the offset')
            }

            if (batch.finalizedHead && !source.ref.compare(batch.head, batch.finalizedHead).isGreaterOrEqual) {
                throw new Error('Head is below the finalized head')
            }

            let lastRef = ctx.offset
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

        const {value, done} = await writer.next(batch, ctx)
        if (done) {
            await reader.return?.()
            return value as TResult
        }

        // FIXME: Do we want this behavior?
        if (value) {
            await reader.return?.()
            return processData(writer, {
                offset: value.offset,
                request: value.request,
            })
        }

        return processStream(reader, writer, {offset: batch.offset})
    }

    const writer = await target.writer()
    return await processData(writer, {offset: opts.offset})
}
