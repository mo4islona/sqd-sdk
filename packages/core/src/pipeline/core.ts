import {isForkException} from './errors'
import type {Data, DataBatch, DataFork, DataRef} from './data'
import type {Awaitable, Maybe} from '../internal/types'

export interface DataReaderOptions<TData extends Data> {
    offset: Maybe<TData['id']>
}

export interface DataReader<TData extends Data> extends AsyncIterator<DataBatch<TData>> {}

export interface DataReaderReadOptions<TData extends Data> {
    offset: Maybe<TData['id']>
}

export interface DataWriterOptions<TData extends Data> {
    // FIXME: silence linter
    _?: TData
}

export interface DataSource<T extends Data, TUnfinalized extends boolean> {
    unfinalized: TUnfinalized
    ref: DataRef<T['id']>
    reader: (opts: DataReaderOptions<T>) => Awaitable<DataReader<T>>
}

export function createSource<TData extends Data, TUnfinalized extends boolean>(
    source: DataSource<TData, TUnfinalized>
): DataSource<TData, TUnfinalized> {
    return {
        ...source,
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

export interface DataWriterContext<TData extends Data> {
    offset: Maybe<TData['id']>
}

export interface FinalizedDataWriter<TData extends Data, TReturn = unknown> {
    offset: Maybe<TData['id']>
    next(batch: DataBatch<TData>, ctx: DataWriterContext<TData>): Promise<IteratorResult<Maybe<TData['id']>, TReturn>>
    return?(): Promise<IteratorReturnResult<TReturn>>
    fork?(
        fork: DataFork<TData['id']>,
        ctx: DataWriterContext<TData>
    ): Promise<IteratorResult<Maybe<TData['id']>, TReturn>>
    throw?(err: any): Promise<void>
}

export interface UnfinalizedDataWriter<TData extends Data, TReturn = unknown>
    extends FinalizedDataWriter<TData, TReturn> {
    fork(
        fork: DataFork<TData['id']>,
        ctx: DataWriterContext<TData>
    ): Promise<IteratorResult<TData['id'] | undefined, TReturn>>
}

export type DataWriter<TData extends Data, TUnfinalized extends boolean> = TUnfinalized extends true
    ? UnfinalizedDataWriter<TData>
    : FinalizedDataWriter<TData>

export interface DataTarget<TData extends Data, TUnfinalized extends boolean> {
    unfinalized: TUnfinalized
    writer: (opts: DataWriterOptions<TData>) => Awaitable<DataWriter<TData, NoInfer<TUnfinalized>>>
}

export function createTarget<TData extends Data, TUnfinalized extends boolean>(
    target: DataTarget<TData, TUnfinalized>
): DataTarget<TData, TUnfinalized> {
    return target
}

export interface DataDuplex<
    TData extends Data,
    UData extends Data,
    TUnfinalized extends boolean,
    UUnfinalized extends boolean
> {
    target: DataTarget<TData, TUnfinalized>
    source: DataSource<UData, UUnfinalized>
}

export interface DataFactoryOptions<TData extends Data, TUnfinalized extends boolean> {
    unfinalized: TUnfinalized
    ref: DataRef<TData['id']>
}

export type DataTargetFactory<TData extends Data, TUnfinalized extends boolean> = (
    opts: DataFactoryOptions<TData, TUnfinalized>
) => DataTarget<TData, TUnfinalized>

export type DataDuplexFactory<
    TData extends Data,
    UData extends Data,
    TUnfinalized extends boolean,
    UUnfinalized extends boolean
> = (opts: DataFactoryOptions<TData, TUnfinalized>) => DataDuplex<TData, UData, TUnfinalized, UUnfinalized>

export interface DataPipeOptions {
    validateBatches?: boolean
}

export interface Pipeline<TData extends Data, TUnfinalized extends boolean> {
    pipeThrough<UData extends Data, UUnfinalized extends boolean>(
        duplexFactory: (
            opts: DataFactoryOptions<TData, TUnfinalized>
        ) => Awaitable<DataDuplex<TData, UData, TUnfinalized extends true ? true : boolean, UUnfinalized>>,
        opts?: DataPipeOptions
    ): Pipeline<UData, UUnfinalized>
    pipeTo(
        targetFactory: (
            opts: DataFactoryOptions<TData, TUnfinalized>
        ) => Awaitable<DataTarget<TData, TUnfinalized extends true ? true : boolean>>,
        opts?: DataPipeOptions
    ): Promise<void>
    [Symbol.asyncIterator](): AsyncIterableIterator<DataBatch<TData>>
}

export function pipeline<TData extends Data, TUnfinalized extends boolean>(
    sourceFactory: () => Awaitable<DataSource<TData, TUnfinalized>>
): Pipeline<TData, TUnfinalized> {
    return {
        pipeThrough: (duplexFactory) => {
            return pipeline(async () => {
                const source = await sourceFactory()
                const duplex = await duplexFactory({
                    unfinalized: source.unfinalized as TUnfinalized,
                    ref: source.ref,
                })

                return createSource({
                    unfinalized: duplex.source.unfinalized,
                    ref: duplex.source.ref,
                    reader: async (opts) => {
                        const reader = await duplex.source.reader(opts)

                        const pipePromise = pipe(source, duplex.target).catch((err) => {
                            throw err
                        })

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
                        }
                    },
                })
            })
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

            let reader: DataReader<TData> | undefined
            return {
                next: async () => {
                    if (!reader) {
                        const source = await sourceFactory()
                        reader = await source.reader({offset})
                    }
                    return reader.next()
                },
                return: async () => {
                    let result = await reader?.return?.()
                    return result ? result : {done: true, value: undefined}
                },
                throw: async (err) => {
                    let result = await reader?.throw?.(err)
                    return result ? result : {done: true, value: undefined}
                },
                [Symbol.asyncIterator]() {
                    return this
                },
            }
        },
    }
}

async function pipe<TData extends Data, TUnfinalized extends boolean>(
    source: DataSource<TData, TUnfinalized>,
    target: DataTarget<TData, TUnfinalized extends true ? true : boolean>,
    opts: DataPipeOptions = {validateBatches: true}
): Promise<void> {
    if (source.unfinalized && !target.unfinalized) {
        throw new TypeError('Cannot pipe from unfinalized DataSource to finalized DataTarget')
    }

    const processData = async (writer: DataWriter<TData, boolean>, ctx: DataWriterContext<TData>): Promise<unknown> => {
        const reader = await source.reader({offset: ctx.offset ?? writer.offset})
        return processStream(reader, writer, ctx)
    }

    const processStream = async (
        reader: DataReader<TData>,
        writer: DataWriter<TData, boolean>,
        ctx: DataWriterContext<TData>
    ): Promise<unknown> => {
        let batch: DataBatch<TData> | undefined
        try {
            const {done, value} = await reader.next()
            if (done) return writer.return?.()
            batch = value
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
            if (done) return value

            return processData(writer, {offset: value})
        }

        const {value, done} = await writer.next(batch, ctx)
        if (done) {
            await reader.return?.()
            return value
        }

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
                if (lastRef && !source.ref.compare(item.id, lastRef).isGreaterOrEqual) {
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

        // NOTE: If the offset is not the same as the batch offset,
        // it means that the batch was not fully consumed or we want to skip
        // so we break the current stream and start from the new offset
        // FIXME: Do we want this behavior?
        if (!value || !source.ref.compare(value, batch.offset).isEqual) {
            // FIXME: looks like a hack, revisit this
            await reader.return?.()
            return processData(writer, {offset: value})
        }

        return processStream(reader, writer, {offset: batch.offset})
    }

    const writer = await target.writer({})
    await processData(writer, {offset: undefined})
}
