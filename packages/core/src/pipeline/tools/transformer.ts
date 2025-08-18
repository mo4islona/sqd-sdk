import type {Data, DataBatch, DataFork, DataRef} from '../data'
import type {
    DataDuplex,
    DataDuplexFactory,
    DataFactoryOptions,
    DataReaderOptions,
    DataWriter,
    DataWriterContext,
    WriterOperationResult,
} from '../core'
import {createSource, createTarget} from '../core'
import type {Awaitable, Maybe} from '../../internal/types'
import {createFuture, type Future, SyncQueue} from '../../internal/async'

export interface DataTransformer<TInputData extends Data, TOutputData extends Data, TUnfinalized extends boolean> {
    unfinalized: TUnfinalized
    ref: DataRef<TOutputData['id']>
    transformer: (
        opts: DataReaderOptions<TOutputData>
    ) => Awaitable<DataTransformerTransformer<TInputData, TOutputData>>
}

export type DataTransformerFactory<TInputData extends Data, TOutputData extends Data, TUnfinalized extends boolean> = (
    opts: DataFactoryOptions<TInputData, TUnfinalized>
) => Promise<DataTransformer<TInputData, TOutputData, TUnfinalized>>

export interface DataTransformerTransformer<TInputData extends Data, TOutputData extends Data>
    extends WriterOperationResult<TInputData> {
    transform(batch: DataBatch<TInputData>, ctx: DataWriterContext<TInputData>): Promise<DataBatch<TOutputData>>
    fork(fork: DataFork<TInputData['id']>, ctx: DataWriterContext<TInputData>): Promise<DataFork<TOutputData['id']>>
    flush?(): Promise<DataBatch<TOutputData>>
}

export function createTransformer<TInputData extends Data, TOutputData extends Data, TUnfinalized extends boolean>(
    transformerOrFactory:
        | DataTransformer<TInputData, TOutputData, TUnfinalized>
        | DataTransformerFactory<TInputData, TOutputData, TUnfinalized>
): DataDuplexFactory<TInputData, TOutputData, TUnfinalized, TUnfinalized> {
    return async (opts) => {
        if (typeof transformerOrFactory === 'function') {
            const transformer = await transformerOrFactory(opts)
            return createTransformer(transformer)(opts)
        }

        let writerFuture: Future<DataWriter<TInputData, TUnfinalized>> | undefined = undefined

        const target = createTarget<TInputData, TUnfinalized>({
            unfinalized: opts.unfinalized,
            writer: () => {
                if (!writerFuture) {
                    writerFuture = createFuture()
                }

                return writerFuture.promise()
            },
        })

        const source = createSource<TOutputData, TUnfinalized>({
            unfinalized: opts.unfinalized,
            ref: opts.ref,
            reader: async (readerOpts) => {
                const transformer = await transformerOrFactory.transformer({
                    offset: readerOpts.offset,
                    request: readerOpts.request,
                })

                const queue = new SyncQueue<DataBatch<TOutputData>>()

                if (!writerFuture) {
                    writerFuture = createFuture()
                }

                writerFuture.resolve({
                    offset: transformer.offset,
                    request: transformer.request,
                    next: async (batch, ctx) => {
                        if (queue.isClosed) {
                            return {done: true, value: undefined}
                        }

                        const outputBatch = await transformer.transform(batch, ctx)
                        await queue.put(outputBatch)

                        return {done: false, value: {offset: outputBatch.offset}}
                    },
                    return: async () => {
                        queue.close()
                        return {done: true, value: undefined}
                    },
                    throw: async (err) => {
                        queue.close()
                        throw err
                    },
                    fork: async (fork, ctx) => {
                        return {done: true, value: undefined}
                    },
                })

                return {
                    async next() {
                        if (queue.isClosed) {
                            if (transformer.flush) {
                                const batch = await transformer.flush()
                                return {done: false, value: batch}
                            }

                            return {done: true, value: undefined}
                        }

                        const batch = await queue.take()
                        if (!batch) {
                            queue.close()
                            return {done: true, value: undefined}
                        }

                        return {done: false, value: batch}
                    },

                    async return() {
                        writerFuture = undefined
                        queue.close()
                        return {done: true, value: undefined}
                    },
                    async throw(err) {
                        queue.close()
                        throw err
                    },
                }
            },
        })

        return {
            target: await target(opts),
            source: await source(),
        }
    }
}
