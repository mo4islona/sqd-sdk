import type {Data, DataBatch, DataFork, DataRef} from '../data'
import type {DataDuplex, DataFactoryOptions, DataReader, DataWriter, DataWriterContext} from '../core'
import {createSource, createTarget} from '../core'
import type {Awaitable, Maybe} from '../../internal/types'
import {createFuture, type Future, SyncQueue} from '../../internal/async'

export interface DataTransformer<TInputData extends Data, TOutputData extends Data, TUnfinalized extends boolean> {
    unfinalized: TUnfinalized
    ref: DataRef<TOutputData['id']>
    transformer: ({
        offset,
    }: {
        offset: Maybe<TInputData['id']>
    }) => Awaitable<DataTransformerTransformer<TInputData, TOutputData>>
}

export interface DataTransformerTransformer<TInputData extends Data, TOutputData extends Data> {
    offset: Maybe<TInputData['id']>
    transform(batch: DataBatch<TInputData>, ctx: DataWriterContext<TInputData>): Promise<DataBatch<TOutputData>>
    fork(fork: DataFork<TInputData['id']>, ctx: DataWriterContext<TInputData>): Promise<DataFork<TOutputData['id']>>
}

export async function createTransformer<
    TInputData extends Data,
    TOutputData extends Data,
    TUnfinalized extends boolean
>(
    opts: DataTransformer<TInputData, TOutputData, TUnfinalized>
): Promise<DataDuplex<TInputData, TOutputData, TUnfinalized, TUnfinalized>> {
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

    let index = 0

    const source = createSource<TOutputData, TUnfinalized>({
        unfinalized: opts.unfinalized,
        ref: opts.ref,
        reader: async (readerOpts) => {
            const transformer = await opts.transformer({offset: readerOpts.offset})

            const queue = new SyncQueue<DataBatch<TOutputData>>()

            if (!writerFuture) {
                writerFuture = createFuture()
            }

            const num = index++

            writerFuture.resolve({
                offset: transformer.offset,
                next: async (batch, ctx) => {
                    if (queue.isClosed) {
                        console.log(`transformer writer ${num} is closed by next`)
                        return {done: true, value: undefined}
                    }

                    const outputBatch = await transformer.transform(batch, ctx)
                    await queue.put(outputBatch)

                    return {done: false, value: {offset: outputBatch.offset}}
                },
                return: async () => {
                    console.log(`transformer writer ${num} is closed by return`)
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
                        console.log(`transformer reader ${num} is closed by next`)
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
                    console.log(`transformer reader ${num} is closed by return`)
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
        target: target,
        source: source,
    }
}
