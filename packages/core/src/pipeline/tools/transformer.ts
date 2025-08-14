import {SyncQueue, type Future, createFuture} from '../../internal/async'
import {type DataDuplexFactory, DataTarget, type DataWriterOptions, DataSource} from '../core'
import type {Data, DataRef, DataBatch, DataFork} from '../data'

export interface DataTransformer<TData extends Data, UData extends Data> {
    offset: TData['id'] | undefined
    ref: DataRef<UData['id']>
    transform: (batch: DataBatch<TData>) => Promise<DataBatch<UData>>
    fork?: (fork: DataFork<TData>) => Promise<DataFork<UData> | undefined>
}

export interface TransformerOptions<TData extends Data> {
    offset: TData['id'] | undefined
    ref: DataRef<TData['id']>
}

export interface TransformerConfig<TData extends Data, UData extends Data> {
    transformer: (opts: TransformerOptions<UData>) => PromiseLike<DataTransformer<TData, UData>>
}

export function transformer<TData extends Data, UData extends Data, TUnfinalized extends boolean>(
    config: TransformerConfig<TData, UData>
): DataDuplexFactory<TData, UData, TUnfinalized, TUnfinalized> {
    return (parent) => {
        const queue = new SyncQueue<DataBatch<UData>>()
        let offsetFuture: Future<UData['id'] | undefined> = createFuture()
        let refFuture: Future<DataRef<UData['id']>> = createFuture()

        const target = new DataTarget<TData, TUnfinalized>({
            unfinalized: parent.unfinalized,
            writer: async (opts: DataWriterOptions<TData>) => {
                const transformer = await config.transformer({
                    offset: await offsetFuture.promise(),
                    ref: opts.ref,
                })

                refFuture.resolve(transformer.ref)

                if (parent.unfinalized && !transformer.fork) {
                    throw new TypeError('Missing fork method in unfinalized DataTransformer')
                }

                return {
                    offset: transformer.offset,
                    async next(batch: DataBatch<TData>) {
                        if (queue.isClosed) return {done: true, value: undefined}

                        const data = await transformer.transform(batch)
                        await queue.put(data)
                        return {done: false, value: batch.offset}
                    },
                    async fork(fork) {
                        return {done: true, value: undefined}
                    },
                    async return() {
                        queue.close()
                        return {done: true, value: undefined}
                    },
                }
            },
        })

        const source = new DataSource<UData, TUnfinalized>({
            unfinalized: parent.unfinalized,
            ref: {} as any,
            reader: async (opts) => {
                offsetFuture.resolve(opts.offset)

                const ref = await refFuture.promise()

                return {
                    ref,
                    async next(): Promise<IteratorResult<DataBatch<UData>>> {
                        if (queue.isClosed) return {done: true, value: undefined}

                        const value = await queue.take()
                        return value ? {done: false, value} : {done: true, value: undefined}
                    },
                    async return(): Promise<IteratorResult<DataBatch<UData>>> {
                        queue.close()
                        return {done: true, value: undefined}
                    },
                }
            },
        })

        // FIXME: how to type this?
        return {
            target,
            source,
        }
    }
}
