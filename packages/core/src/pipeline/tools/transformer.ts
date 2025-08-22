import type {Data, DataRef} from '../data'
import type {
    DataDuplex,
    DataDuplexFactory,
    DataFactoryOptions,
    DataReadOptions,
    DataMessage,
    DataWriteOptions,
    DataTarget,
    DataSource,
    DataStream,
} from '../core'
import {createSource, createTarget, stream} from '../core'
import {createFuture, SyncQueue, type Future} from '../../internal/async'

export interface DataTransformer<
    TInputData extends Data,
    TOutputData extends Data,
    TUnfinalized extends boolean,
    TInputRequest,
    TOutputRequest,
> {
    unfinalized: TUnfinalized
    ref: DataRef<TOutputData['id']>
    transform: (
        opts: DataReadOptions<TOutputData, TOutputRequest> & DataWriteOptions<TInputData, TUnfinalized, TInputRequest>,
    ) => AsyncIterableIterator<DataMessage<TOutputData, TUnfinalized>>
}

export type DataTransformerFactory<
    TInputData extends Data,
    TOutputData extends Data,
    TUnfinalized extends boolean,
    TInputRequest,
    TOutputRequest,
> = (
    opts: DataFactoryOptions<TInputData, TUnfinalized>,
) => DataTransformer<TInputData, TOutputData, TUnfinalized, TInputRequest, TOutputRequest>

export function createTransformer<
    TInputData extends Data,
    TOutputData extends Data,
    TUnfinalized extends boolean,
    TInputRequest,
    TOutputRequest,
>(
    transformerOrFactory:
        | DataTransformer<TInputData, TOutputData, TUnfinalized, TInputRequest, TOutputRequest>
        | DataTransformerFactory<TInputData, TOutputData, TUnfinalized, TInputRequest, TOutputRequest>,
): DataDuplexFactory<TInputData, TOutputData, TUnfinalized, TUnfinalized, TInputRequest, TOutputRequest> {
    return (opts) => {
        if (typeof transformerOrFactory === 'function') {
            const transformer = transformerOrFactory(opts)
            return createTransformer(transformer)(opts)
        }

        const target = createTarget<
            TInputData,
            TUnfinalized,
            TInputRequest,
            DataStream<TOutputData, TUnfinalized, TOutputRequest>
        >({
            unfinalized: opts.unfinalized,
            write: (writeOpts) => {
                const queue = new SyncQueue<DataMessage<TOutputData, TUnfinalized>>()
                let readOptsFuture: Future<DataReadOptions<TOutputData, TOutputRequest>> = createFuture()

                return stream(
                    createSource<TOutputData, TUnfinalized, TOutputRequest>({
                        unfinalized: transformerOrFactory.unfinalized,
                        ref: transformerOrFactory.ref,
                        read: (readOpts) => {
                            Promise.resolve()
                                .then(async () => {
                                    try {
                                        for await (const message of transformerOrFactory.transform({
                                            ...writeOpts,
                                            ...readOpts,
                                        })) {
                                            await queue.put(message)
                                            if (queue.isClosed) break
                                        }
                                    } finally {
                                        queue.close()
                                    }
                                })
                                .catch((e) => {
                                    throw e
                                })

                            return {
                                next: async () => {
                                    const message = await queue.take()
                                    if (message) {
                                        return {done: false, value: message}
                                    }
                                    return {done: true, value: undefined}
                                },
                                return: async () => {
                                    queue.close()
                                    readOptsFuture = createFuture()
                                    return {done: true, value: undefined}
                                },
                                throw: async (error) => {
                                    queue.close()
                                    readOptsFuture = createFuture()
                                    throw error
                                },
                                [Symbol.asyncIterator]() {
                                    return this
                                },
                            }
                        },
                    }),
                )
            },
        })

        return target(opts)
    }
}
