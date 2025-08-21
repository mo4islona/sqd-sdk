import type {Data, DataRef} from '../data'
import type {
    DataDuplex,
    DataDuplexFactory,
    DataFactoryOptions,
    DataReadOptions,
    DataMessage,
    DataWriteOptions,
    DataTarget,
} from '../core'
import {createSource, createTarget} from '../core'
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
) => Promise<DataTransformer<TInputData, TOutputData, TUnfinalized, TInputRequest, TOutputRequest>>

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
    return async (opts) => {
        if (typeof transformerOrFactory === 'function') {
            const transformer = await transformerOrFactory(opts)
            return createTransformer(transformer)(opts)
        }

        const queue = new SyncQueue<DataMessage<TOutputData, TUnfinalized>>()
        let readOptsFuture: Future<DataReadOptions<TOutputData, TOutputRequest>> | undefined = undefined

        const target = createTarget<TInputData, TUnfinalized, TInputRequest>({
            unfinalized: opts.unfinalized,
            write: async (writeOpts) => {
                if (!readOptsFuture) {
                    readOptsFuture = createFuture()
                }

                const readOpts = await readOptsFuture.promise()
                try {
                    for await (const message of transformerOrFactory.transform({
                        ...writeOpts,
                        ...readOpts,
                    })) {
                        await queue.put(message)
                    }
                } finally {
                    queue.close()
                }
            },
        })

        const source = createSource<TOutputData, TUnfinalized, TOutputRequest>({
            unfinalized: transformerOrFactory.unfinalized,
            ref: transformerOrFactory.ref,
            read: (readOpts) => {
                if (!readOptsFuture) {
                    readOptsFuture = createFuture()
                }

                readOptsFuture.resolve(readOpts)

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
                        readOptsFuture = undefined
                        return {done: true, value: undefined}
                    },
                    throw: async (error) => {
                        queue.close()
                        readOptsFuture = undefined
                        throw error
                    },
                    [Symbol.asyncIterator]() {
                        return this
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
