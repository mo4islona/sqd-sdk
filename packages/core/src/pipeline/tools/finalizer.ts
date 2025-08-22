import type {Data, DataRef, DataBatch, DataFork} from '../data'
import type {DataDuplexFactory, DataStream} from '../core'
import {createSource, createTarget, stream} from '../core'
import {maybeLast} from '../../internal/misc'

interface BatchProcessingResult<TData extends Data> {
    buffer: TData[]
    batch?: DataBatch<TData>
}

function findRollbackIndex<TId>(currentChain: TId[], forkChain: TId[], ref: DataRef<TId>): number {
    let currentIndex = 0
    let forkIndex = 0
    let lastCommonIndex = -1

    while (currentIndex < currentChain.length && forkIndex < forkChain.length) {
        const currentBlock = currentChain[currentIndex]
        const forkBlock = forkChain[forkIndex]
        const cmp = ref.compare(forkBlock, currentBlock)

        if (cmp.isFork) {
            return lastCommonIndex
        }

        if (cmp.isLess) {
            forkIndex++
            continue
        }

        if (cmp.isGreater) {
            currentIndex++
            continue
        }

        lastCommonIndex = currentIndex
        currentIndex++
        forkIndex++
    }

    return lastCommonIndex
}

function createFullyFinalizedBatch<TData extends Data>({
    batch,
    data,
}: {
    batch: DataBatch<TData>
    data: TData[]
}): BatchProcessingResult<TData> {
    return {
        buffer: [],
        batch: {
            finalizedHead: batch.finalizedHead,
            head: batch.finalizedHead,
            offset: batch.offset,
            data,
        },
    }
}

function createPartialBatch<TData extends Data>({
    batch,
    data,
    finalizedId,
    ref,
}: {
    batch: DataBatch<TData>
    data: TData[]
    finalizedId: TData['id']
    ref: DataRef<TData['id']>
}): BatchProcessingResult<TData> {
    const finalizeIndex = data.findIndex((item) => ref.compare(item.id, finalizedId).isGreater)
    const finalizedData = data.slice(0, finalizeIndex)

    return {
        buffer: data.slice(finalizeIndex),
        batch: {
            finalizedHead: batch.finalizedHead,
            head: batch.finalizedHead,
            offset: maybeLast(finalizedData)?.id ?? finalizedId,
            data: finalizedData,
        },
    }
}

function handleBatch<TData extends Data>({
    batch,
    buffer,
    ref,
    finalizedId,
}: {
    batch: DataBatch<TData>
    buffer: TData[]
    ref: DataRef<TData['id']>
    finalizedId: TData['id'] | undefined
}): BatchProcessingResult<TData> {
    const mergedData = buffer.length > 0 ? [...buffer, ...batch.data] : batch.data
    const unfinalizedIndex = mergedData.findIndex((item) => ref.compare(item.id, batch.finalizedHead).isGreater)

    if (unfinalizedIndex < 0) {
        return createFullyFinalizedBatch({
            batch,
            data: mergedData,
        })
    }

    const newFinalizedId = batch.finalizedHead ?? mergedData[unfinalizedIndex - 1]?.id ?? finalizedId
    if (!newFinalizedId) {
        return {
            buffer: mergedData,
        }
    }

    return createPartialBatch({
        batch,
        data: mergedData,
        finalizedId: newFinalizedId,
        ref,
    })
}

function handleFork<TData extends Data>({
    fork,
    buffer,
    finalizedId,
    ref,
}: {
    fork: DataFork<TData['id']>
    buffer: TData[]
    finalizedId: TData['id'] | undefined
    ref: DataRef<TData['id']>
}): {
    buffer: TData[]
} {
    const unfinalizedChain = buffer.map((item) => item.id)
    const currentChain = finalizedId ? [finalizedId, ...unfinalizedChain] : unfinalizedChain
    const rollbackIndex = findRollbackIndex(currentChain, fork.heads, ref)

    if (rollbackIndex < 0) {
        // FIXME: add better error message
        throw new Error('Unable to process fork')
    }

    return {
        buffer: buffer.slice(finalizedId ? 1 : 0, rollbackIndex + 1),
    }
}

export function createFinalizer<TData extends Data, TRequest>(): DataDuplexFactory<
    TData,
    TData,
    true,
    false,
    TRequest,
    TRequest
> {
    return createTarget<TData, true, TRequest, DataStream<TData, false, TRequest>>({
        unfinalized: true,
        write: (writeOptions) => {
            return stream(
                createSource({
                    unfinalized: false,
                    ref: writeOptions.ref,
                    read: async function* (readOptions) {
                        let finalizedId: TData['id'] | undefined
                        let buffer: TData[] = []

                        for await (const message of writeOptions.read(readOptions)) {
                            switch (message.type) {
                                case 'batch': {
                                    const result = handleBatch({
                                        batch: message.value,
                                        buffer,
                                        ref: writeOptions.ref,
                                        finalizedId,
                                    })
                                    buffer = result.buffer
                                    finalizedId = result.batch?.offset

                                    if (result.batch) {
                                        yield {
                                            type: 'batch',
                                            value: result.batch,
                                        }
                                    }
                                    break
                                }
                                case 'fork': {
                                    const result = handleFork({
                                        fork: message.value,
                                        buffer,
                                        finalizedId,
                                        ref: writeOptions.ref,
                                    })
                                    buffer = result.buffer
                                    break
                                }
                            }
                        }
                    },
                }),
            )
        },
    })
}
