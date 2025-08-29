import type {DataCursorUtils} from '../cursor'
import type {DataBatchMessage, DataBatchItem, DataForkMessage, DataDuplex} from '../core'
import {maybeLast} from '../../internal/misc'
import {createTransformer} from './transformer'

interface BatchProcessingResult<TCursor, TValue> {
    buffer: DataBatchItem<TCursor, TValue>[]
    batch?: DataBatchMessage<TCursor, TValue>
}

function findRollbackIndex<TId>(currentChain: TId[], forkChain: TId[], cursorUtils: DataCursorUtils<TId>): number {
    let currentIndex = 0
    let forkIndex = 0
    let lastCommonIndex = -1

    while (currentIndex < currentChain.length && forkIndex < forkChain.length) {
        const currentBlock = currentChain[currentIndex]
        const forkBlock = forkChain[forkIndex]
        const cmp = cursorUtils.compare(forkBlock, currentBlock)

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

function createFullyFinalizedBatch<TCursor, TValue>({
    batch,
    data,
}: {
    batch: DataBatchMessage<TCursor, TValue>
    data: DataBatchItem<TCursor, TValue>[]
}): BatchProcessingResult<TCursor, TValue> {
    return {
        buffer: [],
        batch: {
            type: 'batch',
            finalizedHead: batch.finalizedHead,
            head: batch.finalizedHead!,
            cursor: batch.cursor,
            data,
        },
    }
}

function createPartialBatch<TCursor, TValue>({
    batch,
    data,
    finalizedId,
    cursorUtils,
}: {
    batch: DataBatchMessage<TCursor, TValue>
    data: DataBatchItem<TCursor, TValue>[]
    finalizedId: TCursor
    cursorUtils: DataCursorUtils<TCursor>
}): BatchProcessingResult<TCursor, TValue> {
    const finalizeIndex = data.findIndex((item) => cursorUtils.compare(item.cursor, finalizedId).isGreater)
    const finalizedData = data.slice(0, finalizeIndex)

    return {
        buffer: data.slice(finalizeIndex),
        batch: {
            type: 'batch' as const,
            finalizedHead: batch.finalizedHead,
            head: batch.finalizedHead!,
            cursor: maybeLast(finalizedData)?.cursor ?? finalizedId,
            data: finalizedData,
        },
    }
}

function handleBatch<TCursor, TValue>({
    batch,
    buffer,
    cursorUtils,
    finalizedId,
}: {
    batch: DataBatchMessage<TCursor, TValue>
    buffer: DataBatchItem<TCursor, TValue>[]
    cursorUtils: DataCursorUtils<TCursor>
    finalizedId: TCursor | undefined
}): BatchProcessingResult<TCursor, TValue> {
    const mergedData = buffer.length > 0 ? [...buffer, ...batch.data] : batch.data
    const unfinalizedIndex = mergedData.findIndex(
        (item) => cursorUtils.compare(item.cursor, batch.finalizedHead!).isGreater,
    )

    if (unfinalizedIndex < 0) {
        return createFullyFinalizedBatch({
            batch,
            data: mergedData,
        })
    }

    const newFinalizedId = batch.finalizedHead ?? mergedData[unfinalizedIndex - 1]?.cursor ?? finalizedId
    if (!newFinalizedId) {
        return {
            buffer: mergedData,
        }
    }

    return createPartialBatch({
        batch,
        data: mergedData,
        finalizedId: newFinalizedId,
        cursorUtils,
    })
}

function handleFork<TCursor, TValue>({
    fork,
    buffer,
    finalizedId,
    cursorUtils,
}: {
    fork: DataForkMessage<TCursor>
    buffer: DataBatchItem<TCursor, TValue>[]
    finalizedId: TCursor | undefined
    cursorUtils: DataCursorUtils<TCursor>
}): {
    buffer: DataBatchItem<TCursor, TValue>[]
} {
    const unfinalizedChain = buffer.map((item) => item.cursor)
    const currentChain = finalizedId ? [finalizedId, ...unfinalizedChain] : unfinalizedChain
    const rollbackIndex = findRollbackIndex(currentChain, fork.cursors, cursorUtils)

    if (rollbackIndex < 0) {
        // FIXME: add better error message
        throw new Error('Unable to process fork')
    }

    return {
        buffer: buffer.slice(finalizedId ? 1 : 0, rollbackIndex + 1),
    }
}

export function createFinalizer<TCursor, TValue, TQuery>(): DataDuplex<
    TCursor,
    TCursor,
    TValue,
    TValue,
    TQuery,
    TQuery
> {
    return createTransformer<TCursor, TCursor, TValue, TValue, TQuery, TQuery>({
        transform: (writeOptions) => {
            return {
                unfinalized: false,
                cursorUtils: writeOptions.cursorUtils,
                read: async function* (DataReadRequest) {
                    let finalizedCursor: TCursor | undefined
                    let buffer: DataBatchItem<TCursor, TValue>[] = []

                    for await (const message of writeOptions.read(DataReadRequest)) {
                        switch (message.type) {
                            case 'batch': {
                                const result = handleBatch({
                                    batch: message,
                                    buffer,
                                    cursorUtils: writeOptions.cursorUtils,
                                    finalizedId: finalizedCursor,
                                })
                                buffer = result.buffer
                                finalizedCursor = result.batch?.cursor

                                if (result.batch) {
                                    yield result.batch
                                }
                                break
                            }
                            case 'fork': {
                                const result = handleFork({
                                    fork: message,
                                    buffer,
                                    finalizedId: finalizedCursor,
                                    cursorUtils: writeOptions.cursorUtils,
                                })
                                buffer = result.buffer
                                break
                            }
                        }
                    }
                },
            }
        },
    })
}
