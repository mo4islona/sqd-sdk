import type {Data, DataBatch, DataFork, DataCursorUtils} from '../data'
import type {DataDuplexFactory, DataStream} from '../core'
import {createSource, createTarget, stream} from '../core'
import {maybeLast} from '../../internal/misc'

interface BatchProcessingResult<TData extends Data> {
    buffer: TData[]
    batch?: DataBatch<TData, false>
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
            cursor: batch.cursor,
            data,
        },
    }
}

function createPartialBatch<TData extends Data>({
    batch,
    data,
    finalizedId,
    cursorUtils,
}: {
    batch: DataBatch<TData>
    data: TData[]
    finalizedId: TData['cursor']
    cursorUtils: DataCursorUtils<TData['cursor']>
}): BatchProcessingResult<TData> {
    const finalizeIndex = data.findIndex((item) => cursorUtils.compare(item.cursor, finalizedId).isGreater)
    const finalizedData = data.slice(0, finalizeIndex)

    return {
        buffer: data.slice(finalizeIndex),
        batch: {
            finalizedHead: batch.finalizedHead,
            head: batch.finalizedHead,
            cursor: maybeLast(finalizedData)?.cursor ?? finalizedId,
            data: finalizedData,
        },
    }
}

function handleBatch<TData extends Data>({
    batch,
    buffer,
    cursorUtils,
    finalizedId,
}: {
    batch: DataBatch<TData>
    buffer: TData[]
    cursorUtils: DataCursorUtils<TData['cursor']>
    finalizedId: TData['cursor'] | undefined
}): BatchProcessingResult<TData> {
    const mergedData = buffer.length > 0 ? [...buffer, ...batch.data] : batch.data
    const unfinalizedIndex = mergedData.findIndex(
        (item) => cursorUtils.compare(item.cursor, batch.finalizedHead).isGreater,
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

function handleFork<TData extends Data>({
    fork,
    buffer,
    finalizedId,
    cursorUtils,
}: {
    fork: DataFork<TData['cursor']>
    buffer: TData[]
    finalizedId: TData['cursor'] | undefined
    cursorUtils: DataCursorUtils<TData['cursor']>
}): {
    buffer: TData[]
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
                    cursorUtils: writeOptions.cursorUtils,
                    read: async function* (readOptions) {
                        let finalizedCursor: TData['cursor'] | undefined
                        let buffer: TData[] = []

                        for await (const message of writeOptions.read(readOptions)) {
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
                                        yield {
                                            ...result.batch,
                                            type: 'batch',
                                        }
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
                }),
            )
        },
    })
}
