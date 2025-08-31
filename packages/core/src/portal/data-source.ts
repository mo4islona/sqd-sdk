import {maybeLast} from '../internal/misc'
import {Throttler} from '../internal/throttler'
import {type DataMessage, createSource, type DataSource, type DataItem, BlockRefUtils} from '../pipeline'
import {type BlockRef, PortalClient, type PortalClientOptions, isForkException} from './client'
import type {GetBlock, Query} from './query'

export interface PortalDataSourceOptions<TQuery extends Query> {
    portal: PortalClientOptions | PortalClient
    query: TQuery
}

function calculateHead(portalHead: BlockRef, lastBlock: BlockRef | undefined): BlockRef {
    if (!lastBlock) return portalHead
    return BlockRefUtils.compare(lastBlock, portalHead).isGreater ? lastBlock : portalHead
}

function createBlockCursor(block: {header: {number: number; hash: string}}): BlockRef {
    return {
        number: block.header.number,
        hash: block.header.hash,
    }
}

function findUnfinalizedBlockIndex(
    blocks: {header: {number: number; hash: string}}[],
    finalizedHead: BlockRef,
): number {
    const index = blocks.findIndex((block) => BlockRefUtils.compare(createBlockCursor(block), finalizedHead).isGreater)
    return index < 0 ? blocks.length : index
}

function* processUnfinalizedBlocks<TQuery extends Query>(
    blocks: GetBlock<TQuery>[],
    unfinalizedStartIndex: number,
): IterableIterator<DataItem<BlockRef, PortalData<TQuery>>> {
    for (let i = unfinalizedStartIndex; i < blocks.length; i++) {
        const block = blocks[i]
        const cursor = createBlockCursor(block)
        yield {value: [block], cursor}
    }
}

function processBlockBatch<TQuery extends Query>(
    blocks: GetBlock<TQuery>[],
    finalizedHead?: BlockRef,
): DataItem<BlockRef, PortalData<TQuery>>[] {
    if (blocks.length === 0) {
        return []
    }

    if (!finalizedHead) {
        return blocks.map((block) => {
            const cursor = createBlockCursor(block)
            return {value: [block], cursor}
        })
    }

    const unfinalizedStartIndex = findUnfinalizedBlockIndex(blocks, finalizedHead)
    const dataItems: DataItem<BlockRef, PortalData<TQuery>>[] = []

    if (unfinalizedStartIndex > 0) {
        const finalizedBlocks =
            unfinalizedStartIndex === blocks.length ? blocks : blocks.slice(0, unfinalizedStartIndex)
        const lastFinalizedBlock = finalizedBlocks[finalizedBlocks.length - 1]
        const finalizedCursor = createBlockCursor(lastFinalizedBlock)
        dataItems.push({value: finalizedBlocks, cursor: finalizedCursor})
    }

    const unfinalizedItems = processUnfinalizedBlocks(blocks, unfinalizedStartIndex)
    dataItems.push(...unfinalizedItems)

    return dataItems
}

export type PortalData<TQuery extends Query> = GetBlock<TQuery>[]

export function portalDataSource<TQuery extends Query>(
    options: PortalDataSourceOptions<TQuery>,
): DataSource<BlockRef, PortalData<TQuery>, TQuery> {
    const portal = options.portal instanceof PortalClient ? options.portal : new PortalClient(options.portal)
    const headThrottler = new Throttler(async () => portal.getHead(), 5_000)

    const createBlockStream = async function* (
        resumeFromCursor?: BlockRef,
    ): AsyncIterable<DataMessage<BlockRef, PortalData<TQuery>>> {
        let parentBlockHash: string | undefined
        let startBlockNumber = options.query.fromBlock ?? 0

        if (resumeFromCursor) {
            startBlockNumber = Math.max(resumeFromCursor.number + 1, startBlockNumber)
            parentBlockHash = startBlockNumber === resumeFromCursor.number + 1 ? resumeFromCursor.hash : undefined
        }

        const endBlockNumber = options.query.toBlock

        const streamQuery = {
            ...options.query,
            fromBlock: startBlockNumber,
            parentBlockHash,
            toBlock: endBlockNumber,
        }

        try {
            let lastProcessedCursor = resumeFromCursor

            for await (const blockBatch of portal.getStream(streamQuery)) {
                const currentPortalHead = await headThrottler.get()
                if (!currentPortalHead) continue

                const processedDataItems = processBlockBatch(blockBatch.blocks, blockBatch.finalizedHead)

                const effectiveHead = calculateHead(currentPortalHead, maybeLast(processedDataItems)?.cursor)
                const batchFinalizedHead = blockBatch.finalizedHead

                yield {
                    type: 'data',
                    head: effectiveHead,
                    finalizedHead: batchFinalizedHead,
                    data: processedDataItems,
                }

                lastProcessedCursor = maybeLast(processedDataItems)?.cursor ?? lastProcessedCursor
            }
        } catch (error) {
            if (isForkException(error)) {
                yield {
                    type: 'fork',
                    cursors: error.lastBlocks,
                }
            }
            throw error
        }
    }

    return createSource({
        unfinalized: true,
        cursorUtils: BlockRefUtils,
        read: (readOptions) => createBlockStream(readOptions.cursor),
    })
}
