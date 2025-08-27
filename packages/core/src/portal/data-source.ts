import {last, maybeLast} from '../internal/misc'
import {Throttler} from '../internal/throttler'
import {type DataMessage, createSource, type DataSource} from '../pipeline'
import {BlockRefUtils} from '../pipeline/block'
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

export type PortalData<TQuery extends Query> = GetBlock<TQuery>

export function portalDataSource<TQuery extends Query>(
    options: PortalDataSourceOptions<TQuery>,
): DataSource<BlockRef, PortalData<TQuery>, TQuery> {
    const portal = options.portal instanceof PortalClient ? options.portal : new PortalClient(options.portal)
    const headThrottler = new Throttler(async () => portal.getHead(), 5_000)

    const createBlockStream = async function* (
        offset?: BlockRef,
    ): AsyncIterable<DataMessage<BlockRef, PortalData<TQuery>>> {
        let parentBlockHash: string | undefined
        let fromBlock = options.query.fromBlock ?? 0
        if (offset) {
            fromBlock = Math.max(offset.number + 1, fromBlock)
            parentBlockHash = fromBlock === offset.number + 1 ? offset.hash : undefined
        }
        const toBlock = options.query.toBlock

        const streamQuery = {
            ...options.query,
            fromBlock,
            parentBlockHash,
            toBlock,
        }

        try {
            let lastCursor = offset

            for await (const batch of portal.getStream(streamQuery)) {
                const portalHead = await headThrottler.get()
                if (!portalHead) continue // no data?

                const data = batch.blocks.map((value) => ({
                    value,
                    cursor: {number: value.header.number, hash: value.header.hash},
                }))

                const cursor = maybeLast(data)?.cursor ?? lastCursor
                if (!cursor) continue

                const head = calculateHead(portalHead, cursor)
                const finalizedHead = batch.finalizedHead

                yield {
                    type: 'batch',
                    cursor: cursor,
                    head,
                    finalizedHead,
                    data,
                }

                lastCursor = cursor
            }
        } catch (err) {
            if (isForkException(err)) {
                yield {
                    type: 'fork',
                    cursors: err.lastBlocks,
                }
            }
            throw err
        }
    }

    return createSource({
        unfinalized: true,
        cursorUtils: BlockRefUtils,
        read: (opts) => createBlockStream(opts.cursor),
    })
}
