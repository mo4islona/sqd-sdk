import {last} from '../internal/misc'
import {Throttler} from '../internal/throttler'
import type {DataSourceFactory, DataMessage, BlockSourceFactory, BlockMessage} from '../pipeline'
import {type BlockData, BlockRefUtils, createBlockSource} from '../pipeline/block'
import {PortalClient, type BlockRef, type PortalClientOptions, isForkException} from './client'
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
): BlockSourceFactory<PortalData<TQuery>, true, never> {
    const portal = options.portal instanceof PortalClient ? options.portal : new PortalClient(options.portal)
    const headThrottler = new Throttler(async () => portal.getHead(), 5_000)

    const createBlockStream = async function* (
        offset?: BlockRef,
    ): AsyncIterableIterator<BlockMessage<PortalData<TQuery>, true>> {
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
            for await (const batch of portal.getStream(streamQuery)) {
                const portalHead = await headThrottler.get()
                if (!portalHead) continue // no data?

                const data = batch.blocks.map((value) => ({
                    value,
                    cursor: {number: value.header.number, hash: value.header.hash},
                }))

                const cursor = last(data).cursor
                const head = calculateHead(portalHead, cursor)
                const finalizedHead = batch.finalizedHead

                yield {
                    type: 'batch',
                    cursor: cursor,
                    head,
                    finalizedHead,
                    data,
                }
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

    return createBlockSource({
        unfinalized: true,
        read: (opts) => createBlockStream(opts.cursor),
    })
}
