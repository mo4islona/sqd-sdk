import {last} from '../internal/misc'
import {Throttler} from '../internal/throttler'
import {type Data, DataRef, createSource, type DataSourceFactory, type DataMessage} from '../pipeline'
import {PortalClient, type BlockRef, type PortalClientOptions, isForkException} from './client'
import type {GetBlock, Query} from './query'

export interface PortalDataSourceOptions<TQuery extends Query> {
    portal: PortalClientOptions | PortalClient
    query: TQuery
}

export const BlockId = {
    fromBlock(block: {header: BlockRef}): BlockRef {
        return {number: block.header.number, hash: block.header.hash}
    },

    compare(a: BlockRef, b: BlockRef): DataRef.CompareResult {
        if (a.number < b.number) return DataRef.Less
        if (a.number > b.number) return DataRef.Greater
        if (a.hash !== b.hash) return DataRef.Fork
        return DataRef.Equal
    },
}

function calculateHead(portalHead: BlockRef, lastBlock: BlockRef | undefined): BlockRef {
    if (!lastBlock) return portalHead
    return BlockId.compare(lastBlock, portalHead).isGreater ? lastBlock : portalHead
}

export type PortalData<TQuery extends Query> = Data<GetBlock<TQuery>, BlockRef>

export function portalDataSource<TQuery extends Query>(
    options: PortalDataSourceOptions<TQuery>,
): DataSourceFactory<PortalData<TQuery>, true> {
    const portal = options.portal instanceof PortalClient ? options.portal : new PortalClient(options.portal)
    const headThrottler = new Throttler(async () => portal.getHead(), 5_000)

    const createDataStream = async function* (
        offset?: BlockRef,
    ): AsyncIterableIterator<DataMessage<PortalData<TQuery>, true>> {
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
                    id: BlockId.fromBlock(value),
                }))

                const offset = last(data).id
                const head = calculateHead(portalHead, offset)
                const finalizedHead = batch.finalizedHead

                yield {
                    type: 'batch',
                    value: {
                        offset,
                        head,
                        finalizedHead,
                        data,
                    },
                }
            }
        } catch (err) {
            if (isForkException(err)) {
                yield {
                    type: 'fork',
                    value: {heads: err.lastBlocks},
                }
            }
            throw err
        }
    }

    return createSource({
        unfinalized: true,
        ref: BlockId,
        read: (opts) => createDataStream(opts.offset),
    })
}
