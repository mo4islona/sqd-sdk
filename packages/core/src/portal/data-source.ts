import {last} from '../internal/misc'
import {Throttler} from '../internal/throttler'
import {type Data, type DataBatch, DataReader, DataRef, DataSource, ForkException} from '../pipeline'
import {isForkException, PortalClient, type BlockRef, type PortalClientOptions} from './client'
import type {evm, solana, substrate} from './query'

export interface PortalDataSourceOptions {
    portal: PortalClientOptions | PortalClient
    query: evm.Query | solana.Query | substrate.Query
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

export function portalDataSource<T extends Data<any, BlockRef>>(options: PortalDataSourceOptions): DataSource<T, true> {
    const portal = options.portal instanceof PortalClient ? options.portal : new PortalClient(options.portal)
    const headThrottler = new Throttler(async () => portal.getHead(), 5_000)

    const createDataStream = async function* (offset?: T['id']): AsyncIterableIterator<DataBatch<T>> {
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

                // FIXME: how to type this?
                const data = batch.blocks.map((value) => ({value, id: BlockId.fromBlock(value)})) as T[]

                const offset = last(data).id
                const head = calculateHead(portalHead, offset)
                const finalizedHead = batch.finalizedHead

                yield {
                    data,
                    finalizedHead,
                    head,
                    offset,
                }
            }
        } catch (err) {
            if (isForkException(err)) {
                throw new ForkException<T['id']>({heads: err.lastBlocks})
            }
            throw err
        }
    }

    return new DataSource({
        unfinalized: true,
        reader: async (opts) => DataReader.fromAsync(createDataStream(opts.offset), BlockId),
    })
}
