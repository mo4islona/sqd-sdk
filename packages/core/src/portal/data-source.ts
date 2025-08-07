import {assert, last} from '../internal/misc'
import {Throttler} from '../internal/throttler'
import {type Data, type DataBatch, DataRef, DataSource, ForkException, source} from '../pipeline'
import {
    isForkException,
    PortalClient,
    type PortalQuery,
    type BlockRef as BlockRef_,
    type PortalClientOptions,
} from './client'

export interface PortalDataSourceOptions {
    portal: PortalClientOptions | PortalClient
    query: PortalQuery
}

export class BlockId implements DataRef<BlockRef_> {
    static fromBlock(block: {header: BlockRef_}): BlockId {
        return new BlockId(block.header)
    }

    constructor(readonly value: BlockRef_) {}

    compare(other: BlockId): DataRef.CompareResult {
        if (this.value.number < other.value.number) return DataRef.Less
        if (this.value.number > other.value.number) return DataRef.Greater
        if (this.value.hash !== other.value.hash) return DataRef.Fork
        return DataRef.Equal
    }
}

function calculateHead(portalHead: BlockId, lastBlock: BlockId | undefined): BlockId {
    if (!lastBlock) return portalHead
    return lastBlock.compare(portalHead).isGreater ? lastBlock : portalHead
}

export function portalDataSource<T extends Data<any, BlockRef_>>(
    options: PortalDataSourceOptions
): DataSource<T, true> {
    const portal = options.portal instanceof PortalClient ? options.portal : new PortalClient(options.portal)
    const headThrottler = new Throttler(async () => portal.getHead(), 5_000)

    const createDataStream = async function* (offset?: BlockId): AsyncIterableIterator<DataBatch<T>> {
        let parentBlockHash: string | undefined
        let fromBlock = options.query.fromBlock ?? 0
        if (offset) {
            fromBlock = Math.max(offset.value.number + 1, fromBlock)
            parentBlockHash = fromBlock === offset.value.number + 1 ? offset.value.hash : undefined
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
                    ref: BlockId.fromBlock(value),
                })) as T[]

                const offset = last(data).ref
                const head = calculateHead(new BlockId(portalHead), offset)
                const finalizedHead = batch.finalizedHead ? new BlockId(batch.finalizedHead) : undefined

                yield {
                    data,
                    finalizedHead,
                    head,
                    offset,
                }
            }
        } catch (err) {
            if (isForkException(err)) {
                throw new ForkException<T>({
                    heads: err.lastBlocks.map((b) => new BlockId(b)),
                })
            }
            throw err
        }
    }

    return new DataSource({
        unfinalized: true,
        reader: async (opts) => {
            const stream = createDataStream(opts.offset)

            return {
                read: async () => {
                    const batch = await stream.next()
                    return batch.done ? undefined : batch.value
                },
                close: async () => stream.return?.(),
            }
        },
    })
}
