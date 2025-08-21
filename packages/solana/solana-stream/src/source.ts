import {applyRangeBound, mergeRangeRequests} from '@sqd-sdk/core/internal/range/index'
import {type Data, createSource, type DataSourceFactory, type DataMessage, handleMessage} from '@sqd-sdk/core/pipeline'
import {
    type Block,
    blockFromPartial,
    type BlockPartial,
    type FieldSelection,
    type RequiredFieldSelection,
    REQUIRED_FIELDS,
} from './objects'
import {setUpRelations} from './objects/relations'
import {mergeDataRequests, type SolanaQueryOptions} from './query'
import {
    BlockId,
    type BlockRef,
    type PortalClient,
    type PortalClientOptions,
    portalDataSource,
} from '@sqd-sdk/core/portal'
import {type MergeSelection, mergeSelection} from '@sqd-sdk/core/internal/selection'

type GetFields<F extends FieldSelection> = MergeSelection<RequiredFieldSelection, F>

export interface SolanaPortalDataReaderOptions<Q extends SolanaQueryOptions> {
    portal: PortalClientOptions | PortalClient
    query: Q
}

export type SolanaPortalData<Q extends SolanaQueryOptions> = Data<Block<GetFields<Q['fields']>>, BlockRef>

export function solanaPortalDataSource<Q extends SolanaQueryOptions>(
    options: SolanaPortalDataReaderOptions<Q>,
): DataSourceFactory<SolanaPortalData<Q>, true> {
    const fields = getFields(options.query.fields)
    const requests = mergeRangeRequests(options.query.requests, mergeDataRequests)

    const createDataStream = async function* (
        offset?: BlockRef,
    ): AsyncIterableIterator<DataMessage<SolanaPortalData<Q>, true>> {
        const requestsBounded = offset ? applyRangeBound(requests, {from: offset.number + 1}) : requests

        for (const request of requestsBounded) {
            const portalSource = await portalDataSource({
                portal: options.portal,
                query: {
                    type: 'solana' as const,
                    fromBlock: request.range.from,
                    toBlock: request.range.to,
                    fields,
                    ...request.request,
                },
            })()

            for await (const message of portalSource.read({offset})) {
                yield handleMessage(message, {
                    batch: (batch) => {
                        return {
                            type: 'batch' as const,
                            value: {
                                data: batch.data.map(
                                    (i): SolanaPortalData<Q> => ({
                                        value: mapBlock(i.value, fields),
                                        id: i.id,
                                    }),
                                ),
                                finalizedHead: batch.finalizedHead,
                                head: batch.head,
                                offset: batch.offset,
                            },
                        }
                    },
                    fork: (fork) => {
                        return {
                            type: 'fork' as const,
                            value: {heads: fork.heads},
                        }
                    },
                })
            }
        }
    }

    return createSource({
        unfinalized: true,
        ref: BlockId,
        read: (opts) => createDataStream(opts.offset),
    })
}

export function mapBlock<F extends RequiredFieldSelection>(
    rawBlock: unknown,
    fields: RequiredFieldSelection,
): Block<F> {
    const block = blockFromPartial(rawBlock as BlockPartial<F>)
    setUpRelations(block as any)

    return block
}

function getFields<T extends FieldSelection>(fields: T): GetFields<T> {
    return mergeSelection(REQUIRED_FIELDS, fields)
}
