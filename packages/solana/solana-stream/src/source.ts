import {applyRangeBound, mergeRangeRequests} from '@sqd-sdk/core/internal/range/index'
import {type DataBatch, type Data, DataSource, DataReader} from '@sqd-sdk/core/pipeline'
import {cast} from '@sqd-sdk/core/validation'
import {
    type Block,
    blockFromPartial,
    type BlockPartial,
    type FieldSelection,
    type RequiredFieldSelection,
    REQUIRED_FIELDS,
} from './objects'
import {getDataSchema} from './schema'
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
    options: SolanaPortalDataReaderOptions<Q>
): DataSource<SolanaPortalData<Q>, true> {
    const fields = getFields(options.query.fields)
    const requests = mergeRangeRequests(options.query.requests, mergeDataRequests)

    const createDataStream = async function* (
        offset?: BlockRef
    ): AsyncIterableIterator<DataBatch<SolanaPortalData<Q>>> {
        const requestsBounded = offset ? applyRangeBound(requests, {from: offset.number + 1}) : requests

        for (const request of requestsBounded) {
            for await (const data of portalDataSource({
                portal: options.portal,
                query: {
                    type: 'solana' as const,
                    fromBlock: request.range.from,
                    toBlock: request.range.to,
                    fields,
                    ...request.request,
                },
            }).read({offset})) {
                yield data as DataBatch<SolanaPortalData<Q>>
            }

            return
        }
    }

    return new DataSource({
        unfinalized: true,
        ref: BlockId,
        reader: async (opts) => createDataStream(opts.offset),
    })
}

export function mapBlock<F extends RequiredFieldSelection>(
    rawBlock: unknown,
    fields: RequiredFieldSelection
): Block<F> {
    const validator = getDataSchema(fields)
    const partial = cast(validator, rawBlock) as BlockPartial<F>
    const block = blockFromPartial(partial)
    setUpRelations(block as any)

    return block
}

function getFields<T extends FieldSelection>(fields: T): GetFields<T> {
    return mergeSelection(REQUIRED_FIELDS, fields)
}
