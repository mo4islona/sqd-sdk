import {type Range, applyRangeBound, mergeRangeRequests} from '@belopash/core/internal/range'
import {
    type BlockRef,
    BlockRefUtils,
    type DataMessage,
    type DataReadRequest,
    createSource,
} from '@belopash/core/pipeline'
import {type PortalClient, type PortalClientOptions, portalDataSource} from '@belopash/core/portal'
import type * as solana from '@belopash/core/portal/solana/query'
import {type Block, type FieldSelection, type RequiredFieldSelection, createBlock} from './objects'
import {type SolanaDataRequestRange, mergeDataRequests} from './query'

export interface SolanaPortalDataReaderOptions<F extends FieldSelection> {
    portal: PortalClientOptions | PortalClient
    fields: F
    query?: SolanaDataRequestRange[]
    range?: Range
}

export type SolanaData<F extends FieldSelection> = Block<F>[]

export function createSolanaPortalSource<F extends FieldSelection>(options: SolanaPortalDataReaderOptions<F>) {
    let baseQuery = mergeRangeRequests(options.query ?? [], mergeDataRequests)
    if (options.range) {
        baseQuery = applyRangeBound(baseQuery, options.range)
    }

    const createBlockStream = async function* (
        cursor?: BlockRef,
        query?: SolanaDataRequestRange[],
    ): AsyncIterableIterator<DataMessage<BlockRef, SolanaData<F>>> {
        const requestsBounded = cursor
            ? applyRangeBound(query ? mergeRangeRequests([...baseQuery, ...query], mergeDataRequests) : baseQuery, {
                  from: cursor.number + 1,
              })
            : baseQuery

        const fields = toPortalFieldSelection(options.fields)

        for (const request of requestsBounded) {
            const portalSource = portalDataSource({
                portal: options.portal,
                query: {
                    type: 'solana' as const,
                    fromBlock: request.range.from,
                    toBlock: request.range.to,
                    fields,
                    ...request.request,
                },
            })

            yield* portalSource.map((i) => i.map((i) => createBlock<F>(i))).read({cursor})
        }
    }

    return createSource({
        unfinalized: true,
        cursorUtils: BlockRefUtils,
        read: (opts: DataReadRequest<BlockRef, SolanaDataRequestRange[]>) => createBlockStream(opts.cursor, opts.query),
    })
}

function toPortalFieldSelection<T extends FieldSelection>(fields: T) {
    return {
        block: {
            ...fields.block,
            number: true,
            hash: true,
        },
        transaction: {
            ...fields.transaction,
            transactionIndex: true,
        },
        log: {
            ...fields.log,
            transactionIndex: true,
            logIndex: true,
            instructionAddress: true,
        },
        instruction: {
            ...fields.instruction,
            transactionIndex: true,
            instructionAddress: true,
        },
        balance: {
            ...fields.balance,
            account: true,
            transactionIndex: true,
        },
        tokenBalance: {
            ...fields.tokenBalance,
            account: true,
            transactionIndex: true,
        },
        reward: {
            ...fields.reward,
            pubkey: true,
        },
    } satisfies solana.FieldSelection
}
