import {applyRangeBound, mergeRangeRequests, type Range} from '@belopash/core/internal/range'
import {
    BlockRefUtils,
    createSource,
    type DataReadRequest,
    type BlockRef,
    type DataMessage,
} from '@belopash/core/pipeline'
import {createBlock, type Block, type FieldSelection, type RequiredFieldSelection} from './objects'
import {mergeDataRequests, type SolanaDataRequestRange} from './query'
import {type PortalClient, type PortalClientOptions, portalDataSource} from '@belopash/core/portal'
import type * as solana from '@belopash/core/portal/solana/query'

export interface SolanaPortalDataReaderOptions<F extends FieldSelection> {
    portal: PortalClientOptions | PortalClient
    fields: F
    query?: SolanaDataRequestRange[]
    range?: Range
}

export function solanaPortalDataSource<F extends FieldSelection>(options: SolanaPortalDataReaderOptions<F>) {
    let baseQuery = mergeRangeRequests(options.query ?? [], mergeDataRequests)
    if (options.range) {
        baseQuery = applyRangeBound(baseQuery, options.range)
    }

    const createBlockStream = async function* (
        cursor?: BlockRef,
        query?: SolanaDataRequestRange[],
    ): AsyncIterableIterator<DataMessage<BlockRef, Block<F>>> {
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

            for await (const message of portalSource.read({cursor})) {
                switch (message.type) {
                    case 'batch': {
                        yield {
                            type: 'batch',
                            data: message.data.map((i) => {
                                const value = createBlock<F>(i.value)

                                return {
                                    cursor: i.cursor,
                                    value,
                                }
                            }),
                            finalizedHead: message.finalizedHead,
                            head: message.head,
                            cursor: message.cursor,
                        }
                        break
                    }
                    case 'fork': {
                        yield message
                        break
                    }
                }
            }
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
