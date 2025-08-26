import {applyRangeBound, mergeRangeRequests, type Range} from '@sqd-sdk/core/internal/range/index'
import {BlockRefUtils, createSource, type ReadOptions, type BlockRef, type DataMessage} from '@sqd-sdk/core/pipeline'
import {createBlock, type Block, type FieldSelection, type RequiredFieldSelection} from './objects'
import {mergeDataRequests, type SolanaDataRequestRange} from './query'
import {type PortalClient, type PortalClientOptions, portalDataSource} from '@sqd-sdk/core/portal'
import type * as solana from '@sqd-sdk/core/portal/solana'

export interface SolanaPortalDataReaderOptions<F extends FieldSelection> {
    portal: PortalClientOptions | PortalClient
    fields: F
    request: SolanaDataRequestRange[]
    range?: Range
}

export function solanaPortalDataSource<F extends FieldSelection>(options: SolanaPortalDataReaderOptions<F>) {
    let requests = mergeRangeRequests(options.request, mergeDataRequests)
    if (options.range) {
        requests = applyRangeBound(requests, options.range)
    }

    const createBlockStream = async function* (
        cursor?: BlockRef,
        request?: SolanaDataRequestRange[],
    ): AsyncIterableIterator<DataMessage<BlockRef, Block<F>>> {
        const requestsBounded = cursor
            ? applyRangeBound(request ? mergeRangeRequests([...requests, ...request], mergeDataRequests) : requests, {
                  from: cursor.number + 1,
              })
            : requests

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
        read: (opts: ReadOptions<BlockRef, SolanaDataRequestRange[]>) => createBlockStream(opts.cursor, opts.request),
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
