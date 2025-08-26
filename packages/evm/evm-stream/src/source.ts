import {applyRangeBound, mergeRangeRequests, type Range} from '@sqd-sdk/core/internal/range/index'
import type {BlockSourceFactory, BlockMessage} from '@sqd-sdk/core/pipeline'
import {createBlock, type Block, type FieldSelection} from './objects'
import {mergeDataRequests, type EvmDataRequestRange} from './query'
import {type PortalClient, type PortalClientOptions, portalDataSource} from '@sqd-sdk/core/portal'
import {createBlockSource, type BlockRef} from '@sqd-sdk/core/pipeline'
import type * as EVM from '@sqd-sdk/core/portal/evm'

export interface EvmPortalDataReaderOptions<F extends FieldSelection> {
    portal: PortalClientOptions | PortalClient
    fields: F
    request: EvmDataRequestRange[]
    range?: Range
}

export function evmPortalDataSource<F extends FieldSelection>(
    options: EvmPortalDataReaderOptions<F>,
): BlockSourceFactory<Block<F>, true, EvmDataRequestRange[]> {
    let requests = mergeRangeRequests(options.request, mergeDataRequests)
    if (options.range) {
        requests = applyRangeBound(requests, options.range)
    }

    const createBlockStream = async function* (
        cursor?: BlockRef,
        request?: EvmDataRequestRange[],
    ): AsyncIterableIterator<BlockMessage<Block<F>, true>> {
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
                    type: 'evm' as const,
                    fromBlock: request.range.from,
                    toBlock: request.range.to,
                    fields,
                    ...request.request,
                },
            })()

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

    return createBlockSource({
        unfinalized: true,
        read: (opts) => createBlockStream(opts.cursor, opts.request),
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
        },
        trace: {
            ...fields.trace,
            transactionIndex: true,
            traceAddress: true,
            type: true,
        },
        stateDiff: {
            ...fields.stateDiff,
            transactionIndex: true,
            address: true,
            key: true,
        },
    } satisfies EVM.FieldSelection
}
