import {applyRangeBound, mergeRangeRequests, type Range} from '@sqd-sdk/core/internal/range/index'
import type {DataSourceFactory, DataMessage, BlockData, BlockSourceFactory, BlockMessage} from '@sqd-sdk/core/pipeline'
import {
    type Block,
    blockFromPartial,
    type FieldSelection,
    type RequiredFieldSelection,
    REQUIRED_FIELDS,
} from './objects'
import {mergeDataRequests, type SolanaDataRequestRange} from './query'
import {type PortalClient, type PortalClientOptions, portalDataSource} from '@sqd-sdk/core/portal'
import {type MergeSelection, mergeSelection} from '@sqd-sdk/core/internal/selection'
import {createBlockSource, type BlockRef} from '@sqd-sdk/core/pipeline'

type GetFields<F extends FieldSelection> = MergeSelection<RequiredFieldSelection, F>

export interface SolanaPortalDataReaderOptions<F extends FieldSelection> {
    portal: PortalClientOptions | PortalClient
    fields: F
    request: SolanaDataRequestRange[]
    range?: Range
}

export type SolanaPortalData<F extends FieldSelection> = Block<GetFields<F>>

export function solanaPortalDataSource<F extends FieldSelection>(
    options: SolanaPortalDataReaderOptions<F>,
): BlockSourceFactory<SolanaPortalData<F>, true, SolanaDataRequestRange[]> {
    const fields = getFields(options.fields)
    let requests = mergeRangeRequests(options.request, mergeDataRequests)
    if (options.range) {
        requests = applyRangeBound(requests, options.range)
    }

    const createBlockStream = async function* (
        cursor?: BlockRef,
        request?: SolanaDataRequestRange[],
    ): AsyncIterableIterator<BlockMessage<SolanaPortalData<F>, true>> {
        const requestsBounded = cursor
            ? applyRangeBound(request ? mergeRangeRequests([...requests, ...request], mergeDataRequests) : requests, {
                  from: cursor.number + 1,
              })
            : requests

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
            })()

            for await (const message of portalSource.read({cursor})) {
                switch (message.type) {
                    case 'batch': {
                        yield {
                            type: 'batch',
                            data: message.data.map((i) => {
                                const value = blockFromPartial<GetFields<F>>(i.value as any)

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

function getFields<T extends FieldSelection>(fields: T): GetFields<T> {
    return mergeSelection(REQUIRED_FIELDS, fields)
}
