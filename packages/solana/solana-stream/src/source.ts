import {applyRangeBound, mergeRangeRequests, type Range} from '@sqd-sdk/core/internal/range/index'
import {type Data, createSource, type DataSourceFactory, type DataMessage} from '@sqd-sdk/core/pipeline'
import {
    type Block,
    blockFromPartial,
    type FieldSelection,
    type RequiredFieldSelection,
    REQUIRED_FIELDS,
} from './objects'
import {mergeDataRequests, type SolanaDataRequestRange} from './query'
import {
    BlockCursorUtils,
    type BlockRef,
    type PortalClient,
    type PortalClientOptions,
    portalDataSource,
} from '@sqd-sdk/core/portal'
import {type MergeSelection, mergeSelection} from '@sqd-sdk/core/internal/selection'

type GetFields<F extends FieldSelection> = MergeSelection<RequiredFieldSelection, F>

export interface SolanaPortalDataReaderOptions<F extends FieldSelection> {
    portal: PortalClientOptions | PortalClient
    fields: F
    request: SolanaDataRequestRange[]
    range?: Range
}

export type SolanaPortalData<F extends FieldSelection> = Data<Block<GetFields<F>>, BlockRef>

export function solanaPortalDataSource<F extends FieldSelection>(
    options: SolanaPortalDataReaderOptions<F>,
): DataSourceFactory<SolanaPortalData<F>, true, SolanaDataRequestRange[]> {
    const fields = getFields(options.fields)
    let requests = mergeRangeRequests(options.request, mergeDataRequests)
    if (options.range) {
        requests = applyRangeBound(requests, options.range)
    }

    const createDataStream = async function* (
        cursor?: BlockRef,
        request?: SolanaDataRequestRange[],
    ): AsyncIterableIterator<DataMessage<SolanaPortalData<F>, true>> {
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
                            data: message.data.map((i): SolanaPortalData<F> => {
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

    return createSource({
        unfinalized: true,
        cursorUtils: BlockCursorUtils,
        read: (opts) => createDataStream(opts.cursor, opts.request),
    })
}

function getFields<T extends FieldSelection>(fields: T): GetFields<T> {
    return mergeSelection(REQUIRED_FIELDS, fields)
}
