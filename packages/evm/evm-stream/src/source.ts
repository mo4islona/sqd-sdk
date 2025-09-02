import type { DataTarget } from '@belopash/core'
import { type Range, applyRangeBound, mergeRangeRequests } from '@belopash/core/internal/range'
import type { BlockRef, DataMessage, DataReadRequest } from '@belopash/core/pipeline'
import { BlockRefUtils, createSource } from '@belopash/core/pipeline'
import { type PortalClient, type PortalClientOptions, portalDataSource } from '@belopash/core/portal'
import type * as EVM from '@belopash/core/portal/evm'
import { type Block, type FieldSelection, createBlock } from './objects'
import { type EvmDataRequestRange, mergeDataRequests } from './query'

export interface EvmPortalDataReaderOptions<F extends FieldSelection> {
    portal: PortalClientOptions | PortalClient
    fields: F
    query?: EvmDataRequestRange[]
    range?: Range
}

export type EvmData<F extends FieldSelection> = Block<F>[]

export function createEvmPortalSource<F extends FieldSelection>(
    options: EvmPortalDataReaderOptions<F>,
    ...pipeline: DataTarget<BlockRef, any, any, any>[]
) {
    let baseQuery = mergeRangeRequests(options.query ?? [], mergeDataRequests)
    if (options.range) {
        baseQuery = applyRangeBound(baseQuery, options.range)
    }

    const createBlockStream = async function* ({
        cursor,
        query,
    }: DataReadRequest<BlockRef, EvmDataRequestRange[]>): AsyncIterableIterator<DataMessage<BlockRef, EvmData<F>>> {
        const mergedQuery = query ? mergeRangeRequests([...baseQuery, ...query], mergeDataRequests) : baseQuery
        const requestsBounded = cursor ? applyRangeBound(mergedQuery, { from: cursor.number + 1 }) : mergedQuery

        const fields = toPortalFieldSelection(options.fields)

        for (const request of requestsBounded) {
            let portalSource = portalDataSource({
                portal: options.portal,
                query: {
                    type: 'evm',
                    fromBlock: request.range.from,
                    toBlock: request.range.to,
                    fields,
                    ...request.request,
                },
            })

            for (const pipe of pipeline || []) {
                portalSource = portalSource.pipe(pipe)
            }

            yield* portalSource.map((i) => i.map((i) => createBlock<F>(i))).read({ cursor })
        }
    }

    return createSource({
        cursorUtils: BlockRefUtils,
        read: (opts: DataReadRequest<BlockRef, EvmDataRequestRange[]>) => createBlockStream(opts),
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
