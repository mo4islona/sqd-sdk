import {applyRangeBound, mergeRangeRequests, type Range} from '@belopash/core/internal/range'
import type {DataMessage, DataReadRequest} from '@belopash/core/pipeline'
import {createBlock, type Block, type FieldSelection} from './objects'
import {mergeDataRequests, type EvmDataRequestRange} from './query'
import {type PortalClient, type PortalClientOptions, portalDataSource} from '@belopash/core/portal'
import {BlockRefUtils, createSource, type BlockRef} from '@belopash/core/pipeline'
import type * as EVM from '@belopash/core/portal/evm'

export interface EvmPortalDataReaderOptions<F extends FieldSelection> {
    portal: PortalClientOptions | PortalClient
    fields: F
    query?: EvmDataRequestRange[]
    range?: Range
}

export type EvmData<F extends FieldSelection> = Block<F>[]

export function createEvmPortalSource<F extends FieldSelection>(options: EvmPortalDataReaderOptions<F>) {
    let baseQuery = mergeRangeRequests(options.query ?? [], mergeDataRequests)
    if (options.range) {
        baseQuery = applyRangeBound(baseQuery, options.range)
    }

    const createBlockStream = async function* (
        cursor?: BlockRef,
        query?: EvmDataRequestRange[],
    ): AsyncIterableIterator<DataMessage<BlockRef, EvmData<F>>> {
        const mergedQuery = query ? mergeRangeRequests([...baseQuery, ...query], mergeDataRequests) : baseQuery
        const requestsBounded = cursor ? applyRangeBound(mergedQuery, {from: cursor.number + 1}) : mergedQuery

        const fields = toPortalFieldSelection(options.fields)

        for (const request of requestsBounded) {
            const portalSource = portalDataSource({
                portal: options.portal,
                query: {
                    type: 'evm',
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
        read: (opts: DataReadRequest<BlockRef, EvmDataRequestRange[]>) => createBlockStream(opts.cursor, opts.query),
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
