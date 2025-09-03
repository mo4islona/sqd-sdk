import type { DataSource, DataTarget } from '@belopash/core'
import type { BlockRef, DataMessage, DataReadRequest } from '@belopash/core/pipeline'
import { BlockRefUtils, createSource } from '@belopash/core/pipeline'
import type { PortalData } from '@belopash/core/portal'
import { type PortalClientOptions, portalDataSource } from '@belopash/core/portal'
import type { evm } from '@belopash/core/portal/query'
import { type Block, REQUIRED_FIELDS, type RequiredFieldSelection, createBlock } from './objects'
import { EvmQueryBuilder } from './query'

type Middleware = DataTarget<
    BlockRef,
    PortalData<evm.Query>,
    evm.Query,
    DataSource<BlockRef, PortalData<evm.Query>, evm.Query>
>

export interface EvmPortalDataReaderOptions {
    portal: PortalClientOptions | string
    query?: EvmQueryBuilder
    middleware?: Middleware | Middleware[]
}

export function createEvmPortalSource(options: EvmPortalDataReaderOptions) {
    const createBlockStream = async function* ({
        cursor,
        query,
    }: DataReadRequest<BlockRef, EvmQueryBuilder>): AsyncIterableIterator<
        DataMessage<BlockRef, Block<RequiredFieldSelection>[]>
    > {
        const finalQuery = new EvmQueryBuilder().addFields(REQUIRED_FIELDS).merge(options.query).merge(query)
        if (cursor) {
            finalQuery.setRange({ from: cursor.number + 1 })
        }

        for (const { range, request } of finalQuery.calculateRanges()) {
            console.log({
                type: 'evm',
                fromBlock: range.from,
                toBlock: range.to,
                fields: finalQuery.getFields(),
                ...request,
            })
            let portalSource = portalDataSource({
                portal: typeof options.portal === 'string' ? { url: options.portal } : options.portal,
                query: {
                    type: 'evm',
                    fromBlock: range.from,
                    toBlock: range.to,
                    fields: finalQuery.getFields(),
                    ...request,
                },
            })

            if (options.middleware) {
                const middleware = Array.isArray(options.middleware) ? options.middleware : [options.middleware]
                for (const pipe of middleware) {
                    portalSource.pipe(pipe)
                }
            }

            yield* portalSource
                .map((i) =>
                    i.map((i) => {
                        return createBlock<RequiredFieldSelection>(i)
                    }),
                )
                .read({ cursor })
        }
    }

    return createSource({
        cursorUtils: BlockRefUtils,
        read: (opts: DataReadRequest<BlockRef, EvmQueryBuilder>) => createBlockStream(opts),
    })
}
