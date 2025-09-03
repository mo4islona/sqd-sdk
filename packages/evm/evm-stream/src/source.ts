import type { DataSource, DataTarget } from '@belopash/core'
import { createTransformer } from '@belopash/core'
import type { BlockRef, DataReadRequest } from '@belopash/core/pipeline'
import { BlockRefUtils, createSource } from '@belopash/core/pipeline'
import { type PortalClientOptions, portalDataSource } from '@belopash/core/portal'
import type * as EVM from '@belopash/core/portal/evm'
import { REQUIRED_FIELDS, type RequiredFieldSelection, createBlock } from './objects'
import { EvmQueryBuilder } from './query'

type Middleware = DataTarget<
    BlockRef,
    EVM.Block<RequiredFieldSelection>[],
    EvmQueryBuilder,
    DataSource<BlockRef, EVM.Block<RequiredFieldSelection>[], EvmQueryBuilder>
>

export interface EvmPortalDataReaderOptions {
    portal: PortalClientOptions | string
    query?: EvmQueryBuilder
    middleware?: Middleware | Middleware[]
}

export function createEvmPortalSource(options: EvmPortalDataReaderOptions) {
    let source = createSource<BlockRef, EVM.Block<RequiredFieldSelection>[], EvmQueryBuilder>({
        cursorUtils: BlockRefUtils,
        read: async function* ({ cursor, query }: DataReadRequest<BlockRef, EvmQueryBuilder>) {
            const finalQuery = new EvmQueryBuilder().merge(query)
            if (cursor) {
                finalQuery.setRange({ from: cursor.number + 1 })
            }

            for (const { range, request } of finalQuery.calculateRanges()) {
                const portalSource = portalDataSource({
                    portal: typeof options.portal === 'string' ? { url: options.portal } : options.portal,
                    query: {
                        type: 'evm',
                        fromBlock: range.from,
                        toBlock: range.to,
                        fields: finalQuery.getFields(),
                        ...request,
                    },
                })

                yield* portalSource.read({ cursor })
            }
        },
    })

    if (options.middleware) {
        const middlewares = Array.isArray(options.middleware) ? options.middleware : [options.middleware]
        for (const middleware of middlewares) {
            source = source.pipe(middleware)
        }
    }

    return source.pipe(
        /*
         We need to add all queries after middlewares to make any changes in the query visible
        */
        createTransformer((stream) => {
            const defaultSelection = new EvmQueryBuilder().addFields(REQUIRED_FIELDS).merge(options.query)

            return {
                cursorUtils: stream.cursorUtils,
                read: async function* ({ query, cursor }) {
                    for await (const message of stream.read({
                        cursor,
                        query: defaultSelection.merge(query),
                    })) {
                        if (message.type !== 'data') {
                            yield message
                            continue
                        }

                        yield {
                            ...message,
                            data: message.data.map((item) => ({
                                ...item,
                                value: item.value.map((v) => createBlock<RequiredFieldSelection>(v)),
                            })),
                        }
                    }
                },
            }
        }),
    )
}
