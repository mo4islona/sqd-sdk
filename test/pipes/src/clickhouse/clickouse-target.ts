import { type BlockRef, createTarget } from '@belopash/core'
import { last } from '@belopash/core/internal'
import type { ClickHouseClient } from '@clickhouse/client'
import { ClickhouseState } from './clickhouse-state'
import { ClickhouseStore } from './clickhouse-store'

const table = (table: string) => `
CREATE TABLE IF NOT EXISTS ${table}
(
    id               String COMMENT 'Stream identifier to differentiate multiple logical streams',
    latest           String COMMENT 'Latest offset (usually corresponds to the most recent known block)',
    initial          String COMMENT 'The first offset from which this stream started tracking',
    finalized_head   String COMMENT 'The finalized hed',
    chain_continuity String COMMENT 'JSON-encoded list of block references starting from the finalized block and including all unfinalized blocks',
    timestamp        DateTime(3) COMMENT 'Timestamp of the record, in milliseconds with 3 decimal precision',
    sign             Int8 COMMENT 'Marker used by CollapsingMergeTree to distinguish insertions (+1) and deletions (-1)'
) ENGINE = CollapsingMergeTree(sign)
  ORDER BY (timestamp, id)
`

/**
 * Configuration options for ClickhouseState.
 */
export type Settings = {
    /**
     * Name of the ClickHouse database to use.
     * Defaults to "default" if not provided.
     */
    database?: string

    /**
     * Name of the table to store offset data.
     */
    table?: string

    /**
     * Stream identifier used to isolate offset records within the same table.
     * Defaults to "stream" if not provided.
     */
    id?: string

    /**
     * Optional advanced settings.
     */
    settings?: {
        /**
         * Maximum number of rows to retain per unique stream id in the offset table.
         * Older rows beyond this count will be removed.
         * Default is 10,000.
         */
        maxRows?: number
    }
}

export function createClickhouseTarget<Data>({
    client,
    onStart,
    onData,
    onRollback,
    settings = {},
}: {
    client: ClickHouseClient
    settings?: Settings
    onStart?: (batch: { store: ClickhouseStore }) => unknown | Promise<unknown>
    onData: (batch: { store: ClickhouseStore; data: Data[] }) => unknown | Promise<unknown>
    onRollback?: (batch: { type: 'offset_check' | 'blockchain_fork'; store: ClickhouseStore; cursor: BlockRef }) =>
        | unknown
        | Promise<unknown>
}) {
    // GENERATE!!!
    const { database = 'default', table = 'sync_state', id = 'stream' } = settings

    const store = new ClickhouseStore(client)
    const state = new ClickhouseState(client, { database, table, id })

    return createTarget<BlockRef, Data[], never, Promise<void>>({
        write: async (stream) => {
            await onStart?.({ store })

            const cursor = await state.getCursor()
            if (cursor?.current) {
                await onRollback?.({ type: 'offset_check', store, cursor: cursor.current })
            }

            for await (const message of stream.read({ cursor: cursor?.current })) {
                switch (message.type) {
                    case 'data': {
                        await onData({ store, data: message.data.flatMap((d) => d.value) })

                        const first = message.data[0]
                        const last = message.data[message.data.length - 1]

                        await state.saveCursor({
                            batch: {
                                first: first.cursor,
                                last: last.cursor,
                            },
                            finalized: message.finalizedHead,
                            chain_continuity: message.data.map((d) => d.cursor),
                        })

                        break
                    }
                    case 'fork': {
                        if (!onRollback) {
                            throw new Error('Not yet implemented')
                        }

                        const cursor = await state.fork(message.cursors)
                        if (!cursor) {
                            throw new Error('Block not found')
                        }

                        await onRollback({ type: 'blockchain_fork', store, cursor })
                    }
                }
            }

            await store.close()
        },
    })
}
