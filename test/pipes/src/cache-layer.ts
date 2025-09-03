import { gunzip, gzip } from 'node:zlib'
import type { DataDataMessage } from '@belopash/core'
import { type DataSource, createSource, createTarget } from '@belopash/core'
import { last } from '@belopash/core/internal'
import type { BlockRef, PortalData } from '@belopash/core/portal'
import Database from 'better-sqlite3'
import { JSONParse, JSONStringify } from 'json-with-bigint'

import { promisify } from 'node:util'
import type { evm } from '@belopash/core/portal/query'

const gunzipAsync = promisify(gunzip)
const gzipAsync = promisify(gzip)

type Data = PortalData<evm.Query>

export function createCacheLayer({ path, compress = false }: { path: string; compress?: boolean }) {
    const db = new Database(path)

    db.exec('CREATE TABLE IF NOT EXISTS data(number INTEGER PRIMARY KEY, value BLOB)')

    const insert = db.prepare('INSERT INTO data (number, value) VALUES (?, ?);')
    const select = db.prepare<[number], { value: string }>(`SELECT * FROM data WHERE number > ? ORDER BY number ASC;`)

    const decompressValue = async (value: string): Promise<string> => {
        if (!compress) return value

        const buffer = await gunzipAsync(value)
        return buffer.toString('utf8')
    }

    const compressValue = async (value: string): Promise<Buffer> => {
        if (!compress) return Buffer.from(value)

        return await gzipAsync(value)
    }

    return createTarget<BlockRef, Data, evm.Query, DataSource<BlockRef, Data, evm.Query>>({
        write: (writer) => {
            return createSource({
                cursorUtils: writer.cursorUtils,
                read: async function* ({ cursor, query }) {
                    let lastCursor = cursor
                    for (const message of select.iterate(cursor ? cursor.number : 0)) {
                        const decoded: DataDataMessage<BlockRef, Data> = JSONParse(await decompressValue(message.value))

                        yield decoded

                        lastCursor = last(decoded.data).cursor
                    }

                    for await (const message of writer.read({ cursor: lastCursor, query })) {
                        switch (message.type) {
                            case 'data': {
                                const lastItem = last(message.data)

                                insert.run(lastItem.cursor.number, await compressValue(JSONStringify(message)))

                                yield message
                                break
                            }
                            case 'fork':
                                throw new Error('fork for caching layer is not yet implemented')
                        }
                    }
                },
            })
        },
    })
}
