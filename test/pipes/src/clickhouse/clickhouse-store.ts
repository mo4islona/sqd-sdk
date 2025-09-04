import type { ClickHouseClient, InsertParams, InsertResult } from '@clickhouse/client'
import { loadSqlFiles } from './fs'

export class ClickhouseStore {
    constructor(public client: ClickHouseClient) {}

    insert<T>(params: InsertParams<T>): Promise<InsertResult> {
        return this.client.insert(params as any)
    }

    close() {
        return this.client.close()
    }

    // FIXME use glob
    async executeFiles(dir: string) {
        const queries = await loadSqlFiles(dir)

        for (const query of queries) {
            try {
                await this.client.command({ query })
            } catch (e: any) {
                console.error(e)

                process.exit(1)
            }
        }
    }

    async removeAllRows({
        table,
        params,
        where,
    }: {
        table: string | string[]
        where: string
        params?: Record<string, unknown>
    }) {
        const tables = typeof table === 'string' ? [table] : table

        await Promise.all(
            tables.map(async (table) => {
                // TODO check engine

                const count = await this._removeAllRows({
                    table,
                    query: `SELECT * FROM ${table} FINAL WHERE ${where}`,
                    params,
                })

                return { table, count }
            }),
        )
    }

    private async _removeAllRows({
        table,
        query,
        params,
    }: {
        table: string
        query: string
        params?: Record<string, unknown>
    }) {
        let count = 0
        const res = await this.client.query({
            query,
            format: 'JSONEachRow',
            clickhouse_settings: {
                date_time_output_format: 'iso',
            },
            query_params: params,
        })

        for await (const rows of res.stream()) {
            await this.client.insert({
                table,
                values: rows.map((row: any) => {
                    const data = row.json()

                    data.sign = -1

                    return data
                }),
                format: 'JSONEachRow',
                clickhouse_settings: {
                    date_time_input_format: 'best_effort',
                },
            })

            count += rows.length
        }

        return count
    }
}
