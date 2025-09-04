import path from 'node:path'
import { createEvmPortalSource } from '@belopash/evm-stream'
import { createClient } from '@clickhouse/client'
import { createCacheLayer } from '../cache-layer'
import { createClickhouseTarget } from '../clickhouse/clickouse-target'
import { createProgressTracker } from '../progress-tracker'
import { swapPriceExtension } from './swap-price-extension'
import { uniswapV3Swaps } from './uniswap-v3-swaps'

async function main() {
    const client = createClient({
        password: process.env.CLIKCHOUSE_PASSWORD,
        clickhouse_settings: {
            date_time_input_format: 'best_effort', // support native date class
        },
    })

    createEvmPortalSource({
        portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
        middleware: createCacheLayer({
            path: './cache.sqlite',
            compress: true,
        }),
    })
        .pipe(createProgressTracker())
        .pipe(uniswapV3Swaps({ range: { from: '12,369,621' } }))
        .pipe(swapPriceExtension())
        .pipe(
            createClickhouseTarget({
                client,
                onStart: async ({ store }) => {
                    await store.executeFiles(path.join(__dirname, 'sql/00_create_tables.sql'))
                },
                onData: async ({ store, data }) => {
                    await store.insert({
                        table: 'swaps_raw',
                        values: data
                            .flatMap((d) => d.swaps)
                            .map((s) => ({
                                timestamp: s.timestamp,
                                account: s.sender,
                                token_a: '',
                                token_b: '',
                                amount_a: s.amount0.toString(),
                                amount_b: s.amount1.toString(),

                                block_number: s.rawEvent.block.header.number,
                                transaction_index: s.rawEvent.transactionIndex,
                                log_index: s.rawEvent.logIndex,

                                sign: 1,
                            })),
                        format: 'JSONEachRow',
                    })
                },
                onRollback: async ({ store, cursor }) => {
                    await store.removeAllRows({
                        table: 'swaps_raw',
                        where: 'block_number > {block_number:UInt32}',
                        params: { block_number: cursor.number },
                    })
                },
            }),
        )
}

void main()
