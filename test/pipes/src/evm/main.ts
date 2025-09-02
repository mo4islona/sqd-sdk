import { createEvmPortalSource } from '@belopash/evm-stream'
import { createCacheLayer } from '../cache-layer'
import { createProgressTracker } from '../progress-tracker'
import { uniswapV3Swaps } from './uniswap-v3'

async function main() {
    const stream = createEvmPortalSource(
        {
            portal: {
                url: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
            },
            fields: {
                block: {
                    timestamp: true,
                },
                log: {
                    address: true,
                    topics: true,
                    data: true,
                    transactionHash: true,
                },
                transaction: {
                    hash: true,
                    transactionIndex: true,
                },
            },
        },
        createCacheLayer({ path: 'uniswap3-zip.sqlite', compress: true }),
    )
        .pipe(createProgressTracker())
        .pipe(uniswapV3Swaps({ to: 13369621 }))

    for await (const blocks of stream) {
        const swapsCount = blocks.reduce((acc, { swaps }) => acc + swaps.length, 0)

        console.log(`${swapsCount} swaps for ${blocks.length} blocks`)
    }
}

void main()
