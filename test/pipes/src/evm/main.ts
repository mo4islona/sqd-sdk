import { EvmQueryBuilder, createEvmPortalSource } from '@belopash/evm-stream'
import { createCacheLayer } from '../cache-layer'
import { createProgressTracker } from '../progress-tracker'
import { swapPriceExtension } from './swap-price-extension'
import { uniswapV3Pools } from './uniswap-v3-pools'
import { uniswapV3Swaps } from './uniswap-v3-swaps'

async function main() {
    const stream = createEvmPortalSource({
        portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
        query: new EvmQueryBuilder().addFields({
            log: {
                transactionIndex: true,
            },
            transaction: {
                sighash: true,
            },
        }),
        middleware: createCacheLayer({
            path: './cache.sqlite',
            compress: true,
        }),
    })
        .pipe(createProgressTracker())
        .pipe(uniswapV3Swaps({ range: { from: '12,369,621', to: '+1_000' } }))
        .pipe(swapPriceExtension())
        .pipe(uniswapV3Pools({ range: { from: '12,369,621', to: '+2_000' } }))

    for await (const blocks of stream) {
        const swapsCount = blocks.reduce((acc, { swaps }) => acc + swaps.length, 0)
        const poolsCount = blocks.reduce((acc, { pools }) => acc + pools.length, 0)

        const swaps = blocks.flatMap((b) => b.swaps)
        if (swaps[0]) {
            console.log('SWAP EXAMPLE', swaps[0])
        }
        console.log(`${swapsCount} swaps and ${poolsCount} pools for ${blocks.length} blocks`)
    }
}

void main()
