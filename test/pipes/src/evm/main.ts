import { createEvmPortalSource } from '@belopash/evm-stream'
import { createCacheLayer } from '../cache-layer'
import { createProgressTracker } from '../progress-tracker'
import { swapPriceExtension } from './swap-price-extension'
import { uniswapV3Pools } from './uniswap-v3-pools'
import { uniswapV3Swaps } from './uniswap-v3-swaps'

async function main() {
    const stream = createEvmPortalSource({
        portal: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
        middleware: createCacheLayer({ path: 'uniswap3-zip.sqlite', compress: true }),
    })
        .pipe(createProgressTracker())
        .pipe(uniswapV3Swaps({ to: 13369621 }))
        .pipe(swapPriceExtension())
        .pipe(uniswapV3Pools({ to: 13370621 }))

    for await (const blocks of stream) {
        const swapsCount = blocks.reduce((acc, { swaps }) => acc + swaps.length, 0)
        const poolsCount = blocks.reduce((acc, { pools }) => acc + pools.length, 0)

        const swaps = blocks.flatMap((b) => b.swaps)
        console.log(swaps[0])
        console.log(`${swapsCount} swaps and ${poolsCount} pools for ${blocks.length} blocks`)
    }
}

void main()
