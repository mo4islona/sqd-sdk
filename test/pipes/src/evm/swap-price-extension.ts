import type { BlockRef } from '@belopash/core'
import { createTransformer } from '@belopash/core'
import type { EvmQueryBuilder } from '@belopash/evm-stream'

import type { Swap } from './uniswap-v3-swaps'

type SwapExtended = Swap & { price: string }

export function swapPriceExtension<Pipe extends { swaps: Swap[] }>() {
    return createTransformer<Pipe[], (Pipe & { swaps: SwapExtended[] })[], EvmQueryBuilder, BlockRef>((stream) => {
        return {
            cursorUtils: stream.cursorUtils,
            read: async function* (reader) {
                for await (const message of stream.read(reader)) {
                    if (message.type !== 'data') {
                        yield message
                        continue
                    }

                    // VERY UGLY!
                    yield {
                        ...message,
                        data: message.data.map((blocks) => {
                            return {
                                ...blocks,
                                value: blocks.value.map((block) => {
                                    return {
                                        ...block,
                                        swaps: block.swaps.map((s): SwapExtended => {
                                            return {
                                                ...s,
                                                price: '10_000',
                                            }
                                        }),
                                    }
                                }),
                            }
                        }),
                    }
                }
            },
        }
    })
}
