import type { BlockRef } from '@belopash/core'
import { createTransformer } from '@belopash/core'
import { type Block, EvmQueryBuilder } from '@belopash/evm-stream'
import { type BlockRange, parseRange } from '../block-range'
import * as poolAbi from './abi/pool'

type Swap = ReturnType<typeof poolAbi.events.Swap.decode>

export function uniswapV3Swaps<Pipe>({ range }: { range?: BlockRange }) {
    const { from, to } = parseRange({ range, defaultFrom: 12369621 })

    const fields = {
        log: {
            address: true,
            topics: true,
            data: true,
            transactionIndex: true,
            transactionHash: true,
            logIndex: true,
        },
        transaction: {
            hash: true,
            transactionIndex: true,
        },
    } as const

    const uniswapQuery = new EvmQueryBuilder().addFields(fields).addLog({
        range: { from, to },
        request: {
            topic0: [poolAbi.events.Swap.topic],
        },
    })

    return createTransformer<(Pipe & Block<typeof fields>)[], (Pipe & { swaps: Swap[] })[], EvmQueryBuilder, BlockRef>(
        (stream) => {
            return {
                cursorUtils: stream.cursorUtils,
                read: async function* (reader) {
                    for await (const message of stream.read({
                        cursor: reader.cursor,
                        query: uniswapQuery.merge(reader.query),
                    })) {
                        if (message.type !== 'data') {
                            yield message
                            continue
                        }

                        yield {
                            ...message,
                            data: message.data.map((m) => {
                                return {
                                    cursor: m.cursor,
                                    value: m.value.map((v) => ({
                                        ...v,
                                        swaps: v.logs
                                            .filter((log) => poolAbi.events.Swap.is(log))
                                            .map((log) => poolAbi.events.Swap.decode(log)),
                                    })),
                                }
                            }),
                        }
                    }
                },
            }
        },
    )
}
