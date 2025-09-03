import type { BlockRef } from '@belopash/core'
import { createTransformer } from '@belopash/core'
import { type Block, EvmQueryBuilder } from '@belopash/evm-stream'
import { type BlockRange, parseRange } from '../block-range'
import * as factoryAbi from './abi/factory'

type PoolCreated = ReturnType<typeof factoryAbi.events.PoolCreated.decode>

export function uniswapV3Pools<Pipe>({ range }: { range?: BlockRange }) {
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

    const poolQuery = new EvmQueryBuilder().addFields(fields).addLog({
        range: { from, to },
        request: {
            topic0: [factoryAbi.events.PoolCreated.topic],
        },
    })

    return createTransformer<
        (Pipe & Block<typeof fields>)[],
        (Pipe & { pools: PoolCreated[] })[],
        EvmQueryBuilder,
        BlockRef
    >((stream) => {
        return {
            cursorUtils: stream.cursorUtils,
            read: async function* (reader) {
                for await (const message of stream.read({
                    cursor: reader.cursor,
                    query: poolQuery.merge(reader.query),
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
                                    pools: v.logs
                                        .filter((log) => factoryAbi.events.PoolCreated.is(log))
                                        .map((log) => factoryAbi.events.PoolCreated.decode(log)),
                                })),
                            }
                        }),
                    }
                }
            },
        }
    })
}
