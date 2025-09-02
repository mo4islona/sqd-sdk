import {createTransformer} from '@belopash/core'
import {type Block, type EvmDataRequestRange, EvmQueryBuilder} from '@belopash/evm-stream'
import * as poolAbi from './abi/pool'

type Swap = ReturnType<typeof poolAbi.events.Swap.decode>

type Selection = {
    log: {
        address: true
        topics: true
        data: true
        transactionIndex: true
        transactionHash: true
        logIndex: true
    }
    transaction: {hash: true; transactionIndex: true}
}

type BlockRequest = number

export function uniswapV3Swaps<Cursor, In extends Block<Selection>>({
    from = 12369621,
    to,
}: {from?: BlockRequest; to?: BlockRequest}) {
    const uniswapQuery = new EvmQueryBuilder()
        .addLog({
            range: {from, to},
            request: {
                topic0: [poolAbi.events.Swap.topic],
            },
        })
        .build()

    return createTransformer<
        Cursor,
        Cursor,
        In[],
        (In & {swaps: Swap[]})[],
        EvmDataRequestRange[],
        EvmDataRequestRange[]
    >((stream) => {
        return {
            cursorUtils: stream.cursorUtils,
            read: async function* (reader) {
                for await (const message of stream.read({
                    cursor: reader.cursor,
                    query: [...uniswapQuery, ...(reader.query || [])],
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
    })
}
