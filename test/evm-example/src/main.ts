import {In} from 'typeorm'
import {PortalClient} from '@belopash/core/portal'
import {HttpClient} from '@belopash/core/http-client'
import {
    createTransformer,
    DataCursor,
    type BlockRef,
    type DataDuplex,
    type DataTargetFactoryOptions,
} from '@belopash/core/pipeline'
import {createEvmPortalSource, EvmQueryBuilder, type Block, type EvmDataRequestRange} from '@belopash/evm-stream'
import * as factoryAbi from './abi/factory'
import * as poolAbi from './abi/pool'
import {Pool, Swap} from './model'
import type {Store as OrmStore} from '@belopash/typeorm-target'
import * as fs from 'node:fs'
import * as path from 'node:path'

export interface PoolIndexItem {
    cursor: {number: number; hash: string}
    value: {poolAddress: string}
}

export const FACTORY_ADDRESS = '0x1f98431c8ad98523631ae4a59f267346ea31f984'

let portal = new PortalClient({
    url: 'https://portal.sqd.dev/datasets/ethereum-mainnet',
    http: new HttpClient({
        retryAttempts: Number.POSITIVE_INFINITY,
    }),
    minBytes: 100 * 1024 * 1024,
})

async function main() {
    await createEvmPortalSource({
        portal,
        fields: {
            block: {number: true, timestamp: true, hash: true, parentHash: true},
            log: {
                address: true,
                topics: true,
                data: true,
                transactionIndex: true,
                transactionHash: true,
                logIndex: true,
            },
            transaction: {hash: true, transactionIndex: true},
        },
    })
        .pipe(createFactoryFilter({address: FACTORY_ADDRESS}))
        .map((item) => item.flatMap((i) => i.logs))
        .scan((acc, item) => acc + item.length, 0)
        .forEach((count) => console.log(`found ${count} swaps`))

    console.log('end')
}

function createFactoryFilter<
    TValue extends Block<{
        log: {
            address: true
            topics: true
            data: true
        }
    }>[],
>({
    address,
}: {address: string}): (
    opts: DataTargetFactoryOptions,
) => DataDuplex<BlockRef, BlockRef, TValue, TValue[number][], EvmDataRequestRange[], never> {
    const POOL_LIMIT = 4876 // 200KB limit
    const POOLS_FILE = path.join(process.cwd(), 'assets', 'pools.json')

    const createSwapQuery = (pools: {cursor: BlockRef; value: {poolAddress: string}}[], end?: BlockRef) => {
        const builder = new EvmQueryBuilder()
        const limitedPools = pools.slice(0, POOL_LIMIT)

        // Query for known pools
        builder.addLog({
            range: {from: 0, to: end?.number},
            request: {
                topic0: [poolAbi.events.Swap.topic],
                address: limitedPools.map((p) => p.value.poolAddress),
            },
        })

        // Wildcard query for new pools
        const lastPoolCursor = limitedPools[limitedPools.length - 1]?.cursor ?? end
        builder.addLog({
            range: {from: lastPoolCursor.number + 1},
            request: {topic0: [poolAbi.events.Swap.topic]},
        })

        return builder.build()
    }

    const factoryQuery = new EvmQueryBuilder()
        .addLog({
            request: {
                address: [address],
                topic0: [factoryAbi.events.PoolCreated.topic],
            },
        })
        .build()

    const loadPools = (): {pools: {cursor: BlockRef; value: {poolAddress: string}}[]; cursor?: BlockRef} => {
        try {
            if (fs.existsSync(POOLS_FILE)) {
                const data = JSON.parse(fs.readFileSync(POOLS_FILE, 'utf8'))
                return {
                    pools: Array.isArray(data.pools) ? data.pools : [],
                    cursor: data.cursor,
                }
            }
        } catch {}
        return {pools: []}
    }

    const savePools = (pools: {cursor: BlockRef; value: {poolAddress: string}}[], cursor?: BlockRef) => {
        fs.mkdirSync(path.dirname(POOLS_FILE), {recursive: true})
        fs.writeFileSync(POOLS_FILE, JSON.stringify({pools, cursor}, null, 2))
    }

    const processPoolCreationLogs = (
        logs: TValue[number]['logs'],
        cursor: BlockRef,
        pools: {cursor: BlockRef; value: {poolAddress: string}}[],
    ) => {
        for (const log of logs) {
            if (log.address.toLowerCase() === address.toLowerCase() && factoryAbi.events.PoolCreated.is(log)) {
                const event = factoryAbi.events.PoolCreated.decode(log)
                const poolAddress = event.pool.toLowerCase()
                pools.push({cursor, value: {poolAddress}})
                console.log('discovered pool', poolAddress)
            }
        }
    }

    const shouldStopPreindexing = (itemCursor: BlockRef, cursorUtils: any, finalizedHead?: BlockRef) => {
        return finalizedHead && cursorUtils.compare(itemCursor, finalizedHead).isGreater
    }

    return createTransformer((opts) => {
        const {pools: preindexedPools, cursor: preindexedCursor} = loadPools()

        return {
            cursorUtils: opts.cursorUtils,
            read: async function* (readOpts) {
                // Preindexing phase
                console.log('preindexing starting...')
                for await (const message of opts.read({cursor: preindexedCursor, query: factoryQuery})) {
                    if (message.type !== 'data') continue

                    for (const item of message.data) {
                        if (shouldStopPreindexing(item.cursor, opts.cursorUtils, message.finalizedHead)) {
                            break
                        }
                        for (const block of item.value) {
                            processPoolCreationLogs(block.logs, item.cursor, preindexedPools)
                        }
                    }

                    if (shouldStopPreindexing(message.head, opts.cursorUtils, message.finalizedHead)) {
                        break
                    }
                }

                savePools(preindexedPools, preindexedCursor)
                console.log('preindex ended. known pools: ', preindexedPools.length)

                const poolsQuery = createSwapQuery(preindexedPools, preindexedCursor)
                const poolsSet = new Set(preindexedPools.map((p) => p.value.poolAddress))

                for await (const message of opts.read({
                    cursor: readOpts.cursor,
                    query: [...factoryQuery, ...poolsQuery],
                })) {
                    if (message.type !== 'data') {
                        yield message
                        continue
                    }

                    // FIXME: track new pools as well

                    const data = message.data.map((i) => {
                        return {
                            cursor: i.cursor,
                            value: i.value.map((b) => ({
                                ...b,
                                logs: b.logs.filter((log) => poolsSet.has(log.address)),
                            })),
                        }
                    })

                    yield {
                        type: 'data',
                        finalizedHead: message.finalizedHead,
                        head: message.head,
                        data,
                    }
                }
            },
        }
    })
}

interface PoolData {
    id: string
    token0: string
    token1: string
}

function getPoolData(log: {address: string; data: string; topics: string[]}): PoolData {
    let event = factoryAbi.events.PoolCreated.decode(log)

    let id = event.pool.toLowerCase()
    let token0 = event.token0.toLowerCase()
    let token1 = event.token1.toLowerCase()

    return {
        id,
        token0,
        token1,
    }
}

async function createPools(store: {insert: (e: any) => Promise<unknown>}, poolsData: PoolData[]) {
    let pools: Pool[] = []

    for (let p of poolsData) {
        let pool = new Pool(p)
        pools.push(pool)
    }

    if (pools.length > 0) {
        await store.insert(pools)
    }
}

interface SwapEvent {
    id: string
    block: {height: number; timestamp: number}
    pool: string
    amount0: bigint
    amount1: bigint
    recipient: string
    sender: string
    txHash: string
}

function getSwap(
    block: {header: {number: number; timestamp: number; hash: string}},
    log: {
        address: string
        data: string
        topics: string[]
        transactionIndex: number
        transactionHash: string
        logIndex: number
    },
): SwapEvent {
    let event = poolAbi.events.Swap.decode(log)

    let pool = log.address.toLowerCase()
    let recipient = event.recipient.toLowerCase()
    let sender = event.sender.toLowerCase()

    return {
        id: formatId({number: block.header.number, hash: block.header.hash}, log.transactionIndex, log.logIndex),
        block: {height: block.header.number, timestamp: block.header.timestamp},
        txHash: log.transactionHash,
        pool,
        amount0: event.amount0,
        amount1: event.amount1,
        recipient,
        sender,
    }
}

async function processSwaps(store: Pick<OrmStore, 'findBy' | 'insert'>, swapsData: SwapEvent[]) {
    let poolIds = new Set<string>()
    for (let t of swapsData) {
        poolIds.add(t.pool)
    }

    let pools = await store.findBy(Pool, {id: In([...poolIds]) as unknown as any}).then(toEntityMap)

    let swaps: Swap[] = []
    for (let s of swapsData) {
        let {id, block, txHash, amount0, amount1, recipient, sender} = s

        let pool = pools.get(s.pool)
        if (!pool) continue

        swaps.push(
            new Swap({
                id,
                blockNumber: block.height,
                timestamp: new Date(block.timestamp),
                txHash,
                pool,
                amount0,
                amount1,
                recipient,
                sender,
            }),
        )
    }

    if (swaps.length > 0) {
        await store.insert(swaps)
    }
}

interface LogRef extends BlockRef {
    logIndex?: number
}

const LogRefUrils = {
    compare: (a: LogRef, b: LogRef) => {
        if (a.number > b.number) return DataCursor.Greater
        if (a.number < b.number) return DataCursor.Less
        if (a.hash !== b.hash) return DataCursor.Fork
        if (a.logIndex == null && b.logIndex == null) return DataCursor.Equal
        if (a.logIndex == null) return DataCursor.Greater
        if (b.logIndex == null) return DataCursor.Less
        if (a.logIndex > b.logIndex) return DataCursor.Greater
        if (a.logIndex < b.logIndex) return DataCursor.Less
        return DataCursor.Equal
    },
    serialize: (value: LogRef) => value,
    deserialize: (value: unknown) => value as LogRef,
}

function toEntityMap<E extends {id: string}>(entities: E[]): Map<string, E> {
    return new Map(entities.map((e) => [e.id, e]))
}

function createTimer() {
    let start: number | undefined = undefined

    return {
        start: () => {
            start = Date.now()
        },
        stop: () => {
            if (start == null) return 0
            let elapsed = Date.now() - start
            start = undefined
            return elapsed
        },
    }
}

function formatId(block: {number: number; hash: string}, ...address: number[]): string {
    let no = block.number.toString().padStart(12, '0')
    let hash = block.hash.slice(0, 5)
    let id = `${no}-${hash}`
    for (let index of address) {
        id += `-${index.toString().padStart(6, '0')}`
    }
    return id
}

main()
