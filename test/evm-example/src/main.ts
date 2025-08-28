import {In} from 'typeorm'
import {PortalClient} from '@belopash/core/portal'
import {HttpClient} from '@belopash/core/http-client'
import {
    createStream,
    createTracker,
    createTransformer,
    type BlockRef,
    type DataDuplex,
    type DataTargetFactoryOptions,
} from '@belopash/core/pipeline'
import {createTypeormTarget} from '@belopash/typeorm-target/database'
import {
    evmPortalDataSource,
    EvmQueryBuilder,
    type Block,
    type EvmDataRequestRange,
    type Log,
} from '@belopash/evm-stream'
import * as factoryAbi from './abi/factory'
import * as poolAbi from './abi/pool'
import {Pool, Swap} from './model'
import type {Store as OrmStore} from '@belopash/typeorm-target'
import {createLogger} from '@belopash/core/logger'
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
    await createStream(() =>
        evmPortalDataSource({
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
        }),
    )
        .pipe(createFactoryFilter({address: FACTORY_ADDRESS}))
        .pipe(createProgressTracker('evm'))
        .pipe(
            createTypeormTarget({}, async (store, batch) => {
                let pools: PoolData[] = []
                let swaps: SwapEvent[] = []

                for (let block of batch) {
                    for (let event of block.uniswap) {
                        const address = event.log.address.toLowerCase()
                        const topic0 = event.log.topics[0]?.toLowerCase()

                        if (address === FACTORY_ADDRESS && topic0 === factoryAbi.events.PoolCreated.topic) {
                            pools.push(getPoolData(event.log))
                        } else if (topic0 === poolAbi.events.Swap.topic) {
                            swaps.push(getSwap(block, event.log))
                        }
                    }
                }

                await createPools(store, pools)
                await processSwaps(store, swaps)
            }),
        )

    console.log('end')
}

function createFactoryFilter<
    TValue extends Block<{
        log: {
            address: true
            topics: true
            data: true
        }
    }>,
>({
    address,
}: {address: string}): (
    opts: DataTargetFactoryOptions,
) => DataDuplex<
    BlockRef,
    BlockRef,
    TValue,
    TValue & {uniswap: {data: any; log: TValue['logs'][number]}[]},
    EvmDataRequestRange[],
    never
> {
    function createQuery(pools: {cursor: BlockRef; value: {poolAddress: string}}[], end: BlockRef | undefined) {
        const queryBuiler = new EvmQueryBuilder()

        const limitedPools = pools.slice(0, 4876) // 200KB
        queryBuiler.addLog({
            range: {from: 0, to: end?.number},
            request: {
                topic0: [poolAbi.events.Swap.topic],
                address: limitedPools.map((p) => p.value.poolAddress),
            },
        })
        const wildcardCursor = limitedPools[limitedPools.length - 1].cursor ?? end
        if (wildcardCursor) {
            queryBuiler.addLog({
                range: {from: wildcardCursor.number + 1},
                request: {
                    topic0: [poolAbi.events.Swap.topic],
                },
            })
        }
        return queryBuiler.build()
    }

    const factoryQuery = new EvmQueryBuilder()
        .addLog({
            request: {
                address: [address],
                topic0: [factoryAbi.events.PoolCreated.topic],
            },
        })
        .build()

    return createTransformer((opts) => {
        const preindexedPools: {cursor: BlockRef; value: {poolAddress: string}}[] = []
        let preindexedCursor: BlockRef | undefined = undefined
        const dir = path.join(process.cwd(), 'assets')
        const file = path.join(dir, 'pools.json')

        try {
            if (fs.existsSync(file)) {
                const json = fs.readFileSync(file, 'utf8')
                const parsed = JSON.parse(json)
                if (parsed && Array.isArray(parsed.pools)) {
                    preindexedPools.push(...parsed.pools)
                    preindexedCursor = parsed.cursor
                }
            }
        } catch {}

        const savePools = () => {
            fs.mkdirSync(dir, {recursive: true})
            const out = {
                cursor: preindexedCursor,
                pools: preindexedPools,
            }
            fs.writeFileSync(file, JSON.stringify(out, null, 2))
        }

        return {
            cursorUtils: opts.cursorUtils,
            read: async function* (readOpts) {
                console.log('preindexing starting...')
                for await (let message of opts.read({
                    cursor: preindexedCursor,
                    query: factoryQuery,
                })) {
                    if (message.type === 'batch') {
                        for (let item of message.data) {
                            if (
                                !message.finalizedHead ||
                                opts.cursorUtils.compare(item.cursor, message.finalizedHead).isGreater
                            ) {
                                break
                            }

                            const block = item.value
                            for (let log of block.logs) {
                                if (
                                    log.address.toLowerCase() === FACTORY_ADDRESS &&
                                    factoryAbi.events.PoolCreated.is(log)
                                ) {
                                    const event = factoryAbi.events.PoolCreated.decode(log)
                                    const poolAddress = event.pool.toLowerCase()
                                    preindexedPools.push({cursor: item.cursor, value: {poolAddress}})
                                    console.log('discovered pool', poolAddress)
                                }
                            }

                            preindexedCursor = item.cursor
                        }

                        if (
                            !message.finalizedHead ||
                            opts.cursorUtils.compare(message.cursor, message.finalizedHead).isGreater
                        ) {
                            break
                        }
                    }
                }
                savePools()
                console.log('preindex ended. known pools', preindexedPools.length)

                const poolsQuery = createQuery(preindexedPools, preindexedCursor)
                const poolsSet = new Set(preindexedPools.map((p) => p.value.poolAddress))

                for await (let message of opts.read({
                    cursor: readOpts.cursor,
                    query: [...factoryQuery, ...poolsQuery],
                })) {
                    if (message.type === 'batch') {
                        yield {
                            type: 'batch',
                            cursor: message.cursor,
                            finalizedHead: message.finalizedHead,
                            head: message.head,
                            data: message.data.map((item) => {
                                return {
                                    cursor: item.cursor,
                                    value: {
                                        ...item.value,
                                        uniswap: item.value.logs
                                            .filter((l) => poolsSet.has(l.address) || l.address === address)
                                            .map((l) => ({
                                                data:
                                                    l.address === address
                                                        ? factoryAbi.events.PoolCreated.decode(l)
                                                        : poolAbi.events.Swap.decode(l),
                                                log: l,
                                            })),
                                    },
                                }
                            }),
                        }
                    } else {
                        yield message
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

function toEntityMap<E extends {id: string}>(entities: E[]): Map<string, E> {
    return new Map(entities.map((e) => [e.id, e]))
}

export function createProgressTracker<
    TCursor extends {number: number; hash: string},
    TValue extends {header: {timestamp: number}},
    TQuery,
>(prefix: string) {
    const logger = createLogger(`sqd:${prefix}`)
    const readTimer = createTimer()
    const writeTimer = createTimer()
    const logIntervalMs = 5_000
    const emitLog = () => {
        if (!stats?.cursor || !stats.head) return
        const now = Date.now()
        const headNumber = stats.head?.number ?? stats.cursor.number
        const finalizedNumber = stats.finalizedHead?.number
        const remainingBlocks = Math.max(0, headNumber - stats.cursor.number)
        const percentVal = (1 - remainingBlocks / headNumber) * 100
        const percent = Math.max(0, Math.min(100, percentVal))
        const etaSec =
            stats.avgBlocksPerSec && stats.avgBlocksPerSec > 0 ? remainingBlocks / stats.avgBlocksPerSec : undefined
        const percentStr = `${percent.toFixed(2)}%`
        const etaStr = etaSec == null ? 'n/a' : `${etaSec.toFixed(0)}s`

        // Compute windowed throughput since last log; if nothing happened, it decays to 0
        const windowMs = now - (stats.lastLogTimeMs ?? now)
        const windowBlocks = (stats.totalBlocks ?? 0) - (stats.lastLogTotalBlocks ?? 0)
        const windowBlocksPerSec = windowMs > 0 ? (windowBlocks * 1000) / windowMs : 0
        stats.avgBlocksPerSec =
            stats.avgBlocksPerSec == null ? windowBlocksPerSec : (stats.avgBlocksPerSec + windowBlocksPerSec) / 2

        logger.info(
            {
                lag: `${((now - (stats.lastBlockTime ?? now)) / 1000).toFixed(2)}s`,
                batchSize: stats.lastBatchSize ?? 0,
                blocksPerSec: Number(windowBlocksPerSec.toFixed(2)),
                avgBlocksPerSec: Number((stats.avgBlocksPerSec ?? 0).toFixed(2)),
                avgBatchSize: Number((stats.avgBatchSize ?? 0).toFixed(2)),
                avgReadTime: `${((stats.avgReadTime ?? 0) / 1000).toFixed(2)}s`,
                lastReadTime: `${((stats.lastReadTime ?? 0) / 1000).toFixed(2)}s`,
                avgWriteTime: `${((stats.avgWriteTime ?? 0) / 1000).toFixed(2)}s`,
                lastWriteTime: `${((stats.lastWriteTime ?? 0) / 1000).toFixed(2)}s`,
                totalBlocks: stats.totalBlocks,
            },
            `progress: ${stats.cursor.number} / ${headNumber} (${finalizedNumber ?? 0}) — ${percentStr}, ETA: ${etaStr}`,
        )

        // Update window markers
        stats.lastLogTimeMs = now
        stats.lastLogTotalBlocks = stats.totalBlocks ?? 0
    }

    let stats:
        | {
              cursor: TCursor
              head: TCursor | undefined
              finalizedHead: TCursor | undefined
              lastBlockTime: number | undefined
              avgReadTime: number | undefined
              lastReadTime: number | undefined
              avgWriteTime: number | undefined
              lastWriteTime: number | undefined
              startNumber: number | undefined
              startTimeMs: number | undefined
              targetNumber: number | undefined
              lastLogTimeMs: number | undefined
              lastLogTotalBlocks: number | undefined
              totalBlocks: number | undefined
              lastBatchSize: number | undefined
              avgBatchSize: number | undefined
              avgBlocksPerSec: number | undefined
          }
        | undefined = undefined

    return createTracker<TCursor, TValue, TQuery>({
        beforeRead: (cursor) => {
            if (!stats && cursor) {
                logger.info(`continue from ${cursor.number}`)
                stats = {
                    cursor,
                    head: undefined,
                    finalizedHead: undefined,
                    lastBlockTime: undefined,
                    avgReadTime: undefined,
                    avgWriteTime: undefined,
                    lastReadTime: undefined,
                    lastWriteTime: undefined,
                    startNumber: cursor.number,
                    startTimeMs: Date.now(),
                    targetNumber: undefined,
                    lastLogTimeMs: Date.now(),
                    lastLogTotalBlocks: 0,
                    totalBlocks: 0,
                    lastBatchSize: undefined,
                    avgBatchSize: undefined,
                    avgBlocksPerSec: undefined,
                }
                setInterval(emitLog, logIntervalMs)
            }
            readTimer.start()
        },
        afterRead: () => {
            if (!stats) return

            const elapsed = readTimer.stop()

            stats.avgReadTime = stats.avgReadTime == null ? elapsed : (stats.avgReadTime + elapsed) / 2
            stats.lastReadTime = elapsed
        },
        beforeWrite: () => {
            writeTimer.start()
        },
        afterWrite: (message) => {
            if (!stats) return

            const elapsed = writeTimer.stop()
            stats.avgWriteTime = stats.avgWriteTime == null ? elapsed : (stats.avgWriteTime + elapsed) / 2
            stats.lastWriteTime = elapsed
            stats.lastBlockTime = message.data[message.data.length - 1].value.header.timestamp * 1000
            stats.head = message.head
            stats.finalizedHead = message.finalizedHead
            stats.cursor = message.cursor
            if (stats.targetNumber == null) {
                stats.targetNumber = message.finalizedHead?.number ?? message.head.number
            }

            const batchSize = message.data.length
            stats.totalBlocks = (stats.totalBlocks ?? 0) + batchSize
            stats.lastBatchSize = batchSize
            stats.avgBatchSize = stats.avgBatchSize == null ? batchSize : (stats.avgBatchSize + batchSize) / 2

            const cycleMs = (stats.lastReadTime ?? 0) + (stats.lastWriteTime ?? 0)
            const instBlocksPerSec = cycleMs > 0 ? (batchSize * 1000) / cycleMs : 0
            stats.avgBlocksPerSec =
                stats.avgBlocksPerSec == null ? instBlocksPerSec : (stats.avgBlocksPerSec + instBlocksPerSec) / 2
        },
    })
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
