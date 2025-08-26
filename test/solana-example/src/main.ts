import {HttpClient} from '@sqd-sdk/core/http-client'
import {assert} from '@sqd-sdk/core/internal/misc'
import {createLogger} from '@sqd-sdk/core/logger'
import {createStream, createTracker, createMapper} from '@sqd-sdk/core/pipeline'
import {PortalClient} from '@sqd-sdk/core/portal'
import {solanaPortalDataSource} from '@sqd-sdk/solana-stream'
import {createTypeormTarget} from '@sqd-sdk/typeorm-store/lib/database'
import * as tokenProgram from './abi/token-program'
import * as whirlpool from './abi/whirlpool'
import {Exchange} from './model'

async function main() {
    let portal = new PortalClient({
        url: 'https://portal.sqd.dev/datasets/solana-mainnet',
        http: new HttpClient({
            retryAttempts: Number.POSITIVE_INFINITY,
        }),
        minBytes: 100 * 1024 * 1024,
    })

    let head = await portal.getHead().then((h) => h?.number ?? 0)
    let fromBlock = head - 100_000
    let toBlock = undefined

    console.log(`processing range: [${fromBlock}, ${toBlock ?? null}]`)

    await createStream(() =>
        solanaPortalDataSource({
            portal,
            fields: {
                block: {number: true, timestamp: true, hash: true, parentHash: true},
                transaction: {signatures: true, err: true, transactionIndex: true},
                instruction: {
                    programId: true,
                    accounts: true,
                    data: true,
                    isCommitted: true,
                    transactionIndex: true,
                    instructionAddress: true,
                },
                tokenBalance: {
                    account: true,
                    preMint: true,
                    preOwner: true,
                    preAmount: true,
                    postMint: true,
                    postOwner: true,
                    postAmount: true,
                },
            },
            request: [
                {
                    range: {from: fromBlock, to: toBlock},
                    request: {
                        instructions: [
                            {
                                programId: ['whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'],
                                d8: ['0xf8c69e91e17587c8'],
                                isCommitted: true,
                                innerInstructions: true,
                                transaction: true,
                                transactionTokenBalances: true,
                            },
                        ],
                    },
                },
            ],
        }),
    )
        //.pipe(createFinalizer())
        .pipe(
            createMapper((block) => {
                return {
                    ...block,
                    mapped: true,
                }
            }),
        )
        .pipe(createProgressTracker('solana'))
        .pipe(
            createTypeormTarget({}, async (store, batch) => {
                for (let block of batch) {
                    for (let ins of block.instructions) {
                        if (ins.programId === whirlpool.programId && ins.d8 === whirlpool.instructions.swap.d8) {
                            let exchange = new Exchange({
                                id: formatId(block.header, ins.transactionIndex, ...ins.instructionAddress),
                                slot: block.header.number,
                                tx: ins.transaction?.signatures[0] ?? 'null',
                                timestamp: new Date(block.header.timestamp * 1000),
                            })

                            assert(ins.inner.length === 2)
                            let srcTransfer = tokenProgram.transfer.decode(ins.inner[0])
                            let destTransfer = tokenProgram.transfer.decode(ins.inner[1])

                            let srcBalance = ins.transaction?.tokenBalances.find(
                                (tb) => tb.account === srcTransfer.accounts.source,
                            )
                            let destBalance = ins.transaction?.tokenBalances.find(
                                (tb) => tb.account === destTransfer.accounts.destination,
                            )

                            let srcMint = ins.transaction?.tokenBalances.find(
                                (tb) => tb.account === srcTransfer.accounts.destination,
                            )?.preMint
                            let destMint = ins.transaction?.tokenBalances.find(
                                (tb) => tb.account === destTransfer.accounts.source,
                            )?.preMint

                            assert(srcMint != null)
                            assert(destMint != null)

                            exchange.fromToken = srcMint
                            exchange.fromOwner = srcBalance?.preOwner || srcTransfer.accounts.source
                            exchange.fromAmount = srcTransfer.data.amount

                            exchange.toToken = destMint
                            exchange.toOwner =
                                destBalance?.postOwner || destBalance?.preOwner || destTransfer.accounts.destination
                            exchange.toAmount = destTransfer.data.amount

                            await store.insert(exchange)
                        }
                    }
                }
            }),
        )

    console.log('end')
}

function createProgressTracker<
    TCursor extends {number: number; hash: string},
    TValue extends {header: {timestamp: number}},
    TRequest,
>(prefix: string) {
    const logger = createLogger(`sqd:${prefix}`)
    const readTimer = createTimer()
    const writeTimer = createTimer()

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
          }
        | undefined = undefined

    return createTracker<TCursor, TValue, TRequest>({
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
                }
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
            stats.head = message.cursor
            stats.finalizedHead = message.finalizedHead
            stats.cursor = message.cursor

            logger.info(
                {
                    lag: `${(Date.now() - (stats.lastBlockTime ?? 0) / 1000).toFixed(2)}s`,
                    avgReadTime: `${(stats.avgReadTime ?? 0 / 1000).toFixed(2)}s`,
                    lastReadTime: `${(stats.lastReadTime ?? 0 / 1000).toFixed(2)}s`,
                    avgWriteTime: `${(stats.avgWriteTime ?? 0 / 1000).toFixed(2)}s`,
                    lastWriteTime: `${(stats.lastWriteTime ?? 0 / 1000).toFixed(2)}s`,
                },
                `progress: ${stats.cursor.number} / ${stats.head.number} (${stats.finalizedHead?.number ?? 0})`,
            )
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
