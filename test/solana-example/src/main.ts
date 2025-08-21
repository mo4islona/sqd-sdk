import {HttpClient} from '@sqd-sdk/core/http-client'
import {assert} from '@sqd-sdk/core/internal/misc'
import {createLogger} from '@sqd-sdk/core/logger'
import {
    createTarget,
    createTransformer,
    handleMessage,
    pipeline,
    type Data,
    type DataDuplexFactory,
} from '@sqd-sdk/core/pipeline'
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

    await pipeline(
        solanaPortalDataSource({
            portal,
            query: {
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
                requests: [
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
            },
        }),
    )
        .pipeThrough(createProgressTracker('solana'))
        .pipeTo(
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

                            assert(srcMint)
                            assert(destMint)

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
    T extends Data<{header: {timestamp: number}}, {number: number}>,
    TUnfinalized extends boolean,
    TRequest,
>(prefix: string): DataDuplexFactory<T, T, TUnfinalized, TUnfinalized, TRequest, TRequest> {
    const logger = createLogger(`sqd:${prefix}`)

    return createTransformer(async (opts) => {
        return {
            unfinalized: opts.unfinalized,
            ref: opts.ref,
            transform: async function* (transformOpts) {
                if (transformOpts.offset) {
                    logger.info(`continue from ${transformOpts.offset.number}`)
                }

                for await (const message of transformOpts.read({
                    offset: transformOpts.offset,
                    request: transformOpts.request,
                })) {
                    switch (message.type) {
                        case 'batch':
                            if (message.value.data.length > 0) {
                                const {offset, head, finalizedHead, data} = message.value
                                logger.info(
                                    [
                                        `progress: ${offset.number} / ${head.number} (${finalizedHead?.number ?? 0})`,
                                        `blocks: ${message.value.data.length}, lag: ${(
                                            (Date.now() - data[data.length - 1].value.header.timestamp * 1000) / 1000
                                        ).toFixed(2)}s`,
                                    ].join(', '),
                                )
                            }
                            break
                        case 'fork':
                            logger.info(`fork: ${message.value.heads[message.value.heads.length - 1].number}`)
                            break
                    }

                    yield message
                }
            },
        }
    })
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
