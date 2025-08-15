import {HttpClient} from '@sqd-sdk/core/http-client'
import {createLogger} from '@sqd-sdk/core/logger'
import {
    type Data,
    type DataBatch,
    type DataRef,
    DataTarget,
    transformer,
    type DataDuplexFactory,
    type DataDuplex,
    DataSource,
    DataTargetFactory,
} from '@sqd-sdk/core/pipeline'
import {BlockId, PortalClient} from '@sqd-sdk/core/portal'
import {type SolanaPortalData, solanaPortalDataSource} from '@sqd-sdk/solana-stream'

async function main() {
    let portal = new PortalClient({
        url: 'https://portal.sqd.dev/datasets/solana-mainnet',
        http: new HttpClient({
            retryAttempts: Number.POSITIVE_INFINITY,
        }),
        minBytes: 100 * 1024 * 1024,
    })

    let toBlock = await portal.getHead().then((h) => h?.number ?? 0)
    let fromBlock = toBlock - 50_000

    console.log(`processing range: [${fromBlock}, ${toBlock ?? null}]`)

    await solanaPortalDataSource({
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
                            },
                        ],
                    },
                },
            ],
        },
    })
        .pipeThrough(createProgressTracker('solana'))
        .pipeTo(
            (opts) =>
                new DataTarget({
                    unfinalized: true,
                    ref: opts.ref,
                    writer: async () => {
                        return {
                            next: async (batch) => {
                                return {done: false, value: batch?.offset}
                            },
                            fork: async (fork) => {
                                return {done: false, value: fork.heads[fork.heads.length - 1]}
                            },
                        }
                    },
                })
        )

    console.log('end')
}

interface StateManager<T extends Data<any, any>> {
    get(): Promise<T['id'] | undefined>
    set(ref: T['id']): Promise<void>
    fork(refs: T['id'][]): Promise<T['id'] | undefined>
}

// function createStateTarget<T extends Data<any, any>>(opts: {
//     state: StateManager<T>
//     transact: (batch: DataBatch<T>) => Promise<unknown>
//     rollback: (block: DataRef<T>) => Promise<unknown>
// }): DataTargetFactory<T, true> {
//     const {state, transact, rollback} = opts

//     return (opts) =>
//         new DataTarget({
//             unfinalized: opts.unfinalized,
//             ref: opts.ref,
//             writer: async () => {
//                 const head = await state.get()
//                 if (head) {
//                     await rollback(head)
//                 }

//                 return {
//                     offset: head,
//                     next: async (batch) => {
//                         await transact(batch)

//                         if (batch.data.length > 0) {
//                             await state.set(batch.data[batch.data.length - 1].id)
//                         }

//                         return {done: false, value: batch.offset}
//                     },
//                     fork: async (fork) => {
//                         const newHead = await state.fork(fork.heads)
//                         if (newHead) {
//                             await rollback(newHead)
//                         }
//                         return {done: false, value: newHead}
//                     },
//                 }
//             },
//         })
// }

function createProgressTracker<
    T extends Data<{header: {timestamp: number}}, {number: number}>,
    TFinalized extends boolean
>(prefix: string): DataDuplexFactory<T, T, TFinalized, TFinalized> {
    const logger = createLogger(`sqd:${prefix}`)

    return transformer({
        transformer: async (opts) => {
            return {
                offset: opts.offset,
                ref: opts.ref,
                transform: async (batch) => {
                    if (batch.data.length > 0) {
                        const {offset, head, finalizedHead, data} = batch
                        logger.info(
                            [
                                `progress: ${offset.number} / ${head.number} (${finalizedHead?.number ?? 0})`,
                                `blocks: ${batch.data.length}, lag: ${(
                                    (Date.now() - data[data.length - 1].value.header.timestamp * 1000) /
                                    1000
                                ).toFixed(2)}s`,
                            ].join(', ')
                        )
                    }
                    return batch
                },
                fork: async (fork) => fork,
            }
        },
    })
}

main()
