import {HttpClient} from '@sqd-sdk/core/http-client'
import {createLogger} from '@sqd-sdk/core/logger'
import {
    createTransformer,
    type Data,
    type DataBatch,
    type DataDuplex,
    type DataFactoryOptions,
    type DataFork,
    pipeline,
    createTarget,
} from '@sqd-sdk/core/pipeline'
import {PortalClient} from '@sqd-sdk/core/portal'
import {solanaPortalDataSource} from '@sqd-sdk/solana-stream'

async function main() {
    let portal = new PortalClient({
        url: 'https://portal.sqd.dev/datasets/solana-mainnet',
        http: new HttpClient({
            retryAttempts: Number.POSITIVE_INFINITY,
        }),
        minBytes: 100 * 1024 * 1024,
    })

    let head = await portal.getHead().then((h) => h?.number ?? 0)
    let fromBlock = head - 10_000
    let toBlock = undefined

    console.log(`processing range: [${fromBlock}, ${toBlock ?? null}]`)

    await pipeline(() =>
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
    )
        .pipeThrough(createProgressTracker('solana'))
        .pipeTo(({ref}) =>
            createTarget({
                unfinalized: true,
                writer: (opts) => {
                    return {
                        offset: undefined,
                        next: async (batch) => {
                            if (ref.compare(batch.offset, batch.head).isEqual) {
                                return {done: true, value: undefined}
                            }

                            return {done: false, value: {offset: batch.offset}}
                        },
                        fork: async (fork) => {
                            return {done: true, value: undefined}
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
>(prefix: string): (opts: DataFactoryOptions<T, TFinalized>) => Promise<DataDuplex<T, T, TFinalized, TFinalized>> {
    const logger = createLogger(`sqd:${prefix}`)

    return (opts) =>
        createTransformer({
            unfinalized: opts.unfinalized,
            ref: opts.ref,
            transformer: (opts) => {
                return {
                    offset: opts.offset,
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
                    fork: async (fork) => {
                        return {heads: fork.heads}
                    },
                }
            },
        })
}

main()
