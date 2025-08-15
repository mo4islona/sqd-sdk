// export function finalizer<T extends Data>(): DataDuplex<T, T, true, false> {
//     let buffer: T[] = []
//     const queue = new SyncQueue<DataBatch<T>>()
//     let offsetFuture: Future<T['id'] | undefined> = createFuture()

//     const target = new DataTarget<T, true>({
//         unfinalized: true,
//         writer: async (opts: DataWriterOptions<T>) => {
//             const offset = await offsetFuture.promise()
//             return {
//                 offset,
//                 async next(batch: DataBatch<T>) {
//                     buffer.push(...batch.data)

//                     if (batch.finalizedHead) {
//                         let unfinalizedIndex = 0
//                         for (; unfinalizedIndex < buffer.length; unfinalizedIndex++) {
//                             const ref = buffer[unfinalizedIndex].id
//                             if (batch.finalizedHead.compare(ref).isLess) break
//                         }

//                         const data = buffer.splice(0, unfinalizedIndex)
//                         if (data.length > 0) {
//                             const offset = data[data.length - 1].id
//                             await queue.put({
//                                 data,
//                                 offset,
//                                 finalizedHead: batch.finalizedHead,
//                                 head: batch.finalizedHead,
//                             })
//                         }
//                     }

//                     return batch.offset
//                 },
//                 async fork(fork: DataFork<T>): Promise<IteratorResult<T['id'] | undefined>> {
//                     const forkPoint = findFork(
//                         buffer.map((data) => data.id),
//                         fork.heads,
//                     )
//                     if (forkPoint === -1) throw new Error('Cannot process fork')
//                     buffer = buffer.slice(0, forkPoint + 1)

//                     return buffer[buffer.length - 1].id
//                 },
//                 async return(): Promise<IteratorResult<T['id'] | undefined>> {
//                     queue.close()
//                     return {done: true, value: undefined}
//                 },
//             }
//         },
//     })

//     const source = new DataSource<T, false>({
//         unfinalized: false,
//         reader: async (opts) => {
//             offsetFuture.resolve(opts.offset)

//             return {
//                 ref: {compare: (a, b) => DataRef.Greater},
//                 async next(): Promise<IteratorResult<DataBatch<T>>> {
//                     return await queue.take()
//                 },
//                 async close(): Promise<void> {
//                     queue.close()
//                     await target.close().catch(() => {})
//                 },
//             }
//         },
//     })

//     return {
//         target,
//         source,
//     }
// }

//function findFork(chainA: DataRef<any>[], chainB: DataRef<any>[]) {
//    let i = 0
//    let j = 0
//    for (; i < chainA.length; i++) {
//        const blockA = chainA[i]
//        for (; j < chainB.length; j++) {
//            let blockB = chainB[j]
//            if (blockB.compare(blockA).isGreater) break
//            if (blockB.compare(blockA).isFork) return i - 1
//        }
//        if (j === chainB.length) break
//    }
//    return i - 1
//}
