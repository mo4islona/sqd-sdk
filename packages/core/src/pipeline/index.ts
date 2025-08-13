import {createFuture, type Future, SyncQueue} from '../internal/async'
import {isForkException} from './errors'
import {type Data, type DataBatch, type DataFork, DataRef} from './data'

export * from './errors'
export * from './data'

export interface DataReaderOptions<TData extends Data> {
    offset?: TData['id'] | undefined
}

export interface DataReader<TData extends Data> extends AsyncIterator<DataBatch<TData>> {
    readonly ref: DataRef<TData['id']>
}

export namespace DataReader {
    export const fromAsync = <TData extends Data>(
        asyncIterator: AsyncIterable<DataBatch<TData>>,
        ref: DataRef<TData['id']>,
    ): DataReader<TData> => {
        const iterator = asyncIterator[Symbol.asyncIterator]()

        // FIXME: should not define methods if they don't exist
        return {
            ref,
            next: async () => {
                const {done, value} = await iterator.next()
                return {done, value}
            },
            return: async () => {
                await iterator.return?.()
                return {done: true, value: undefined}
            },
            throw: async (err) => {
                await iterator.throw?.(err)
                return {done: true, value: undefined}
            },
        }
    }
}

export interface DataStream<TData extends Data> extends AsyncIterableIterator<DataBatch<TData>> {
    readonly ref: DataRef<TData['id']>
}

export interface DataSource<TData extends Data, TUnfinalized extends boolean> extends AsyncIterable<TData['value']> {
    readonly unfinalized: TUnfinalized
    read(opts: DataReaderOptions<TData>): DataStream<TData>
    pipeThrough<UData extends Data, UUnfinalized extends boolean>(
        duplex: PipableThrough<TData, UData, TUnfinalized extends true ? true : boolean, UUnfinalized>,
    ): DataSource<UData, UUnfinalized>
    pipeTo(target: DataTarget<TData, TUnfinalized extends true ? true : boolean>): Promise<void>
    close(): Promise<void>
}

export interface DataWriterOptions<TData extends Data> {
    read: (opts: DataReaderOptions<TData>) => DataStream<TData>
}

export interface DataWriterWriteOptions<TData extends Data> extends DataReaderOptions<TData> {}

export interface FinalizedDataWriter<TData extends Data, TReturn = unknown> {
    offset: TData['id'] | undefined
    next(batch: DataBatch<TData>): Promise<IteratorResult<TData['id'] | undefined, TReturn>>
    return?(): Promise<IteratorReturnResult<TReturn>>
    fork?(fork: DataFork<TData['id']>): Promise<IteratorResult<TData['id'] | undefined, TReturn>>
}

export interface UnfinalizedDataWriter<TData extends Data, TReturn = unknown>
    extends FinalizedDataWriter<TData, TReturn> {
    fork?(fork: DataFork<TData['id']>): Promise<IteratorResult<TData['id'] | undefined, TReturn>>
}

export type DataWriter<TData extends Data, TUnfinalized extends boolean> = TUnfinalized extends false
    ? UnfinalizedDataWriter<TData>
    : FinalizedDataWriter<TData>

export interface DataTarget<TData extends Data, TUnfinalized extends boolean> {
    readonly unfinalized: TUnfinalized
    write(opts: DataWriterOptions<TData>): Promise<void>
    close(): Promise<void>
}

export interface DataDuplex<
    TData extends Data,
    UData extends Data,
    TUnfinalized extends boolean,
    UUnfinalized extends boolean,
> {
    target: DataTarget<TData, TUnfinalized>
    source: DataSource<UData, UUnfinalized>
}

export type DataDuplexFactory<
    TData extends Data,
    UData extends Data,
    TUnfinalized extends boolean,
    UUnfinalized extends boolean,
> = (opts: {unfinalized: TUnfinalized}) => DataDuplex<TData, UData, TUnfinalized, UUnfinalized>

export type PipableThrough<
    TData extends Data,
    UData extends Data,
    TUnfinalized extends boolean,
    UUnfinalized extends boolean,
> = DataDuplex<TData, UData, TUnfinalized, UUnfinalized> | DataDuplexFactory<TData, UData, TUnfinalized, UUnfinalized>

async function pipe<TData extends Data, TUnfinalized extends boolean>(
    source: DataSource<TData, TUnfinalized>,
    target: DataTarget<TData, TUnfinalized extends true ? true : boolean>,
): Promise<void> {
    if (source.unfinalized && !target.unfinalized) {
        throw new TypeError('Cannot pipe from unfinalized DataSource to finalized DataTarget')
    }

    try {
        return await target.write({
            read: (opts) => source.read(opts),
        })
    } finally {
        await source.close?.().catch(() => {})
        await target.close?.().catch(() => {})
    }
}

export interface DataSourceConfig<T extends Data, TUnfinalized extends boolean = true> {
    reader: (opts: DataReaderOptions<T>) => PromiseLike<DataReader<T>>
    unfinalized?: TUnfinalized
}

export const DataSource: {
    new <TData extends Data, TUnfinalized extends boolean>(
        config: DataSourceConfig<TData, TUnfinalized>,
    ): DataSource<TData, TUnfinalized>
} = class<TData extends Data, TUnfinalized extends boolean> implements DataSource<TData, TUnfinalized> {
    readonly unfinalized: TUnfinalized

    private _state: 'opened' | 'locked' | 'closed' = 'opened'
    private _abortController: AbortController | undefined
    private _reader: (opts: DataReaderOptions<TData>) => PromiseLike<DataReader<TData>>
    private _closePromise: Promise<void> | undefined

    constructor(config: DataSourceConfig<TData, TUnfinalized>) {
        // NOTE: satisfy compiler
        if (config.unfinalized == null) {
            this.unfinalized = true as TUnfinalized
        } else {
            this.unfinalized = config.unfinalized
        }
        this._reader = config.reader
    }

    read(opts: DataReaderOptions<TData> = {}): DataStream<TData> {
        if (this._state === 'closed') {
            throw new Error('DataSource is already closed')
        }

        if (this._state === 'locked') {
            throw new Error('DataSource is locked')
        }
        this._state = 'locked'

        this._abortController = new AbortController()

        let reader: DataReader<TData> | undefined

        return {
            ref: {compare: (a, b) => DataRef.Greater},
            next: async (): Promise<IteratorResult<DataBatch<TData>>> => {
                if (this._abortController?.signal.aborted) {
                    await reader?.return?.().catch(() => {})
                    if (this._state === 'locked') {
                        this._state = 'opened'
                    }
                    throw this._abortController.signal.reason
                }

                if (!reader) {
                    reader = await this._reader(opts)
                }

                try {
                    return await reader.next()
                } catch (err) {
                    if (!isForkException<TData>(err)) throw err
                    if (!this.unfinalized) {
                        throw new TypeError('Got fork exception in finalized DataSource')
                    }
                    throw err
                }
            },
            return: async (): Promise<IteratorResult<DataBatch<TData>>> => {
                await reader?.return?.().catch(() => {})
                this._abortController = undefined
                if (this._state === 'locked') {
                    this._state = 'opened'
                }
                return {done: true, value: undefined}
            },
            throw: async (err) => {
                await reader?.return?.().catch(() => {})
                this._abortController = undefined
                if (this._state === 'locked') {
                    this._state = 'opened'
                }
                throw err
            },
            [Symbol.asyncIterator]() {
                return this
            },
        }
    }

    pipeThrough<UData extends Data, UUnfinalized extends boolean>(
        duplex: PipableThrough<TData, UData, TUnfinalized extends true ? true : boolean, UUnfinalized>,
    ): DataSource<UData, UUnfinalized> {
        if (typeof duplex === 'function') {
            duplex = duplex({unfinalized: this.unfinalized as any}) // FIXME: how to type this?
        }

        pipe(this, duplex.target).catch((err) => {
            throw err
        })
        return duplex.source
    }

    pipeTo(target: DataTarget<TData, TUnfinalized extends true ? true : boolean>): Promise<void> {
        return pipe(this, target)
    }

    async close(reason?: any): Promise<void> {
        if (this._state === 'closed') {
            return this._closePromise
        }

        this._closePromise = this._closePromise || this._performClose(reason)
        return this._closePromise
    }

    private async _performClose(reason?: any): Promise<void> {
        this._state = 'closed'
        this._abortController?.abort(reason)
    }

    async *[Symbol.asyncIterator](): AsyncIterableIterator<TData['value']> {
        for await (const batch of this.read()) {
            for (const data of batch.data) {
                // FIXME: how to type this?
                yield data.value as TData['value']
            }
        }
    }
}

// FIXME: which approach is better: function or class?
export function source<TData extends Data, TUnfinalized extends boolean>(
    config: DataSourceConfig<TData, TUnfinalized>,
): DataSource<TData, TUnfinalized> {
    return new DataSource(config)
}

export interface DataTargetConfig<TData extends Data, TUnfinalized extends boolean = true> {
    writer: (opts: DataWriterOptions<TData>) => PromiseLike<DataWriter<TData, NoInfer<TUnfinalized>>>
    unfinalized?: TUnfinalized
}

// NOTE: workaround to allow constructor overloading
export const DataTarget: {
    new <TData extends Data, TUnfinalized extends boolean>(
        config: DataTargetConfig<TData, TUnfinalized>,
    ): DataTarget<TData, TUnfinalized>
} = class<TData extends Data, TUnfinalized extends boolean> implements DataTarget<TData, TUnfinalized> {
    readonly unfinalized: TUnfinalized

    private _state: 'opened' | 'locked' | 'closed' = 'opened'
    private _abortController: AbortController | undefined
    private _writer: (
        opts: DataWriterOptions<TData>,
    ) => PromiseLike<FinalizedDataWriter<TData> | UnfinalizedDataWriter<TData>>
    private _closePromise: Promise<void> | undefined

    constructor(config: DataTargetConfig<TData, TUnfinalized>) {
        // NOTE: satisfy compiler
        if (config.unfinalized == null) {
            this.unfinalized = true as TUnfinalized
        } else {
            this.unfinalized = config.unfinalized
        }
        this._writer = config.writer
    }

    async write(opts: DataWriterOptions<TData>): Promise<void> {
        if (this._state === 'closed') {
            throw new Error('DataTarget is closed')
        }

        if (this._state === 'locked') {
            throw new Error('DataTarget is already locked')
        }
        this._state = 'locked'
        this._abortController = new AbortController()

        const writer = await this._writer(opts)
        const processData = async (offset: TData['id'] | undefined): Promise<unknown> => {
            this._abortController?.signal.throwIfAborted()

            const stream = opts.read({offset})
            return processStream(stream, offset)
        }

        const processStream = async (
            stream: DataStream<TData>,
            currentOffset: TData['id'] | undefined,
        ): Promise<unknown> => {
            this._abortController?.signal.throwIfAborted()

            let batch: DataBatch<TData> | undefined
            try {
                const {done, value} = await stream.next()
                if (done) return value
                batch = value
            } catch (err) {
                // FIXME: do we need to return?
                await stream.return?.().catch(() => {})

                if (!isForkException<TData>(err)) {
                    throw err
                }
                if (!this.unfinalized) {
                    throw new TypeError('Got fork exception in finalized DataTarget')
                }
                if (!writer.fork) {
                    throw new TypeError('Missing fork method in unfinalized DataWriter')
                }

                const {value, done} = await writer.fork(err.fork)
                if (done) return value
                return processData(value)
            }
            validateBatch(stream.ref, currentOffset, batch)

            const newOffset = await writer.next(batch)
            // NOTE: If the offset is not the same as the batch offset,
            // it means that the batch was not fully consumed or we want to skip
            // so we break the current stream and start from the new offset
            // FIXME: Do we want this behavior?
            if (!newOffset || !stream.ref.compare(newOffset, batch.offset).isEqual) {
                await stream.return?.().catch(() => {})
                return processData(newOffset)
            }

            return processStream(stream, newOffset)
        }

        try {
            await processData(writer.offset)
        } finally {
            await writer.return?.().catch(() => {})
            this._abortController = undefined
            if (this._state === 'locked') {
                this._state = 'opened'
            }
        }
    }

    async close(reason?: any): Promise<void> {
        if (this._state === 'closed') {
            return this._closePromise
        }

        this._closePromise = this._closePromise || this._performClose(reason)
        return this._closePromise
    }

    private async _performClose(reason?: any): Promise<void> {
        this._state = 'closed'
        this._abortController?.abort(reason)
    }
}

// FIXME: which approach is better: function or class?
export function target<TData extends Data, TUnfinalized extends boolean>(
    config: DataTargetConfig<TData, TUnfinalized>,
): DataTarget<TData, TUnfinalized> {
    return new DataTarget(config)
}

export interface DataTransformer<TData extends Data, UData extends Data> {
    offset: TData['id'] | undefined
    ref: DataRef<UData['id']>
    transform: (batch: DataBatch<TData>) => Promise<DataBatch<UData>>
    fork?: (fork: DataFork<TData>) => Promise<DataFork<UData> | undefined>
}

export interface TransformerOptions<TData extends Data> {
    offset: TData['id'] | undefined
}

export interface TransformerConfig<TData extends Data, UData extends Data> {
    transformer: (opts: TransformerOptions<UData>) => PromiseLike<DataTransformer<TData, UData>>
}

export function transformer<TData extends Data, UData extends Data, TUnfinalized extends boolean>(
    config: TransformerConfig<TData, UData>,
): DataDuplexFactory<TData, UData, TUnfinalized, TUnfinalized> {
    return (parent) => {
        const queue = new SyncQueue<DataBatch<UData>>()
        let offsetFuture: Future<UData['id'] | undefined> = createFuture()
        let refFuture: Future<DataRef<UData['id']>> = createFuture()

        const target = new DataTarget<TData, TUnfinalized>({
            unfinalized: parent.unfinalized,
            writer: async (opts: DataWriterOptions<TData>) => {
                const transformer = await config.transformer({
                    offset: await offsetFuture.promise(),
                })

                refFuture.resolve(transformer.ref)

                if (parent.unfinalized && !transformer.fork) {
                    throw new TypeError('Missing fork method in unfinalized DataTransformer')
                }

                return {
                    offset: transformer.offset,
                    async next(batch: DataBatch<TData>) {
                        if (queue.isClosed) return {done: true, value: undefined}

                        const data = await transformer.transform(batch)
                        await queue.put(data)
                        return {done: false, value: batch.offset}
                    },
                    async fork(fork) {
                        return {done: true, value: undefined}
                    },
                    async return() {
                        queue.close()
                        return {done: true, value: undefined}
                    },
                }
            },
        })

        const source = new DataSource<UData, TUnfinalized>({
            unfinalized: parent.unfinalized,
            reader: async (opts) => {
                offsetFuture.resolve(opts.offset)

                const ref = await refFuture.promise()

                return {
                    ref,
                    async next(): Promise<IteratorResult<DataBatch<UData>>> {
                        if (queue.isClosed) return {done: true, value: undefined}

                        const value = await queue.take()
                        return value ? {done: false, value} : {done: true, value: undefined}
                    },
                    async return(): Promise<IteratorResult<DataBatch<UData>>> {
                        queue.close()
                        return {done: true, value: undefined}
                    },
                }
            },
        })

        // FIXME: how to type this?
        return {
            target,
            source,
        }
    }
}

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

//function validateContinuity<TData extends Data>(offset: TData['id'] | undefined, batch: DataBatch<TData>) {
//    let last = offset
//    for (const item of batch.data) {
//        if (last && item.id.compare(last).isGreater) {
//            throw new Error('Item is below the previous item')
//        }
//        last = item.id
//    }
//}

function validateBatch<TData extends Data>(
    ref: DataRef<TData['id']>,
    offset: TData['id'] | undefined,
    batch: DataBatch<TData>,
) {
    if (offset && ref.compare(batch.offset, offset).isLess) {
        throw new Error('New offset is below the previous offset')
    }

    for (const item of batch.data) {
        if (offset && ref.compare(item.id, offset).isLessOrEqual) {
            throw new Error('Item is below or equal to the previous item')
        }
        offset = item.id
    }

    if (offset && ref.compare(batch.head, offset).isLess) {
        throw new Error('Head is below the data')
    }

    if (batch.finalizedHead && ref.compare(batch.head, batch.finalizedHead).isLess) {
        throw new Error('Head is below the finalized head')
    }

    if (ref.compare(batch.head, batch.offset).isLess) {
        throw new Error('Head is below the offset')
    }
}
