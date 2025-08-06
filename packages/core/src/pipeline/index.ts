import {createFuture, type Future, SyncQueue} from '../internal/async'
import {isForkException} from './errors'
import type {Data, DataBatch, DataFork} from './data'

export * from './errors'
export * from './data'

export interface DataReaderOptions<TData extends Data> {
    offset?: TData['ref'] | undefined
}

export interface DataReader<TData extends Data> {
    read(): Promise<DataBatch<TData> | undefined>
    close?(): Promise<unknown>
}

export type DataSource<TData extends Data = Data, TFinalized extends boolean = boolean> = TFinalized extends true
    ? FinalizedDataSource<TData>
    : TFinalized extends false
      ? UnfinalizedDataSource<TData>
      : FinalizedDataSource<TData> | UnfinalizedDataSource<TData>

export interface FinalizedDataSource<TData extends Data> extends AsyncIterable<TData['value']> {
    readonly finalized: true
    read(opts: DataReaderOptions<TData>): AsyncIterable<DataBatch<TData>>
    pipeThrough<UData extends Data, UFinalized extends boolean>(
        duplex: PipableThrough<TData, UData, true, UFinalized>,
    ): DataSource<UData, UFinalized>
    pipeTo(target: DataTarget<TData>): Promise<void>
    close(): Promise<void>
}

export interface UnfinalizedDataSource<TData extends Data> extends AsyncIterable<TData['value']> {
    readonly finalized: false
    read(opts: DataReaderOptions<TData>): AsyncIterable<DataBatch<TData>>
    pipeThrough<UData extends Data, UFinalized extends boolean>(
        duplex: PipableThrough<TData, UData, false, UFinalized>,
    ): DataSource<UData, UFinalized>
    pipeTo(target: UnfinalizedDataTarget<TData>): Promise<void>
    close(): Promise<void>
}

export interface DataWriterOptions<TData extends Data> {
    read: (opts: DataReaderOptions<TData>) => AsyncIterable<DataBatch<TData>>
}

// biome-ignore lint/suspicious/noEmptyInterface: <explanation>
export interface DataWriterWriteOptions<TData extends Data> {}

export interface FinalizedDataWriter<TData extends Data> {
    readonly offset: TData['ref'] | undefined
    write(batch: DataBatch<TData>, offset: TData['ref'] | undefined): Promise<TData['ref']>
    fork?(fork: DataFork<TData>, offset: TData['ref'] | undefined): Promise<TData['ref'] | undefined>
    close?(): Promise<unknown>
}

export interface UnfinalizedDataWriter<TData extends Data> extends FinalizedDataWriter<TData> {
    fork(fork: DataFork<TData>, offset: TData['ref'] | undefined): Promise<TData['ref'] | undefined>
}

export type DataWriter<TData extends Data> = FinalizedDataWriter<TData> | UnfinalizedDataWriter<TData>

export type DataTarget<TData extends Data = Data, TFinalized extends boolean = boolean> = TFinalized extends true
    ? FinalizedDataTarget<TData>
    : TFinalized extends false
      ? UnfinalizedDataTarget<TData>
      : UnfinalizedDataTarget<TData> | FinalizedDataTarget<TData>

export interface UnfinalizedDataTarget<TData extends Data> {
    readonly finalized: false
    write(opts: DataWriterOptions<TData>): Promise<void>
    close(): Promise<void>
}

export interface FinalizedDataTarget<TData extends Data> {
    readonly finalized: true
    write(opts: DataWriterOptions<TData>): Promise<void>
    close(): Promise<void>
}

export interface DataDuplex<
    TData extends Data = Data,
    UData extends Data = Data,
    TFinalized extends boolean = boolean,
    UFinalized extends boolean = boolean,
> {
    target: DataTarget<TData, TFinalized>
    source: DataSource<UData, UFinalized>
}

export type DataDuplexFactory<
    TData extends Data,
    UData extends Data,
    TFinalized extends boolean = boolean,
    UFinalized extends boolean = boolean,
> = (opts: {finalized: TFinalized}) => DataDuplex<TData, UData, TFinalized, UFinalized>

export type PipableThrough<
    TData extends Data,
    UData extends Data,
    TFinalized extends boolean = boolean,
    UFinalized extends boolean = boolean,
> = DataDuplex<TData, UData, TFinalized, UFinalized> | DataDuplexFactory<TData, UData, TFinalized, UFinalized>

async function pipe<TData extends Data>(source: DataSource<TData>, target: DataTarget<TData>): Promise<void> {
    if (!source.finalized && target.finalized) {
        throw new TypeError('Cannot pipe from unfinalized DataSource to finalized DataTarget')
    }

    try {
        await target.write(source)
    } finally {
        await source.close?.().catch(() => {})
        await target.close?.().catch(() => {})
    }
}

export interface FinalizedDataSourceConfig<T extends Data> {
    reader: (opts: DataReaderOptions<T>) => PromiseLike<DataReader<T>>
    finalized: true
}

export interface UnfinalizedDataSourceConfig<T extends Data> {
    reader: (opts: DataReaderOptions<T>) => PromiseLike<DataReader<T>>
    finalized?: false
}

// NOTE: workaround to allow constructor overloading
export const DataSource: {
    new <TData extends Data>(config: UnfinalizedDataSourceConfig<TData>): UnfinalizedDataSource<TData>
    new <TData extends Data>(config: FinalizedDataSourceConfig<TData>): FinalizedDataSource<TData>
    new <TData extends Data, TFinalized extends boolean>(
        config: FinalizedDataSourceConfig<TData> | UnfinalizedDataSourceConfig<TData>,
    ): DataSource<TData, TFinalized>
} = class<TData extends Data> {
    readonly finalized: any // FIXME: how to type this?

    private _state: 'opened' | 'locked' | 'closed' = 'opened'
    private _abortController: AbortController | undefined
    private _reader: (opts: DataReaderOptions<TData>) => PromiseLike<DataReader<TData>>
    private _closePromise: Promise<void> | undefined

    constructor(config: FinalizedDataSourceConfig<TData> | UnfinalizedDataSourceConfig<TData>) {
        const {reader, finalized} = config

        this.finalized = finalized === true
        this._reader = reader
    }

    read(opts: DataReaderOptions<TData> = {}): AsyncIterable<DataBatch<TData>> {
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
            [Symbol.asyncIterator]: () => ({
                next: async (): Promise<IteratorResult<DataBatch<TData>>> => {
                    if (this._abortController?.signal.aborted) {
                        await reader?.close?.().catch(() => {})
                        if (this._state === 'locked') {
                            this._state = 'opened'
                        }
                        throw this._abortController.signal.reason
                    }

                    if (!reader) {
                        reader = await this._reader(opts)
                    }

                    try {
                        const batch = await reader.read()
                        if (!batch) {
                            return {done: true, value: undefined}
                        }
                        return {done: false, value: batch}
                    } catch (err) {
                        if (!isForkException<TData>(err)) throw err
                        if (this.finalized) {
                            throw new TypeError('Got fork exception in finalized DataSource')
                        }
                        throw err
                    }
                },
                return: async (): Promise<IteratorResult<DataBatch<TData>>> => {
                    await reader?.close?.().catch(() => {})
                    this._abortController = undefined
                    if (this._state === 'locked') {
                        this._state = 'opened'
                    }
                    return {done: true, value: undefined}
                },
                throw: async (err) => {
                    await reader?.close?.().catch(() => {})
                    this._abortController = undefined
                    if (this._state === 'locked') {
                        this._state = 'opened'
                    }
                    throw err
                },
            }),
        }
    }

    pipeThrough<UData extends Data, UFinalized extends boolean>(
        duplex: PipableThrough<TData, UData, boolean, UFinalized>,
    ): DataSource<UData, UFinalized> {
        if (typeof duplex === 'function') {
            duplex = duplex(this)
        }

        pipe(this, duplex.target).catch((err) => {
            throw err
        })
        return duplex.source
    }

    pipeTo(target: DataTarget<TData>): Promise<void> {
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
export function source<TData extends Data>(config: UnfinalizedDataSourceConfig<TData>): DataSource<TData, false>
export function source<TData extends Data>(config: FinalizedDataSourceConfig<TData>): DataSource<TData, true>
export function source<TData extends Data>(
    config: FinalizedDataSourceConfig<TData> | UnfinalizedDataSourceConfig<TData>,
): DataSource<TData> {
    return new DataSource(config)
}

export interface UnfinalizedDataTargetConfig<TData extends Data> {
    writer: (opts: DataWriterOptions<TData>) => PromiseLike<UnfinalizedDataWriter<TData>>
    finalized?: false
}

export interface FinalizedDataTargetConfig<TData extends Data> {
    writer: (opts: DataWriterOptions<TData>) => PromiseLike<FinalizedDataWriter<TData>>
    finalized: true
}

function validateBatch<TData extends Data>(offset: TData['ref'] | undefined, batch: DataBatch<TData>) {
    if (offset && batch.offset.compare(offset).isLess) {
        throw new Error('New offset is below the previous offset')
    }

    for (const item of batch.data) {
        if (offset && item.ref.compare(offset).isLessOrEqual) {
            throw new Error('Item is below or equal to the previous item')
        }
        offset = item.ref
    }

    if (offset && batch.head.compare(offset).isLess) {
        throw new Error('Head is below the data')
    }

    if (batch.finalizedHead && batch.head.compare(batch.finalizedHead).isLess) {
        throw new Error('Head is below the finalized head')
    }

    if (batch.head.compare(batch.offset).isLess) {
        throw new Error('Head is below the offset')
    }
}

// NOTE: workaround to allow constructor overloading
export const DataTarget: {
    new <TData extends Data>(config: UnfinalizedDataTargetConfig<TData>): DataTarget<TData, false>
    new <TData extends Data>(config: FinalizedDataTargetConfig<TData>): DataTarget<TData, true>
    new <TData extends Data>(
        config: FinalizedDataTargetConfig<TData> | UnfinalizedDataTargetConfig<TData>,
    ): DataTarget<TData>
} = class<TData extends Data> {
    readonly finalized: any // FIXME: how to type this?

    private _state: 'opened' | 'locked' | 'closed' = 'opened'
    private _abortController: AbortController | undefined
    private _writer: (
        opts: DataWriterOptions<TData>,
    ) => PromiseLike<FinalizedDataWriter<TData> | UnfinalizedDataWriter<TData>>
    private _closePromise: Promise<void> | undefined

    constructor(config: FinalizedDataTargetConfig<TData> | UnfinalizedDataTargetConfig<TData>) {
        this.finalized = config.finalized === true

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
        const processData = async (offset: TData['ref'] | undefined): Promise<void> => {
            this._abortController?.signal.throwIfAborted()

            const stream = opts.read({offset})[Symbol.asyncIterator]()
            return processStream(stream, writer.offset)
        }

        const processStream = async (
            stream: AsyncIterator<DataBatch<TData>>,
            currentOffset: TData['ref'] | undefined,
        ): Promise<void> => {
            this._abortController?.signal.throwIfAborted()

            let batch: DataBatch<TData> | undefined
            try {
                const {done, value} = await stream.next()
                if (done) return
                batch = value
            } catch (err) {
                // FIXME: do we need to return?
                await stream.return?.().catch(() => {})

                if (!isForkException<TData>(err)) {
                    throw err
                }
                if (this.finalized) {
                    throw new TypeError('Got fork exception in finalized DataTarget')
                }
                if (!writer.fork) {
                    throw new TypeError('Missing fork method in unfinalized DataWriter')
                }

                const newOffset = await writer.fork(err.fork, writer.offset)
                return processData(newOffset)
            }
            validateBatch(currentOffset, batch)

            const newOffset = await writer.write(batch, currentOffset)
            // NOTE: If the offset is not the same as the batch offset,
            // it means that the batch was not fully consumed or we want to skip
            // so we break the current stream and start from the new offset
            // FIXME: Do we want this behavior?
            if (!newOffset || !newOffset.compare(batch.offset).isEqual) {
                await stream.return?.().catch(() => {})
                return processData(newOffset)
            }

            return processStream(stream, newOffset)
        }

        try {
            await processData(writer.offset)
        } finally {
            await writer.close?.().catch(() => {})
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
export function target<TData extends Data>(config: UnfinalizedDataTargetConfig<TData>): UnfinalizedDataTarget<TData>
export function target<TData extends Data>(config: FinalizedDataTargetConfig<TData>): FinalizedDataTarget<TData>
export function target<TData extends Data>(
    config: FinalizedDataTargetConfig<TData> | UnfinalizedDataTargetConfig<TData>,
): DataTarget<TData>
export function target<TData extends Data>(
    config: FinalizedDataTargetConfig<TData> | UnfinalizedDataTargetConfig<TData>,
): DataTarget<TData> {
    return new DataTarget(config)
}

export interface DataTransformer<TData extends Data, UData extends Data> {
    offset: TData['ref'] | undefined
    transform: (batch: DataBatch<TData>) => Promise<DataBatch<UData>>
    fork?: (fork: DataFork<TData>) => Promise<DataFork<UData> | undefined>
}

export interface TransformerOptions<TData extends Data> {
    offset: TData['ref'] | undefined
}

export interface TransformerConfig<TData extends Data, UData extends Data> {
    transformer: (opts: TransformerOptions<UData>) => PromiseLike<DataTransformer<TData, UData>>
}

export function transformer<TData extends Data, UData extends Data, TFinalized extends boolean>(
    config: TransformerConfig<TData, UData>,
): DataDuplexFactory<TData, UData, TFinalized, TFinalized> {
    return (parent) => {
        const queue = new SyncQueue<DataBatch<UData>>()
        let offsetFuture: Future<UData['ref'] | undefined> = createFuture()

        const target = new DataTarget({
            finalized: parent.finalized,
            writer: async (opts: DataWriterOptions<TData>) => {
                const transformer = await config.transformer({
                    offset: await offsetFuture.promise(),
                })
                if (!parent.finalized && !transformer.fork) {
                    throw new TypeError('Missing fork method in unfinalized DataTransformer')
                }

                return {
                    offset: transformer.offset,
                    async write(batch: DataBatch<TData>) {
                        const data = await transformer.transform(batch)
                        await queue.put(data)
                        return batch.offset
                    },
                    async fork(fork: DataFork<TData>) {
                        return undefined
                    },
                    async close(): Promise<void> {
                        queue.close()
                        await source.close().catch(() => {})
                    },
                }
            },
        })

        const source = new DataSource({
            finalized: parent.finalized,
            reader: async (opts) => {
                offsetFuture.resolve(opts.offset)

                return {
                    async read(): Promise<DataBatch<UData> | undefined> {
                        return await queue.take()
                    },
                    async close(): Promise<void> {
                        queue.close()
                        await target.close().catch(() => {})
                    },
                }
            },
        })

        // FIXME: how to type this?
        return {
            target,
            source,
        } as any
    }
}

export function finalizer<T extends Data>(): DataDuplex<T, T, false, true> {
    const buffer: T[] = []
    const queue = new SyncQueue<DataBatch<T>>()
    let offsetFuture: Future<T['ref'] | undefined> = createFuture()

    const target = new DataTarget<T>({
        finalized: false,
        writer: async (opts: DataWriterOptions<T>) => {
            const offset = await offsetFuture.promise()
            return {
                offset,
                async write(batch: DataBatch<T>) {
                    buffer.push(...batch.data)

                    if (batch.finalizedHead) {
                        let unfinalizedIndex = 0
                        for (; unfinalizedIndex < buffer.length; unfinalizedIndex++) {
                            const ref = buffer[unfinalizedIndex].ref
                            if (batch.finalizedHead.compare(ref).isLess) break
                        }

                        const data = buffer.splice(0, unfinalizedIndex)
                        if (data.length > 0) {
                            const offset = data[data.length - 1].ref
                            await queue.put({
                                data,
                                offset,
                                finalizedHead: batch.finalizedHead,
                                head: batch.finalizedHead,
                            })
                        }
                    }

                    return batch.offset
                },
                async fork(fork: DataFork<T>): Promise<T['ref'] | undefined> {
                    return undefined
                },
                async close(): Promise<void> {
                    queue.close()
                    await source.close().catch(() => {})
                },
            }
        },
    })

    const source = new DataSource<T>({
        finalized: true,
        reader: async (opts) => {
            offsetFuture.resolve(opts.offset)

            return {
                async read(): Promise<DataBatch<T> | undefined> {
                    return await queue.take()
                },
                async close(): Promise<void> {
                    queue.close()
                    await target.close().catch(() => {})
                },
            }
        },
    })

    return {
        target,
        source,
    }
}
