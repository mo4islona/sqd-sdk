import {isForkException} from './errors'
import type {Data, DataBatch, DataFork, DataRef} from './data'

export interface DataReaderOptions<TData extends Data> {
    offset: TData['id'] | undefined
}

export interface DataReader<TData extends Data> extends AsyncIterator<DataBatch<TData>> {}

export interface DataReaderReadOptions<TData extends Data> {
    offset: TData['id'] | undefined
}

export namespace DataReader {
    export const fromAsync = <TData extends Data>(
        asyncIterable: AsyncIterable<DataBatch<TData>>
    ): DataReader<TData> => {
        const it = asyncIterable[Symbol.asyncIterator]()

        const wrapper: AsyncIterator<DataBatch<TData>> = {
            next: async () => it.next(),
        }

        if (typeof it.return === 'function') {
            const returnFn = it.return
            wrapper.return = async () => returnFn()
        }
        if (typeof it.throw === 'function') {
            const throwFn = it.throw
            wrapper.throw = async (err?: any) => throwFn(err)
        }

        return wrapper
    }
}



export interface DataStream<TData extends Data> extends AsyncIterableIterator<DataBatch<TData>> {}

export interface DataSource<TData extends Data, TUnfinalized extends boolean> extends AsyncIterable<TData['value']> {
    readonly unfinalized: TUnfinalized
    readonly ref: DataRef<TData['id']>
    read(opts: DataReaderReadOptions<TData>): DataStream<TData>
    close(reason?: any): Promise<void>
}

export interface DataWriterOptions<TData extends Data> {
    ref: DataRef<TData['id']>
}

export interface DataTargetWriteOptions<TData extends Data> {
    // FIXME: shutup linter
    _?: TData
}

export interface FinalizedDataWriter<TData extends Data, TReturn = unknown> {
    next(batch?: DataBatch<TData> | undefined): Promise<IteratorResult<TData['id'] | undefined, TReturn>>
    return?(): Promise<IteratorReturnResult<TReturn>>
    fork?(fork: DataFork<TData['id']>): Promise<IteratorResult<TData['id'] | undefined, TReturn>>
}

export interface UnfinalizedDataWriter<TData extends Data, TReturn = unknown>
    extends FinalizedDataWriter<TData, TReturn> {
    fork(fork: DataFork<TData['id']>): Promise<IteratorResult<TData['id'] | undefined, TReturn>>
}

export type DataWriter<TData extends Data, TUnfinalized extends boolean> = TUnfinalized extends true
    ? UnfinalizedDataWriter<TData>
    : FinalizedDataWriter<TData>

export interface DataTarget<TData extends Data, TUnfinalized extends boolean> {
    readonly unfinalized: TUnfinalized

    write(
        opts: DataTargetWriteOptions<TData>,
        read: (offset: TData['id'] | undefined) => DataStream<TData>
    ): Promise<void>
    close(reason?: any): Promise<void>
}

export interface DataDuplex<
    TData extends Data,
    UData extends Data,
    TUnfinalized extends boolean,
    UUnfinalized extends boolean
> {
    target: DataTarget<TData, TUnfinalized>
    source: DataSource<UData, UUnfinalized>
}

export interface DataFactoryOptions<TData extends Data, TUnfinalized extends boolean> {
    unfinalized: TUnfinalized
    ref: DataRef<TData['id']>
}

export type DataTargetFactory<TData extends Data, TUnfinalized extends boolean> = (
    opts: DataFactoryOptions<TData, TUnfinalized>
) => DataTarget<TData, TUnfinalized>

export type PipableTo<TData extends Data, TUnfinalized extends boolean> =
    | DataTarget<TData, TUnfinalized>
    | DataTargetFactory<TData, TUnfinalized>

export type DataDuplexFactory<
    TData extends Data,
    UData extends Data,
    TUnfinalized extends boolean,
    UUnfinalized extends boolean
> = (opts: DataFactoryOptions<TData, TUnfinalized>) => DataDuplex<TData, UData, TUnfinalized, UUnfinalized>

export type PipableThrough<
    TData extends Data,
    UData extends Data,
    TUnfinalized extends boolean,
    UUnfinalized extends boolean
> = DataDuplex<TData, UData, TUnfinalized, UUnfinalized> | DataDuplexFactory<TData, UData, TUnfinalized, UUnfinalized>

async function pipe<TData extends Data, TUnfinalized extends boolean>(
    source: DataSource<TData, TUnfinalized>,
    target: DataTarget<TData, TUnfinalized extends true ? true : boolean>
): Promise<void> {
    if (source.unfinalized && !target.unfinalized) {
        throw new TypeError('Cannot pipe from unfinalized DataSource to finalized DataTarget')
    }

    try {
        return await target.write({}, (offset) => source.read({offset}))
    } finally {
        await source.close?.().catch(() => {})
        await target.close?.().catch(() => {})
    }
}

async function pipe2<TData extends Data, TUnfinalized extends boolean>(
    source: DataSource<TData, TUnfinalized>,
    target: DataTarget<TData, TUnfinalized extends true ? true : boolean>
): Promise<void> {
    if (source.unfinalized && !target.unfinalized) {
        throw new TypeError('Cannot pipe from unfinalized DataSource to finalized DataTarget')
    }
}

export interface DataSourceConfig<T extends Data, TUnfinalized extends boolean = true> {
    unfinalized?: TUnfinalized
    ref: DataRef<T['id']>
    reader: (opts: DataReaderOptions<T>) => PromiseLike<DataReader<T>>
}

export const DataSource: {
    new <TData extends Data, TUnfinalized extends boolean>(config: DataSourceConfig<TData, TUnfinalized>): DataSource<
        TData,
        TUnfinalized
    >
} = class<TData extends Data, TUnfinalized extends boolean> implements DataSource<TData, TUnfinalized> {
    readonly unfinalized: TUnfinalized
    readonly ref: DataRef<TData['id']>

    private _state: 'opened' | 'locked' | 'closed' = 'opened'
    private _abortController?: AbortController
    private _closePromise?: Promise<void>

    private _reader: (opts: DataReaderReadOptions<TData>) => PromiseLike<DataReader<TData>>

    constructor(config: DataSourceConfig<TData, TUnfinalized>) {
        // NOTE: satisfy compiler
        this.unfinalized = config.unfinalized == null ? (true as TUnfinalized) : config.unfinalized
        this.ref = config.ref

        this._reader = config.reader
    }

    read(opts: DataReaderReadOptions<TData> = {offset: undefined}): DataStream<TData> {
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
                if (this._state === 'locked') this._state = 'opened'
                return {done: true, value: undefined}
            },

            throw: async (err: any) => {
                await reader?.return?.().catch(() => {})
                this._abortController = undefined
                if (this._state === 'locked') this._state = 'opened'
                throw err
            },

            [Symbol.asyncIterator]() {
                return this
            },
        }
    }

    pipeThrough<UData extends Data, UUnfinalized extends boolean>(
        duplex: PipableThrough<TData, UData, TUnfinalized extends true ? true : boolean, UUnfinalized>
    ): DataSource<UData, UUnfinalized> {
        if (typeof duplex === 'function') {
            duplex = duplex({
                unfinalized: this.unfinalized as any, // FIXME: how to type this?
                ref: this.ref,
            })
        }

        pipe(this, duplex.target).catch((err) => {
            throw err
        })

        return duplex.source
    }

    pipeTo(target: PipableTo<TData, TUnfinalized extends true ? true : boolean>): Promise<void> {
        if (typeof target === 'function') {
            target = target({
                unfinalized: this.unfinalized as any, // FIXME: how to type this?
                ref: this.ref,
            })
        }

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
    config: DataSourceConfig<TData, TUnfinalized>
): DataSource<TData, TUnfinalized> {
    return new DataSource(config)
}

export interface DataTargetConfig<TData extends Data, TUnfinalized extends boolean = true> {
    writer: (opts: DataTargetWriteOptions<TData>) => PromiseLike<DataWriter<TData, NoInfer<TUnfinalized>>>
    unfinalized?: TUnfinalized
    ref: DataRef<TData['id']>
}

export const DataTarget: {
    new <TData extends Data, TUnfinalized extends boolean>(config: DataTargetConfig<TData, TUnfinalized>): DataTarget<
        TData,
        TUnfinalized
    >
} = class<TData extends Data, TUnfinalized extends boolean> implements DataTarget<TData, TUnfinalized> {
    readonly unfinalized: TUnfinalized
    readonly ref: DataRef<TData['id']>

    private _state: 'opened' | 'locked' | 'closed' = 'opened'
    private _abortController?: AbortController
    private _writer: (opts: DataTargetWriteOptions<TData>) => PromiseLike<DataWriter<TData, TUnfinalized>>
    private _closePromise?: Promise<void>

    constructor(config: DataTargetConfig<TData, TUnfinalized>) {
        // NOTE: satisfy compiler
        this.unfinalized = config.unfinalized == null ? (true as TUnfinalized) : config.unfinalized
        this.ref = config.ref
        this._writer = config.writer
    }

    async write(
        opts: DataTargetWriteOptions<TData>,
        read: (offset: TData['id'] | undefined) => DataStream<TData>
    ): Promise<void> {
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

            const stream = read(offset)
            return processStream(stream, offset)
        }

        const processStream = async (
            stream: DataStream<TData>,
            currentOffset: TData['id'] | undefined
        ): Promise<unknown> => {
            this._abortController?.signal.throwIfAborted()

            let batch: DataBatch<TData> | undefined
            try {
                const {done, value} = await stream.next()
                if (done) {
                    return writer.return?.()
                }
                batch = value
            } catch (err) {
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

            validateBatch(this.ref, currentOffset, batch)

            const {value, done} = await writer.next(batch)
            if (done) return value

            // NOTE: If the offset is not the same as the batch offset,
            // it means that the batch was not fully consumed or we want to skip
            // so we break the current stream and start from the new offset
            // FIXME: Do we want this behavior?
            if (!value || !this.ref.compare(value, batch.offset).isEqual) {
                await stream.return?.().catch(() => {})
                return processData(value)
            }

            return processStream(stream, batch.offset)
        }

        try {
            const {value, done} = await writer.next()
            if (done) return

            await processData(value)
        } finally {
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

export function target<TData extends Data, TUnfinalized extends boolean>(
    config: DataTargetConfig<TData, TUnfinalized>
): DataTarget<TData, TUnfinalized>
export function target<TData extends Data, TUnfinalized extends boolean>(
    factory: (opts: DataFactoryOptions<TData, TUnfinalized>) => DataTargetConfig<TData, TUnfinalized>
): DataTargetFactory<TData, TUnfinalized>
export function target<TData extends Data, TUnfinalized extends boolean>(
    configOrFactory:
        | DataTargetConfig<TData, TUnfinalized>
        | ((opts: DataFactoryOptions<TData, TUnfinalized>) => DataTargetConfig<TData, TUnfinalized>)
): PipableTo<TData, TUnfinalized> {
    if (typeof configOrFactory === 'function') {
        return (opts) => new DataTarget(configOrFactory(opts))
    }
    return new DataTarget(configOrFactory)
}

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
    batch: DataBatch<TData>
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
