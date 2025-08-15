import {createFuture} from '../internal/async'
import type {
    DataFactoryOptions,
    DataReader,
    DataReaderOptions,
    DataSourceConfig,
    DataStream,
    DataTargetConfig,
} from './core'
import type {Data, DataBatch} from './data'
import {isForkException} from './errors'

export interface Pipeline<TData extends Data, TUnfinalized extends boolean> {
    pipeThrough<UData extends Data, UUnfinalized extends boolean>(
        duplex: (opts: DataFactoryOptions<TData, TUnfinalized>) => Promise<{
            target: DataTargetConfig<TData, TUnfinalized extends true ? true : boolean>
            source: DataSourceConfig<UData, UUnfinalized>
        }>
    ): Pipeline<UData, UUnfinalized>
    pipeTo(
        target: (
            opts: DataFactoryOptions<TData, TUnfinalized>
        ) => Promise<DataTargetConfig<TData, TUnfinalized extends true ? true : boolean>>
    ): Promise<void>
    [Symbol.asyncIterator](): AsyncIterableIterator<DataBatch<TData>>
}

export function pipeline<TData extends Data, TUnfinalized extends boolean>(
    sourceFactory: () => Promise<DataSourceConfig<TData, TUnfinalized>>
): Pipeline<TData, TUnfinalized> {
    return {
        pipeThrough: (duplexFactory) => {
            const duplexFuture = createFuture<Awaited<ReturnType<typeof duplexFactory>>>()

            pipe(sourceFactory, (opts) =>
                duplexFactory(opts).then(
                    (duplex) => {
                        duplexFuture.resolve(duplex)
                        return duplex.target
                    },
                    (err) => {
                        duplexFuture.reject(err)
                        throw err
                    }
                )
            )

            return pipeline(async () => duplexFuture.promise().then((duplex) => duplex.source))
        },
        pipeTo: (targetFactory) => pipe(sourceFactory, targetFactory),
        [Symbol.asyncIterator](opts?: DataReaderOptions<TData>) {
            const offset = opts?.offset

            let stream: DataStream<TData> | undefined
            return {
                next: async () => {
                    if (!stream) {
                        const source = await sourceFactory()
                        stream = read(source, {offset})
                    }
                    return stream.next()
                },
                return: async () => {
                    let result = await stream?.return?.()
                    return result ? result : {done: true, value: undefined}
                },
                throw: async (err) => {
                    let result = await stream?.throw?.(err)
                    return result ? result : {done: true, value: undefined}
                },
                [Symbol.asyncIterator]() {
                    return this
                },
            }
        },
    }
}

async function pipe<TData extends Data, TUnfinalized extends boolean>(
    sourceFactory: () => Promise<DataSourceConfig<TData, TUnfinalized>>,
    targetFactory: (
        opts: DataFactoryOptions<TData, TUnfinalized>
    ) => Promise<DataTargetConfig<TData, TUnfinalized extends true ? true : boolean>>
): Promise<void> {
    const source = await sourceFactory()
    const target = await targetFactory({
        unfinalized: source.unfinalized as TUnfinalized,
        ref: source.ref,
    })

    if (source.unfinalized && !target.unfinalized) {
        throw new TypeError('Cannot pipe from unfinalized DataSource to finalized DataTarget')
    }

    const writer = await target.writer({})

    const processData = async (offset: TData['id'] | undefined): Promise<unknown> => {
        const stream = read(source, {offset})
        return processStream(stream, offset)
    }

    const processStream = async (
        stream: DataStream<TData>,
        currentOffset: TData['id'] | undefined
    ): Promise<unknown> => {
        let batch: DataBatch<TData> | undefined
        try {
            const {done, value} = await stream.next()
            if (done) return writer.return?.()
            batch = value
        } catch (err) {
            if (!isForkException<TData>(err)) {
                throw err
            }
            if (!source.unfinalized) {
                throw new TypeError('Got fork exception in finalized DataTarget')
            }
            if (!writer.fork) {
                throw new TypeError('Missing fork method in unfinalized DataWriter')
            }

            const {value, done} = await writer.fork(err.fork)
            if (done) return value
            return processData(value)
        }

        const {value, done} = await writer.next(batch)
        if (done) return value

        // NOTE: If the offset is not the same as the batch offset,
        // it means that the batch was not fully consumed or we want to skip
        // so we break the current stream and start from the new offset
        // FIXME: Do we want this behavior?
        if (!value || !source.ref.compare(value, batch.offset).isEqual) {
            await stream.return?.()
            return processData(value)
        }

        return processStream(stream, batch.offset)
    }

    const {value, done} = await writer.next()
    if (done) return

    await processData(value)
}

function read<TData extends Data, TUnfinalized extends boolean>(
    source: DataSourceConfig<TData, TUnfinalized>,
    opts: DataReaderOptions<TData>
): DataStream<TData> {
    let reader: DataReader<TData> | undefined

    return {
        next: async (): Promise<IteratorResult<DataBatch<TData>>> => {
            if (!reader) {
                reader = await source.reader(opts)
            }

            try {
                return await reader.next()
            } catch (err) {
                if (!isForkException<TData>(err)) throw err
                if (!source.unfinalized) {
                    throw new TypeError('Got fork exception in finalized DataSource')
                }
                throw err
            }
        },
        return: async (): Promise<IteratorResult<DataBatch<TData>>> => {
            const result = await reader?.return?.()
            return result ? result : {done: true, value: undefined}
        },
        throw: async (err: any) => {
            await reader?.return?.().catch(() => {})
            throw err
        },
        [Symbol.asyncIterator]() {
            return this
        },
    }
}
