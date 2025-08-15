import {createFuture} from '../internal/async'
import type {Awaitable} from '../internal/types'
import type {
    DataDuplex,
    DataFactoryOptions,
    DataReader,
    DataReaderOptions,
    DataSource,
    DataStream,
    DataTarget,
} from './core'
import type {Data, DataBatch, DataRef} from './data'
import {isForkException} from './errors'

export interface Pipeline<TData extends Data, TUnfinalized extends boolean> {
    pipeThrough<UData extends Data, UUnfinalized extends boolean>(
        duplexFactory: (
            opts: DataFactoryOptions<TData, TUnfinalized>
        ) => Awaitable<DataDuplex<TData, UData, TUnfinalized extends true ? true : boolean, UUnfinalized>>
    ): Pipeline<UData, UUnfinalized>
    pipeTo(
        targetFactory: (
            opts: DataFactoryOptions<TData, TUnfinalized>
        ) => Awaitable<DataTarget<TData, TUnfinalized extends true ? true : boolean>>
    ): Promise<void>
    [Symbol.asyncIterator](): AsyncIterableIterator<DataBatch<TData>>
}

export function pipeline<TData extends Data, TUnfinalized extends boolean>(
    sourceFactory: () => Awaitable<DataSource<TData, TUnfinalized>>
): Pipeline<TData, TUnfinalized> {
    return {
        pipeThrough: (duplexFactory) => {
            const duplexFuture = createFuture<Awaited<ReturnType<typeof duplexFactory>>>()

            pipe(sourceFactory, async (opts) => {
                const duplex = await duplexFactory(opts)
                duplexFuture.resolve(duplex)
                return duplex.target
            }).catch((err) => {
                duplexFuture.reject(err)
                throw err
            })

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
                        stream = source[Symbol.asyncIterator]({offset})
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
    sourceFactory: () => Awaitable<DataSource<TData, TUnfinalized>>,
    targetFactory: (
        opts: DataFactoryOptions<TData, TUnfinalized>
    ) => Awaitable<DataTarget<TData, TUnfinalized extends true ? true : boolean>>
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
        const stream = await source.reader({offset})
        return processStream(stream, offset)
    }

    const processStream = async (
        stream: AsyncIterator<DataBatch<TData>>,
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
                throw new TypeError('Got fork exception from finalized DataSource')
            }
            if (!target.unfinalized) {
                throw new TypeError('Got fork exception for finalized DataTarget')
            }
            if (!writer.fork) {
                throw new TypeError('Missing fork method in unfinalized DataWriter')
            }

            const {value, done} = await writer.fork(err.fork, currentOffset)
            if (done) return value

            return processData(value)
        }

        const {value, done} = await writer.next(batch, currentOffset)
        if (done) {
            await stream.return?.()
            return value
        }

        validateBatch(source.ref, currentOffset, batch)

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

    const {value, done} = await writer.next(undefined, undefined)
    if (done) return

    await processData(value)
}

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
