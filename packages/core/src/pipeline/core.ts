import {isForkException} from './errors'
import type {Data, DataBatch, DataFork, DataRef} from './data'
import type {Awaitable} from '../internal/types'

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

export interface DataWriterOptions<TData extends Data> {
    ref: DataRef<TData['id']>
}

export interface DataTargetWriteOptions<TData extends Data> {
    // FIXME: shutup linter
    _?: TData
}

export interface FinalizedDataWriter<TData extends Data, TReturn = unknown> {
    next(
        batch: DataBatch<TData> | undefined,
        offset: TData['id'] | undefined
    ): Promise<IteratorResult<TData['id'] | undefined, TReturn>>
    return?(): Promise<IteratorReturnResult<TReturn>>
    fork?(
        fork: DataFork<TData['id']>,
        offset: TData['id'] | undefined
    ): Promise<IteratorResult<TData['id'] | undefined, TReturn>>
}

export interface UnfinalizedDataWriter<TData extends Data, TReturn = unknown>
    extends FinalizedDataWriter<TData, TReturn> {
    fork(fork: DataFork<TData['id']>): Promise<IteratorResult<TData['id'] | undefined, TReturn>>
}

export type DataWriter<TData extends Data, TUnfinalized extends boolean> = TUnfinalized extends true
    ? UnfinalizedDataWriter<TData>
    : FinalizedDataWriter<TData>

export interface DataSource<T extends Data, TUnfinalized extends boolean> {
    unfinalized: TUnfinalized
    ref: DataRef<T['id']>
    reader: (opts: DataReaderOptions<T>) => Awaitable<DataReader<T>>
    [Symbol.asyncIterator](opts: DataReaderOptions<T>): AsyncIterableIterator<DataBatch<T>>
}

export interface DataTarget<TData extends Data, TUnfinalized extends boolean> {
    unfinalized: TUnfinalized
    ref: DataRef<TData['id']>
    writer: (opts: DataTargetWriteOptions<TData>) => Awaitable<DataWriter<TData, NoInfer<TUnfinalized>>>
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

export function source<TData extends Data, TUnfinalized extends boolean>(
    sourceFactory: () => Omit<DataSource<TData, TUnfinalized>, typeof Symbol.asyncIterator>
): () => DataSource<TData, TUnfinalized> {
    return () => {
        const source = sourceFactory()
        return {
            ...source,
            [Symbol.asyncIterator]: (opts: DataReaderOptions<TData>) => {
                let reader: DataReader<TData> | undefined
                return {
                    next: async () => {
                        if (!reader) {
                            reader = await source.reader(opts)
                        }
                        return reader.next()
                    },
                    return: async () => {
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
            },
        }
    }
}

export function target<TData extends Data, TUnfinalized extends boolean>(
    target: (opts: DataFactoryOptions<TData, TUnfinalized>) => DataTarget<TData, TUnfinalized>
): (opts: DataFactoryOptions<TData, TUnfinalized>) => DataTarget<TData, TUnfinalized> {
    return (opts) => target(opts)
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
