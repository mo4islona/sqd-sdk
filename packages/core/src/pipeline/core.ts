import {unexpectedCase} from '../internal/misc'
import type {DataCursorUtils} from './cursor'
import {ForkException} from './errors'
import {createFinalizer, createMapper, createFilter, createScanner, createReducer} from './tools'

/**
 * Represents a single data item with its associated cursor position.
 *
 * @template TCursor - The cursor type that represents a position in the data stream (e.g. block number + hash)
 * @template TValue - The actual data value
 */
export interface DataBatchItem<TCursor, TValue> {
    cursor: TCursor
    value: TValue
}

/**
 * Represents a batch of data items with metadata about the boundaries of
 * the full data stream.
 * This is the primary message type for streaming data through the pipeline.
 *
 * @template TCursor - The cursor type that represents a position in the data stream (e.g. block number + hash)
 * @template TValue - The type of data values in the batch
 */
export interface DataBatchMessage<TCursor, TValue> {
    type: 'batch'
    /** The cursor position at the end of this batch (inclusive) */
    cursor: TCursor
    /** The highest cursor position in the whole data stream */
    head: TCursor
    /** The highest cursor position that corresponds to final data, if available */
    finalizedHead?: TCursor
    data: DataBatchItem<TCursor, TValue>[]
}

/**
 * Represents a fork event in the data stream, indicating that the data
 * stream has forked (e.g. due to a reorg in the blockchain from which the data
 * originates). Comes with a sample of valid cursor positions within
 * the updated data (e.g. the new blockchain consensus).
 *
 * @template TCursor - The cursor type that represents a position in the data stream (e.g. block number + hash)
 */
export interface DataForkMessage<TCursor> {
    type: 'fork'
    /** Array of cursor positions valid within the post-forkdata */
    cursors: TCursor[]
}

export type DataMessage<TCursor, TValue> = DataBatchMessage<TCursor, TValue> | DataForkMessage<TCursor>

/**
 * Runtime options for reading data from a data source. Are typically passed by
 * downstream data stream components upwards (from a data target to a data source)
 *
 * @template TCursor - The cursor type that represents a position in the data stream (e.g. block number + hash)
 * @template TQuery - The type of request parameters for the data source
 */
export interface DataReadRequest<TCursor, TQuery> {
    /** The cursor position to start reading from (optional) */
    cursor: TCursor | undefined
    /** Additional request parameters for the data source (optional) */
    query?: TQuery
}

/**
 * Represents a data source that can stream data with cursor-based positioning.
 * Source that can be piped to targets or iterated over directly.
 * Provides an interface for building data processing pipelines.
 *
 * Data sources can be finalized (providing final data)
 * or unfinalized (providing data that can change due to forks).
 *
 * @template TCursor - The cursor type that represents a position in the data stream (e.g. block number + hash)
 * @template TValue - The type of data values produced by this source
 * @template TQuery - The type of additional request parameters accepted by this source
 */
export interface DataSource<TCursor, TValue, TQuery> {
    /** Whether this source can produce unfinalized data that may change due to forks (arising e.g. due to a reorg in the blockchain) */
    readonly unfinalized: boolean

    /** Necessary utilities for comparing and manipulating cursor values */
    readonly cursorUtils: DataCursorUtils<TCursor>

    /**
     * Reads data from the source starting from the cursor specified in the options
     * and honoring the additional request parameters.
     * Returns an async iterable of data messages (data batches or forks).
     */
    read(options: DataReadRequest<TCursor, TQuery>): AsyncIterable<DataMessage<TCursor, TValue>>

    /**
     * Pipes this stream to a target, creating a data processing pipeline.
     *
     * @template TReturn - The return type of the target's write operation
     * @param targetOrFactory - The target to pipe to, or a factory function that creates a target
     * @param opts - Optional configuration for the pipe operation
     * @returns The result of the target's write operation
     */
    pipe<TReturn>(
        targetOrFactory:
            | DataTarget<TCursor, TValue, TQuery, TReturn>
            | ((opts: DataTargetFactoryOptions) => DataTarget<TCursor, TValue, TQuery, TReturn>),
        opts?: DataPipeOptions,
    ): TReturn

    /**
     * Maps the data values to a new type.
     *
     * @template UValue - The type of the mapped data values
     * @param fn - The mapping function
     * @returns A DataStream instance that can be piped to targets or iterated over
     */
    map<UValue>(mapper: (value: TValue) => UValue): DataSource<TCursor, UValue, TQuery>

    /**
     * Filters the data values based on a predicate function.
     *
     * @param predicate - The predicate function to test each value
     * @returns A DataSource instance with filtered values
     */
    filter(predicate: (value: TValue) => boolean): DataSource<TCursor, TValue, TQuery>

    /**
     * Scans the data values, emitting intermediate accumulated values as a stream.
     *
     * @template UValue - The type of the accumulated value
     * @param reducer - The reducer function to accumulate values
     * @param initialValue - The initial value for the accumulator
     * @returns A DataSource instance with accumulated values
     */
    scan<UValue>(
        reducer: (accumulator: UValue, value: TValue) => UValue,
        initialValue: UValue,
    ): DataSource<TCursor, UValue, TQuery>

    /**
     * Reduces the data values to a single accumulated value by consuming the entire stream.
     *
     * @template UValue - The type of the accumulated value
     * @param reducer - The reducer function to accumulate values
     * @param initialValue - The initial value for the accumulator
     * @param opts - Optional read options for controlling the iteration
     * @returns A Promise that resolves to the final accumulated value
     */
    reduce<UValue>(
        reducer: (accumulator: UValue, value: TValue) => UValue,
        initialValue: UValue,
        opts?: DataReadRequest<TCursor, TQuery>,
    ): Promise<UValue>

    /**
     * Finalizes the stream, making it immutable.
     *
     * @returns A finalized DataStream instance that can be piped to targets or iterated over
     */
    finalize(): DataSource<TCursor, TValue, TQuery>

    /**
     * Makes the stream iterable, allowing direct access to the data values.
     *
     * @param opts - Optional read options for controlling the iteration
     * @returns An async iterable of data values
     */
    [Symbol.asyncIterator](opts?: DataReadRequest<TCursor, TQuery>): AsyncIterable<TValue>
}

/**
 * Context provided to data targets when writing data.
 * Contains the cursor utilities and a read function to access the data stream.
 *
 * @template TCursor - The cursor type that represents a position in the data stream (e.g. block height + hash)
 * @template TValue - The type of data values
 * @template TQuery - The type of additional request parameters for the data source that passes the context
 */
export interface DataWriteContext<TCursor, TValue, TQuery> {
    /** Necessary utilities for comparing and manipulating cursor values */
    cursorUtils: DataCursorUtils<TCursor>

    /**
     * Function to read data from the source stream that passes the context.
     * Returns an async iterable of data messages (data batches or forks).
     */
    read(options: DataReadRequest<TCursor, TQuery>): AsyncIterable<DataMessage<TCursor, TValue>>
}

/**
 * Represents a data target that can consume data from a data stream.
 * Data targets can be finalized (only accept final data)
 * or unfinalized (accept both final and unfinalized data, capable of handling forks).
 *
 * @template TCursor - The cursor type that represents a position in the data stream (e.g. block height + hash)
 * @template TValue - The type of the consumed data values
 * @template TQuery - The type of additional request parameters to be passed back to the data source
 * @template TReturn - The return type of the write operation
 */
export interface DataTarget<TCursor, TValue, TQuery, TReturn> {
    /** Whether this target can handle unfinalized data that may change due to forks (arising e.g. due to a reorg in the blockchain) */
    unfinalized: boolean

    /** Writes data obtained from the provided context */
    write(context: DataWriteContext<TCursor, TValue, TQuery>): TReturn
}

/**
 * @template unfinalized - Whether the target should handle unfinalized data
 */
export interface DataTargetFactoryOptions {
    /** Whether the target should handle unfinalized data that may change due to forks (arising e.g. due to a reorg in the blockchain) */
    unfinalized: boolean
}

export interface DataPipeOptions {
    /** Whether to validate batch ordering and cursor consistency (default: false) */
    validateBatches?: boolean
}

/**
 * All necessities for creating a data source.
 *
 * @template TCursor - The cursor type that represents a position in the data stream (e.g. block height + hash)
 * @template TValue - The type of data values produced by this source
 * @template TQuery - The type of additional request parameters accepted by this source
 */
export interface DataSourceConfig<TCursor, TValue, TQuery> {
    /** Whether this source can produce unfinalized data */
    unfinalized?: boolean
    /** Necessary utilities for comparing and manipulating cursor values */
    cursorUtils: DataCursorUtils<TCursor>

    /**
     * Reads data from the source.
     *
     * @param request - The read request
     * @returns An async iterable of data messages
     */
    read(request: DataReadRequest<TCursor, TQuery>): AsyncIterable<DataMessage<TCursor, TValue>>
}

/**
 * Creates a data source from a configuration object.
 *
 * @template TCursor - The cursor type that represents a position in the data stream (e.g. block height + hash)
 * @template TValue - The type of data values produced by this source
 * @template TQuery - The type of additional request parameters accepted by this source at runtime
 * @param source - Configuration object defining the data source behavior
 * @returns A configured DataSource instance
 */
export function createSource<TCursor, TValue, TQuery>(
    config: DataSourceConfig<TCursor, TValue, TQuery>,
): DataSource<TCursor, TValue, TQuery> {
    return new DataSource(config)
}

/**
 * All necessities for creating a data target.
 *
 * @template TCursor - The cursor type that represents a position in the data stream (e.g. block height + hash)
 * @template TValue - The type of data values consumed by this target
 * @template TQuery - The type of additional request parameters to be passed back to the data source
 * @template TReturn - The return type of the write operation
 */
export interface DataTargetConfig<TCursor, TValue, TQuery, TReturn> {
    /** Whether this target can handle unfinalized data */
    unfinalized?: boolean
    /**
     * Function to write data obtained from the provided context.
     */
    write(context: DataWriteContext<TCursor, TValue, TQuery>): TReturn
}

/**
 * Creates a data target from a configuration object or factory function.
 *
 * @template TCursor - The cursor type that represents a position in the data stream (e.g. block height + hash)
 * @template TValue - The type of data values consumed by this target
 * @template TQuery - The type of additional request parameters to be passed back to the data source
 * @template TReturn - The return type of the write operation
 * @param config - Configuration object defining the data target behavior
 * @returns A configured DataTarget instance
 */
export function createTarget<TCursor, TValue, TQuery, TReturn>(
    config: DataTargetConfig<TCursor, TValue, TQuery, TReturn>,
): DataTarget<TCursor, TValue, TQuery, TReturn>
/**
 * Creates a data target factory from a factory function.
 *
 * @template TCursor - The cursor type that represents a position in the data stream (e.g. block height + hash)
 * @template TValue - The type of data values consumed by this target
 * @template TQuery - The type of additional request parameters to be passed back to the data source
 * @template TReturn - The return type of the write operation
 * @param factory - Factory function that creates a target configuration based on options
 * @returns A configured DataTarget instance
 */
export function createTarget<TCursor, TValue, TQuery, TReturn>(
    factory: (opts: DataTargetFactoryOptions) => DataTargetConfig<TCursor, TValue, TQuery, TReturn>,
): (opts: DataTargetFactoryOptions) => DataTarget<TCursor, TValue, TQuery, TReturn>
export function createTarget<TCursor, TValue, TQuery, TReturn>(
    configOrFactory:
        | DataTargetConfig<TCursor, TValue, TQuery, TReturn>
        | ((opts: DataTargetFactoryOptions) => DataTargetConfig<TCursor, TValue, TQuery, TReturn>),
):
    | DataTarget<TCursor, TValue, TQuery, TReturn>
    | ((opts: DataTargetFactoryOptions) => DataTarget<TCursor, TValue, TQuery, TReturn>) {
    if (typeof configOrFactory === 'function') {
        return (opts: DataTargetFactoryOptions) => createTarget(configOrFactory(opts))
    }

    return {
        unfinalized: configOrFactory.unfinalized ?? true,
        write: (opts) => configOrFactory.write(opts),
    }
}

/**
 * Represents a duplex data stream that can both consume from a source and write to a target.
 * This is useful for data transformation pipelines where data flows through multiple stages.
 *
 * @template TInputCursor - The cursor type (e.g. block height + hash) for the source data
 * @template TInputValue - The type of source data values
 * @template TInpuTQuery - The type of additional request parameters to be passed to the source
 * @template TOutputCursor - The cursor type for the target data
 * @template TOutputValue - The type of data values to be written to the target
 * @template TOutpuTQuery - The type of additional request parameters passed in by the target
 */
export type DataDuplex<TInputCursor, TOutputCursor, TInputValue, TOutputValue, TInpuTQuery, TOutpuTQuery> = DataTarget<
    TInputCursor,
    TInputValue,
    TInpuTQuery,
    DataSource<TOutputCursor, TOutputValue, TOutpuTQuery>
>

export interface DataSourceConstructor {
    new <TCursor, TValue, TQuery>(
        config: DataSourceConfig<TCursor, TValue, TQuery>,
    ): DataSource<TCursor, TValue, TQuery>
}

export const DataSource: DataSourceConstructor = class<TCursor, TValue, TQuery>
    implements DataSource<TCursor, TValue, TQuery>
{
    readonly #unfinalized: boolean
    readonly #read: (request: DataReadRequest<TCursor, TQuery>) => AsyncIterable<DataMessage<TCursor, TValue>>
    readonly #cursorUtils: DataCursorUtils<TCursor>

    get unfinalized() {
        return this.#unfinalized
    }

    get cursorUtils() {
        return this.#cursorUtils
    }

    constructor(config: DataSourceConfig<TCursor, TValue, TQuery>) {
        this.#unfinalized = config.unfinalized ?? true
        this.#read = config.read
        this.#cursorUtils = config.cursorUtils
    }

    read(request: DataReadRequest<TCursor, TQuery>): AsyncIterable<DataMessage<TCursor, TValue>> {
        return this.#read(request)
    }

    pipe<TReturn>(
        targetOrFactory:
            | DataTarget<TCursor, TValue, TQuery, TReturn>
            | ((opts: DataTargetFactoryOptions) => DataTarget<TCursor, TValue, TQuery, TReturn>),
        opts?: DataPipeOptions,
    ): TReturn {
        const target =
            typeof targetOrFactory === 'function' ? targetOrFactory({unfinalized: this.#unfinalized}) : targetOrFactory

        return pipe(this, target, opts)
    }

    map<UValue>(fn: (value: TValue) => UValue): DataSource<TCursor, UValue, TQuery> {
        return this.pipe(createMapper(fn), {validateBatches: false})
    }

    filter(predicate: (value: TValue) => boolean): DataSource<TCursor, TValue, TQuery> {
        return this.pipe(createFilter(predicate), {validateBatches: false})
    }

    scan<UValue>(
        reducer: (accumulator: UValue, value: TValue) => UValue,
        initialValue: UValue,
    ): DataSource<TCursor, UValue, TQuery> {
        return this.pipe(createScanner(reducer, initialValue), {validateBatches: false})
    }

    async reduce<UValue>(
        reducer: (accumulator: UValue, value: TValue) => UValue,
        initialValue: UValue,
        opts?: DataReadRequest<TCursor, TQuery>,
    ): Promise<UValue> {
        return this.pipe(createReducer(reducer, initialValue), {})
    }

    finalize(): DataSource<TCursor, TValue, TQuery> {
        return this.pipe(createFinalizer(), {})
    }

    [Symbol.asyncIterator](opts?: DataReadRequest<TCursor, TQuery>): AsyncIterable<TValue> {
        return pipe(
            this,
            {
                unfinalized: this.#unfinalized,
                write: async function* (streamOpts: DataWriteContext<TCursor, TValue, TQuery>) {
                    for await (const message of streamOpts.read(
                        (opts ?? {cursor: undefined, request: undefined}) as DataReadRequest<TCursor, TQuery>,
                    )) {
                        switch (message.type) {
                            case 'batch':
                                yield* message.data.map((item) => item.value)
                                break
                            case 'fork':
                                throw new ForkException(message.cursors)
                            default:
                                throw unexpectedCase((message as any).type)
                        }
                    }
                },
            },
            {},
        )
    }
}

function pipe<TCursor, TValue, TQuery, TReturn>(
    source: DataSource<TCursor, TValue, TQuery>,
    target: DataTarget<TCursor, TValue, TQuery, TReturn>,
    opts: DataPipeOptions = {},
): TReturn {
    if (source.unfinalized && !target.unfinalized) {
        throw new TypeError('Cannot pipe from unfinalized DataSource to finalized DataTarget')
    }

    return target.write({
        cursorUtils: source.cursorUtils,
        read: async function* (
            streamOpts: DataReadRequest<TCursor, TQuery>,
        ): AsyncIterable<DataMessage<TCursor, TValue>> {
            let offset = streamOpts.cursor

            for await (const message of source.read(streamOpts)) {
                switch (message.type) {
                    case 'batch': {
                        const batch = message

                        if (!source.unfinalized && !batch.finalizedHead) {
                            throw new TypeError('Finalized source data must have a finalized head')
                        }

                        if (opts.validateBatches) {
                            if (offset && !source.cursorUtils.compare(batch.cursor, offset).isGreaterOrEqual) {
                                throw new RangeError('New offset is below the previous offset')
                            }

                            if (!source.cursorUtils.compare(batch.head, batch.cursor).isGreaterOrEqual) {
                                throw new RangeError('Head is below the offset')
                            }

                            if (!source.unfinalized) {
                                if (!source.cursorUtils.compare(batch.head, batch.finalizedHead as TCursor).isEqual) {
                                    throw new RangeError('Head is not equal to the finalized head')
                                }
                            } else if (batch.finalizedHead) {
                                if (!source.cursorUtils.compare(batch.head, batch.finalizedHead).isGreaterOrEqual) {
                                    throw new RangeError('Head is below the finalized head')
                                }
                            }

                            let lastId = offset
                            for (const item of batch.data) {
                                if (lastId && !source.cursorUtils.compare(item.cursor, lastId).isGreater) {
                                    throw new RangeError('Item is below or equal to the previous item')
                                }
                                lastId = item.cursor
                            }

                            if (lastId && !source.cursorUtils.compare(batch.cursor, lastId).isGreaterOrEqual) {
                                throw new RangeError('Offset is below the data')
                            }

                            if (lastId && !source.cursorUtils.compare(batch.head, lastId).isGreaterOrEqual) {
                                throw new RangeError('Head is below the data')
                            }
                        }

                        offset = batch.cursor

                        break
                    }
                    case 'fork': {
                        if (!source.unfinalized) {
                            throw new RangeError('Got fork message from finalized DataSource')
                        }
                        if (!target.unfinalized) {
                            throw new RangeError('Got fork message for finalized DataTarget')
                        }

                        return
                    }
                    default: {
                        throw unexpectedCase((message as any).type)
                    }
                }

                yield message
            }
        },
    })
}
