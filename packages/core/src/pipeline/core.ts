import {unexpectedCase} from '../internal/misc'
import type {DataCursorUtils} from './cursor'
import {ForkException} from './errors'

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
 * @template TRequest - The type of request parameters for the data source
 */
export interface DataReadOptions<TCursor, TRequest> {
    /** The cursor position to start reading from (optional) */
    cursor?: TCursor
    /** Additional request parameters for the data source (optional) */
    request?: TRequest
}

/**
 * Represents a data source that can stream data with cursor-based positioning.
 * Data sources can be finalized (providing final data)
 * or unfinalized (providing data that can change due to forks).
 *
 * @template TCursor - The cursor type that represents a position in the data stream (e.g. block number + hash)
 * @template TValue - The type of data values produced by this source
 * @template TRequest - The type of additional request parameters accepted by this source
 */
export interface DataSource<TCursor, TValue, TRequest> {
    /** Whether this source can produce unfinalized data that may change due to forks (arising e.g. due to a reorg in the blockchain) */
    unfinalized: boolean
    /** Necessary utilities for comparing and manipulating cursor values */
    cursorUtils: DataCursorUtils<TCursor>
    /**
     * Reads data from the source starting from the cursor specified in the options
     * and honoring the additional request parameters.
     * Returns an async iterable of data messages (data batches or forks).
     */
    read(options: DataReadOptions<TCursor, TRequest>): AsyncIterable<DataMessage<TCursor, TValue>>
}

/**
 * Context provided to data targets when writing data.
 * Contains the cursor utilities and a read function to access the data stream.
 *
 * @template TCursor - The cursor type that represents a position in the data stream (e.g. block height + hash)
 * @template TValue - The type of data values
 * @template TRequest - The type of additional request parameters for the data source that passes the context
 */
export interface DataWriteOptions<TCursor, TValue, TRequest> {
    /** Necessary utilities for comparing and manipulating cursor values */
    cursorUtils: DataCursorUtils<TCursor>
    /**
     * Function to read data from the source stream that passes the context.
     * Returns an async iterable of data messages (data batches or forks).
     */
    read(options: DataReadOptions<TCursor, TRequest>): AsyncIterable<DataMessage<TCursor, TValue>>
}

/**
 * Represents a data target that can consume data from a data stream.
 * Data targets can be finalized (only accept final data)
 * or unfinalized (accept both final and unfinalized data, capable of handling forks).
 *
 * @template TCursor - The cursor type that represents a position in the data stream (e.g. block height + hash)
 * @template TValue - The type of the consumed data values
 * @template TRequest - The type of additional request parameters to be passed back to the data source
 * @template TReturn - The return type of the write operation
 */
export interface DataTarget<TCursor, TValue, TRequest, TReturn> {
    /** Whether this target can handle unfinalized data that may change due to forks (arising e.g. due to a reorg in the blockchain) */
    unfinalized: boolean
    /** Writes data obtained from the provided context */
    write(context: DataWriteOptions<TCursor, TValue, TRequest>): TReturn
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
 * @template TRequest - The type of additional request parameters accepted by this source
 */
export interface DataSourceConfig<TCursor, TValue, TRequest> {
    /** Whether this source can produce unfinalized data */
    unfinalized?: boolean
    /** Necessary utilities for comparing and manipulating cursor values */
    cursorUtils: DataCursorUtils<TCursor>
    /**
     * Function to read data from the source.
     * Returns an async iterable of data messages (data batches or forks).
     */
    read(options: DataReadOptions<TCursor, TRequest>): AsyncIterable<DataMessage<TCursor, TValue>>
}

/**
 * Creates a data source from a configuration object.
 *
 * @template TCursor - The cursor type that represents a position in the data stream (e.g. block height + hash)
 * @template TValue - The type of data values produced by this source
 * @template TRequest - The type of additional request parameters accepted by this source at runtime
 * @param source - Configuration object defining the data source behavior
 * @returns A configured DataSource instance
 */
export function createSource<TCursor, TValue, TRequest>(
    source: DataSourceConfig<TCursor, TValue, TRequest>,
): DataSource<TCursor, TValue, TRequest> {
    return {
        unfinalized: source.unfinalized ?? true,
        cursorUtils: source.cursorUtils,
        read: (opts) => source.read(opts),
    }
}

/**
 * All necessities for creating a data target.
 *
 * @template TCursor - The cursor type that represents a position in the data stream (e.g. block height + hash)
 * @template TValue - The type of data values consumed by this target
 * @template TRequest - The type of additional request parameters to be passed back to the data source
 * @template TReturn - The return type of the write operation
 */
export interface DataTargetConfig<TCursor, TValue, TRequest, TReturn> {
    /** Whether this target can handle unfinalized data */
    unfinalized?: boolean
    /**
     * Function to write data obtained from the provided context.
     */
    write(context: DataWriteOptions<TCursor, TValue, TRequest>): TReturn
}

/**
 * Creates a data target from a configuration object or factory function.
 *
 * @template TCursor - The cursor type that represents a position in the data stream (e.g. block height + hash)
 * @template TValue - The type of data values consumed by this target
 * @template TRequest - The type of additional request parameters to be passed back to the data source
 * @template TReturn - The return type of the write operation
 * @param config - Configuration object defining the data target behavior
 * @returns A configured DataTarget instance
 */
export function createTarget<TCursor, TValue, TRequest, TReturn>(
    config: DataTargetConfig<TCursor, TValue, TRequest, TReturn>,
): DataTarget<TCursor, TValue, TRequest, TReturn>
/**
 * Creates a data target factory from a factory function.
 *
 * @template TCursor - The cursor type that represents a position in the data stream (e.g. block height + hash)
 * @template TValue - The type of data values consumed by this target
 * @template TRequest - The type of additional request parameters to be passed back to the data source
 * @template TReturn - The return type of the write operation
 * @param factory - Factory function that creates a target configuration based on options
 * @returns A configured DataTarget instance
 */
export function createTarget<TCursor, TValue, TRequest, TReturn>(
    factory: (opts: DataTargetFactoryOptions) => DataTargetConfig<TCursor, TValue, TRequest, TReturn>,
): (opts: DataTargetFactoryOptions) => DataTarget<TCursor, TValue, TRequest, TReturn>
export function createTarget<TCursor, TValue, TRequest, TReturn>(
    configOrFactory:
        | DataTargetConfig<TCursor, TValue, TRequest, TReturn>
        | ((opts: DataTargetFactoryOptions) => DataTargetConfig<TCursor, TValue, TRequest, TReturn>),
):
    | DataTarget<TCursor, TValue, TRequest, TReturn>
    | ((opts: DataTargetFactoryOptions) => DataTarget<TCursor, TValue, TRequest, TReturn>) {
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
 * @template TInputRequest - The type of additional request parameters to be passed to the source
 * @template TOutputCursor - The cursor type for the target data
 * @template TOutputValue - The type of data values to be written to the target
 * @template TOutputRequest - The type of additional request parameters passed in by the target
 */
export type DataDuplex<TInputCursor, TInputValue, TInputRequest, TOutputCursor, TOutputValue, TOutputRequest> =
    DataTarget<TInputCursor, TInputValue, TInputRequest, DataStream<TOutputCursor, TOutputValue, TOutputRequest>>

/**
 * Represents a data stream that can be piped to targets or iterated over directly.
 * Provides an interface for building data processing pipelines.
 *
 * @template TCursor - The cursor type that represents a position in the data stream (e.g. block height + hash)
 * @template TValue - The type of data values in the stream
 * @template TRequest - The type of additional request parameters that can be passed to the data source
 */
export interface DataStream<TCursor, TValue, TRequest> {
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
            | DataTarget<TCursor, TValue, TRequest, TReturn>
            | ((opts: DataTargetFactoryOptions) => DataTarget<TCursor, TValue, TRequest, TReturn>),
        opts?: DataPipeOptions,
    ): TReturn

    /**
     * Makes the stream iterable, allowing direct access to the data values.
     *
     * @param opts - Optional read options for controlling the iteration
     * @returns An async iterable of data values
     */
    [Symbol.asyncIterator](opts?: DataReadOptions<TCursor, TRequest>): AsyncIterable<TValue>
}

/**
 * Creates a data stream from a data source or source factory.
 *
 * @template TCursor - The cursor type that represents a position in the data stream (e.g. block height + hash)
 * @template TValue - The type of data values in the stream
 * @template TRequest - The type of additional request parameters that can be passed to the data source
 * @param sourceOrFactory - The data source or a factory function that creates a data source
 * @returns A DataStream instance that can be piped to targets or iterated over
 */
export function createStream<TCursor, TValue, TRequest>(
    sourceOrFactory: DataSource<TCursor, TValue, TRequest> | (() => DataSource<TCursor, TValue, TRequest>),
): DataStream<TCursor, TValue, TRequest> {
    const source = typeof sourceOrFactory === 'function' ? sourceOrFactory() : sourceOrFactory

    return {
        pipe: (targetOrFactory, opts: DataPipeOptions = {}) => {
            const target =
                typeof targetOrFactory === 'function'
                    ? targetOrFactory({unfinalized: source.unfinalized})
                    : targetOrFactory

            return pipe(source, target, opts)
        },
        [Symbol.asyncIterator]: (opts?: DataReadOptions<TCursor, TRequest>): AsyncIterable<TValue> => {
            return pipe(
                source,
                {
                    unfinalized: source.unfinalized,
                    write: async function* (streamOpts: DataWriteOptions<TCursor, TValue, TRequest>) {
                        for await (const message of streamOpts.read(
                            (opts ?? {cursor: undefined, request: undefined}) as DataReadOptions<TCursor, TRequest>,
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
        },
    }
}

function pipe<TCursor, TValue, TRequest, TReturn>(
    source: DataSource<TCursor, TValue, TRequest>,
    target: DataTarget<TCursor, TValue, TRequest, TReturn>,
    opts: DataPipeOptions,
): TReturn {
    if (source.unfinalized && !target.unfinalized) {
        throw new TypeError('Cannot pipe from unfinalized DataSource to finalized DataTarget')
    }

    return target.write({
        cursorUtils: source.cursorUtils,
        read: async function* (
            streamOpts: DataReadOptions<TCursor, TRequest>,
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
