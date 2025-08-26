import type {DataCursorUtils} from '../cursor'
import {
    createSource,
    createTarget,
    stream,
    type DataTargetFactoryOptions,
    type ReadOptions,
    type DataMessage,
    type Stream,
} from '../core'

export interface DataTransformer<
    TInputCursor,
    TOutputCursor,
    TInputValue,
    TOutputValue,
    TInputRequest,
    TOutputRequest,
> {
    unfinalized: boolean
    cursorUtils: DataCursorUtils<TOutputCursor>
    transform: (
        write: {
            cursorUtils: DataCursorUtils<TInputCursor>
            read: (
                opts: ReadOptions<TInputCursor, TInputRequest>,
            ) => AsyncIterable<DataMessage<TInputCursor, TInputValue>>
        },
        read: ReadOptions<TOutputCursor, TOutputRequest>,
    ) => AsyncIterableIterator<DataMessage<TOutputCursor, TOutputValue>>
}

export type DataTransformerFactory<
    TInputCursor,
    TOutputCursor,
    TInputValue,
    TOutputValue,
    TInputRequest,
    TOutputRequest,
> = (
    opts: DataTargetFactoryOptions & {cursorUtils: DataCursorUtils<TInputCursor>},
) => DataTransformer<TInputCursor, TOutputCursor, TInputValue, TOutputValue, TInputRequest, TOutputRequest>

export function createTransformer<
    TInputCursor,
    TOutputCursor = TInputCursor,
    TInputValue = unknown,
    TOutputValue = TInputValue,
    TInputRequest = never,
    TOutputRequest = TInputRequest,
>(
    transformerFactory: DataTransformerFactory<
        TInputCursor,
        TOutputCursor,
        TInputValue,
        TOutputValue,
        TInputRequest,
        TOutputRequest
    >,
) {
    return createTarget<TInputCursor, TInputValue, TInputRequest, Stream<TOutputCursor, TOutputValue, TOutputRequest>>(
        (opts: DataTargetFactoryOptions) => {
            return {
                unfinalized: opts.unfinalized,
                write: (writeOpts) => {
                    const transformer = transformerFactory({
                        cursorUtils: writeOpts.cursorUtils,
                        unfinalized: opts.unfinalized,
                    })
                    return stream(() =>
                        createSource({
                            unfinalized: transformer.unfinalized,
                            cursorUtils: transformer.cursorUtils,
                            read: (readOpts) =>
                                transformer.transform(
                                    {
                                        cursorUtils: writeOpts.cursorUtils,
                                        read: writeOpts.read,
                                    },
                                    {
                                        cursor: readOpts.cursor,
                                        request: readOpts.request,
                                    },
                                ),
                        }),
                    )
                },
            }
        },
    )
}
