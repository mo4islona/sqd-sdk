import type {Data, DataCursorUtils} from '../data'
import type {DataDuplexFactory, DataFactoryOptions, DataReadOptions, DataMessage, DataWriteOptions} from '../core'
import {createSource, createTarget, stream} from '../core'

export interface DataTransformer<
    TInputData extends Data,
    TOutputData extends Data,
    TUnfinalized extends boolean,
    TInputRequest,
    TOutputRequest,
> {
    unfinalized: TUnfinalized
    cursorUtils: DataCursorUtils<TOutputData['cursor']>
    transform: (
        writeOpts: DataWriteOptions<TInputData, TUnfinalized, TInputRequest>,
        readOpts: DataReadOptions<TOutputData, TOutputRequest>,
    ) => AsyncIterableIterator<DataMessage<TOutputData, TUnfinalized>>
}

export type DataTransformerFactory<
    TInputData extends Data,
    TOutputData extends Data,
    TUnfinalized extends boolean,
    TInputRequest,
    TOutputRequest,
> = (
    opts: DataFactoryOptions<TUnfinalized> & {cursorUtils: DataCursorUtils<TOutputData['cursor']>},
) => DataTransformer<TInputData, TOutputData, TUnfinalized, TInputRequest, TOutputRequest>

export function createTransformer<
    TInputData extends Data,
    TOutputData extends Data = TInputData,
    TUnfinalized extends boolean = boolean,
    TInputRequest = never,
    TOutputRequest = TInputRequest,
>(
    transformerFactory: DataTransformerFactory<TInputData, TOutputData, TUnfinalized, TInputRequest, TOutputRequest>,
): DataDuplexFactory<TInputData, TOutputData, TUnfinalized, TUnfinalized, TInputRequest, TOutputRequest> {
    return createTarget((opts) => {
        return {
            unfinalized: opts.unfinalized as TUnfinalized,
            write: (writeOpts) => {
                const transformer = transformerFactory({
                    cursorUtils: writeOpts.cursorUtils,
                    unfinalized: opts.unfinalized,
                })
                return stream(
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
    })
}
