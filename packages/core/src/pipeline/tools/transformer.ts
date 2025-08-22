import type {Data, DataRef} from '../data'
import type {DataDuplexFactory, DataFactoryOptions, DataReadOptions, DataMessage, DataWriteOptions} from '../core'
import {createSource, createTarget, stream} from '../core'

export interface DataTransformOptions<
    TInputData extends Data,
    TOutputData extends Data,
    TUnfinalized extends boolean,
    TInputRequest,
    TOutputRequest,
> extends DataReadOptions<TOutputData, TOutputRequest>,
        DataWriteOptions<TInputData, TUnfinalized, TInputRequest> {}

export interface DataTransformer<
    TInputData extends Data,
    TOutputData extends Data,
    TUnfinalized extends boolean,
    TInputRequest,
    TOutputRequest,
> {
    unfinalized: TUnfinalized
    ref: DataRef<TOutputData['id']>
    transform: (
        opts: DataTransformOptions<TInputData, TOutputData, TUnfinalized, TInputRequest, TOutputRequest>,
    ) => AsyncIterableIterator<DataMessage<TOutputData, TUnfinalized>>
}

export type DataTransformerFactory<
    TInputData extends Data,
    TOutputData extends Data,
    TUnfinalized extends boolean,
    TInputRequest,
    TOutputRequest,
> = (
    opts: DataFactoryOptions<TInputData, boolean> & {ref: DataRef<TOutputData['id']>},
) => DataTransformer<TInputData, TOutputData, TUnfinalized, TInputRequest, TOutputRequest>

export function createTransformer<
    TInputData extends Data,
    TOutputData extends Data,
    TUnfinalized extends boolean,
    TInputRequest,
    TOutputRequest,
>(
    transformerFactory: DataTransformerFactory<TInputData, TOutputData, TUnfinalized, TInputRequest, TOutputRequest>,
): DataDuplexFactory<TInputData, TOutputData, TUnfinalized, TUnfinalized, TInputRequest, TOutputRequest> {
    return createTarget((opts) => {
        return {
            unfinalized: opts.unfinalized as TUnfinalized,
            write: (writeOpts) => {
                const transformer = transformerFactory({
                    ref: writeOpts.ref,
                    unfinalized: opts.unfinalized,
                })
                return stream(
                    createSource({
                        unfinalized: transformer.unfinalized,
                        ref: transformer.ref,
                        read: (readOpts) =>
                            transformer.transform({
                                offset: readOpts.offset,
                                request: readOpts.request,
                                ref: writeOpts.ref,
                                read: writeOpts.read,
                            }),
                    }),
                )
            },
        }
    })
}
