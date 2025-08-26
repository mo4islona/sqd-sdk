import type {DataCursorUtils} from '../cursor'
import {
    createSource,
    createTarget,
    type DataTargetFactoryOptions,
    type DataStream,
    createStream,
    type DataWriteOptions,
    type DataTarget,
    type DataDuplex,
} from '../core'

export type DataTransform<TInputCursor, TOutputCursor, TInputValue, TOutputValue, TInputRequest, TOutputRequest> = (
    opts: DataWriteOptions<TInputCursor, TInputValue, TInputRequest>,
) => DataWriteOptions<TOutputCursor, TOutputValue, TOutputRequest>

export interface DataTransformerConfig<
    TInputCursor,
    TOutputCursor,
    TInputValue,
    TOutputValue,
    TInputRequest,
    TOutputRequest,
> {
    unfinalized?: boolean
    transform: DataTransform<TInputCursor, TOutputCursor, TInputValue, TOutputValue, TInputRequest, TOutputRequest>
}

export function createTransformer<
    TInputCursor,
    TOutputCursor = TInputCursor,
    TInputValue = unknown,
    TOutputValue = TInputValue,
    TInputRequest = never,
    TOutputRequest = TInputRequest,
>(
    transform: DataTransform<TInputCursor, TOutputCursor, TInputValue, TOutputValue, TInputRequest, TOutputRequest>,
): (
    opts: DataTargetFactoryOptions,
) => DataDuplex<TInputCursor, TInputValue, TInputRequest, TOutputCursor, TOutputValue, TOutputRequest>
export function createTransformer<
    TInputCursor,
    TOutputCursor = TInputCursor,
    TInputValue = unknown,
    TOutputValue = TInputValue,
    TInputRequest = never,
    TOutputRequest = TInputRequest,
>(
    config: DataTransformerConfig<
        TInputCursor,
        TOutputCursor,
        TInputValue,
        TOutputValue,
        TInputRequest,
        TOutputRequest
    >,
): DataDuplex<TInputCursor, TInputValue, TInputRequest, TOutputCursor, TOutputValue, TOutputRequest>
export function createTransformer<
    TInputCursor,
    TOutputCursor = TInputCursor,
    TInputValue = unknown,
    TOutputValue = TInputValue,
    TInputRequest = never,
    TOutputRequest = TInputRequest,
>(
    transformOrConfig:
        | DataTransform<TInputCursor, TOutputCursor, TInputValue, TOutputValue, TInputRequest, TOutputRequest>
        | DataTransformerConfig<TInputCursor, TOutputCursor, TInputValue, TOutputValue, TInputRequest, TOutputRequest>,
) {
    if (typeof transformOrConfig === 'function') {
        return (opts: DataTargetFactoryOptions) =>
            createTransformer({
                unfinalized: opts.unfinalized,
                transform: transformOrConfig,
            })
    }

    return createTarget<
        TInputCursor,
        TInputValue,
        TInputRequest,
        DataStream<TOutputCursor, TOutputValue, TOutputRequest>
    >({
        unfinalized: transformOrConfig.unfinalized,
        write: (writeOpts) => {
            const {cursorUtils, read} = transformOrConfig.transform(writeOpts)

            return createStream(
                createSource({
                    unfinalized: transformOrConfig.unfinalized,
                    cursorUtils,
                    read,
                }),
            )
        },
    })
}
