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
    opts: DataWriteOptions<TInputCursor, TInputValue, TInputRequest> & {unfinalized: boolean},
) => DataWriteOptions<TOutputCursor, TOutputValue, TOutputRequest> & {unfinalized?: boolean}

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
    TOutputCursor,
    TInputValue,
    TOutputValue,
    TInputRequest,
    TOutputRequest,
>(
    transform: DataTransform<TInputCursor, TOutputCursor, TInputValue, TOutputValue, TInputRequest, TOutputRequest>,
): (
    opts: DataTargetFactoryOptions,
) => DataDuplex<TInputCursor, TOutputCursor, TInputValue, TOutputValue, TInputRequest, TOutputRequest>
export function createTransformer<
    TInputCursor,
    TOutputCursor,
    TInputValue,
    TOutputValue,
    TInputRequest,
    TOutputRequest,
>(
    config: DataTransformerConfig<
        TInputCursor,
        TOutputCursor,
        TInputValue,
        TOutputValue,
        TInputRequest,
        TOutputRequest
    >,
): DataDuplex<TInputCursor, TOutputCursor, TInputValue, TOutputValue, TInputRequest, TOutputRequest>
export function createTransformer<
    TInputCursor,
    TOutputCursor,
    TInputValue,
    TOutputValue,
    TInputRequest,
    TOutputRequest,
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
        unfinalized: transformOrConfig.unfinalized ?? true,
        write: (writeOpts) => {
            const {cursorUtils, read, unfinalized} = transformOrConfig.transform({
                ...writeOpts,
                unfinalized: transformOrConfig.unfinalized ?? true,
            })

            return createStream(
                createSource({
                    unfinalized: unfinalized ?? transformOrConfig.unfinalized ?? true,
                    cursorUtils,
                    read,
                }),
            )
        },
    })
}
