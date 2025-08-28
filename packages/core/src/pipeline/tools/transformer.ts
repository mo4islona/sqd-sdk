import type {DataCursorUtils} from '../cursor'
import {
    createSource,
    createTarget,
    type DataTargetFactoryOptions,
    type DataStream,
    createStream,
    type DataWriteContext,
    type DataTarget,
    type DataDuplex,
} from '../core'

export type DataTransform<TInputCursor, TOutputCursor, TInputValue, TOutputValue, TInpuTQuery, TOutpuTQuery> = (
    opts: DataWriteContext<TInputCursor, TInputValue, TInpuTQuery> & {unfinalized: boolean},
) => DataWriteContext<TOutputCursor, TOutputValue, TOutpuTQuery> & {unfinalized?: boolean}

export interface DataTransformerConfig<
    TInputCursor,
    TOutputCursor,
    TInputValue,
    TOutputValue,
    TInpuTQuery,
    TOutpuTQuery,
> {
    unfinalized?: boolean
    transform: DataTransform<TInputCursor, TOutputCursor, TInputValue, TOutputValue, TInpuTQuery, TOutpuTQuery>
}

export function createTransformer<
    TInputCursor,
    TOutputCursor,
    TInputValue,
    TOutputValue,
    TInpuTQuery,
    TOutpuTQuery,
>(
    transform: DataTransform<TInputCursor, TOutputCursor, TInputValue, TOutputValue, TInpuTQuery, TOutpuTQuery>,
): (
    opts: DataTargetFactoryOptions,
) => DataDuplex<TInputCursor, TOutputCursor, TInputValue, TOutputValue, TInpuTQuery, TOutpuTQuery>
export function createTransformer<
    TInputCursor,
    TOutputCursor,
    TInputValue,
    TOutputValue,
    TInpuTQuery,
    TOutpuTQuery,
>(
    config: DataTransformerConfig<
        TInputCursor,
        TOutputCursor,
        TInputValue,
        TOutputValue,
        TInpuTQuery,
        TOutpuTQuery
    >,
): DataDuplex<TInputCursor, TOutputCursor, TInputValue, TOutputValue, TInpuTQuery, TOutpuTQuery>
export function createTransformer<
    TInputCursor,
    TOutputCursor,
    TInputValue,
    TOutputValue,
    TInpuTQuery,
    TOutpuTQuery,
>(
    transformOrConfig:
        | DataTransform<TInputCursor, TOutputCursor, TInputValue, TOutputValue, TInpuTQuery, TOutpuTQuery>
        | DataTransformerConfig<TInputCursor, TOutputCursor, TInputValue, TOutputValue, TInpuTQuery, TOutpuTQuery>,
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
        TInpuTQuery,
        DataStream<TOutputCursor, TOutputValue, TOutpuTQuery>
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
