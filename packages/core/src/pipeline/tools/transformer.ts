import {
    createSource,
    createTarget,
    DataSource,
    type DataTargetFactoryOptions,
    type DataWriteContext,
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

export function createTransformer<TInputCursor, TOutputCursor, TInputValue, TOutputValue, TInpuTQuery, TOutpuTQuery>(
    transform: DataTransform<TInputCursor, TOutputCursor, TInputValue, TOutputValue, TInpuTQuery, TOutpuTQuery>,
): (
    opts: DataTargetFactoryOptions,
) => DataDuplex<TInputCursor, TOutputCursor, TInputValue, TOutputValue, TInpuTQuery, TOutpuTQuery>
export function createTransformer<TInputCursor, TOutputCursor, TInputValue, TOutputValue, TInpuTQuery, TOutpuTQuery>(
    config: DataTransformerConfig<TInputCursor, TOutputCursor, TInputValue, TOutputValue, TInpuTQuery, TOutpuTQuery>,
): DataDuplex<TInputCursor, TOutputCursor, TInputValue, TOutputValue, TInpuTQuery, TOutpuTQuery>
export function createTransformer<TInputCursor, TOutputCursor, TInputValue, TOutputValue, TInpuTQuery, TOutpuTQuery>(
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

    return createTarget<TInputCursor, TInputValue, TInpuTQuery, DataSource<TOutputCursor, TOutputValue, TOutpuTQuery>>({
        unfinalized: transformOrConfig.unfinalized ?? true,
        write: (writeOpts) => {
            const transformed = transformOrConfig.transform({
                ...writeOpts,
                unfinalized: transformOrConfig.unfinalized ?? true,
            })

            if (transformed instanceof DataSource) {
                return transformed
            }

            return createSource({
                unfinalized: transformed.unfinalized ?? transformOrConfig.unfinalized ?? true,
                cursorUtils: transformed.cursorUtils,
                read: transformed.read,
            })
        },
    })
}
