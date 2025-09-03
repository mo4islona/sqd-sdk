import {
    type DataDuplex,
    DataSource,
    type DataTargetFactoryOptions,
    type DataWriteContext,
    createSource,
    createTarget,
} from '../core'

export type DataTransform<In, Out, Query, Cursor> = (
    opts: DataWriteContext<Cursor, In, Query>,
) => DataWriteContext<Cursor, Out, Query>

export interface DataTransformerConfig<In, Out, Query, Cursor> {
    transform: DataTransform<In, Out, Query, Cursor>
}

export function createTransformer<In, Out, Query, Cursor>(
    transform: DataTransform<In, Out, Query, Cursor>,
): (opts: DataTargetFactoryOptions) => DataDuplex<In, Out, Query, Cursor>
export function createTransformer<In, Out, Query, Cursor>(
    config: DataTransformerConfig<In, Out, Query, Cursor>,
): DataDuplex<In, Out, Query, Cursor>
export function createTransformer<In, Out, Query, Cursor>(
    transformOrConfig: DataTransform<In, Out, Query, Cursor> | DataTransformerConfig<In, Out, Query, Cursor>,
) {
    if (typeof transformOrConfig === 'function') {
        return (opts: DataTargetFactoryOptions) =>
            createTransformer({
                transform: transformOrConfig,
            })
    }

    return createTarget<Cursor, In, Query, DataSource<Cursor, Out, Query>>({
        write: (writeOpts) => {
            const transformed = transformOrConfig.transform({
                ...writeOpts,
            })

            if (transformed instanceof DataSource) {
                return transformed
            }

            return createSource({
                cursorUtils: transformed.cursorUtils,
                read: transformed.read,
            })
        },
    })
}
