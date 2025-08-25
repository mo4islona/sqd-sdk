import {
    createSource,
    createTarget,
    type DataFactoryOptions,
    type DataStream,
    stream,
    type DataSource,
    type DataSourceFactory,
    type DataTarget,
    type DataTargetFactory,
    type DataDuplex,
    type DataDuplexFactory,
    type DataMessage,
} from './core'
import {DataCursor, type DataCursorUtils, type Data} from './data'
import {createTransformer, type DataTransformer, type DataTransformerFactory} from './tools'

export interface BlockRef {
    number: number
    hash: string
}

export const BlockRefUtils = {
    compare: (a: BlockRef, b: BlockRef) => {
        if (a.number < b.number) return DataCursor.Less
        if (a.number > b.number) return DataCursor.Greater
        if (a.hash !== b.hash) return DataCursor.Fork

        return DataCursor.Equal
    },
    serialize: (ref: BlockRef) => ref,
    deserialize: (ref: BlockRef) => ref,
}

export type BlockSource<TValue, TUnfinalized extends boolean, TRequest> = DataSource<
    BlockData<TValue>,
    TUnfinalized,
    TRequest
>

export type BlockSourceFactory<TValue, TUnfinalized extends boolean, TRequest> = () => BlockSource<
    TValue,
    TUnfinalized,
    TRequest
>

export function createBlockSource<TValue, TUnfinalized extends boolean, TRequest>(
    source:
        | Omit<BlockSource<TValue, TUnfinalized, TRequest>, 'cursorUtils'>
        | (() => Omit<BlockSource<TValue, TUnfinalized, TRequest>, 'cursorUtils'>),
): BlockSourceFactory<TValue, TUnfinalized, TRequest> {
    if (typeof source === 'function') {
        source = source()
    }

    return createSource({
        ...source,
        cursorUtils: BlockRefUtils,
    })
}

export type BlockTarget<TValue, TUnfinalized extends boolean, TRequest, TResult> = DataTarget<
    Data<TValue, BlockRef>,
    TUnfinalized,
    TRequest,
    TResult
>

export type BlockTargetFactory<TValue, TUnfinalized extends boolean, TRequest, TResult> = (
    opts: DataFactoryOptions<TUnfinalized>,
) => BlockTarget<TValue, TUnfinalized, TRequest, TResult>

export function createBlockTarget<TValue, TUnfinalized extends boolean, TRequest, TResult>(
    target:
        | BlockTarget<TValue, TUnfinalized, TRequest, TResult>
        | BlockTargetFactory<TValue, TUnfinalized, TRequest, TResult>,
): BlockTargetFactory<TValue, TUnfinalized, TRequest, TResult> {
    return createTarget(target)
}

export type BlockDuplex<
    TInputValue,
    TOutputValue,
    TInputUnfinalized extends boolean,
    TOutputUnfinalized extends boolean,
    TInputRequest,
    TOutputRequest,
> = DataDuplex<
    Data<TInputValue, BlockRef>,
    Data<TOutputValue, BlockRef>,
    TInputUnfinalized,
    TOutputUnfinalized,
    TInputRequest,
    TOutputRequest
>

export type BlockDuplexFactory<
    TInputValue,
    TOutputValue,
    TInputUnfinalized extends boolean,
    TOutputUnfinalized extends boolean,
    TInputRequest,
    TOutputRequest,
> = (
    opts: DataFactoryOptions<TInputUnfinalized>,
) => BlockDuplex<TInputValue, TOutputValue, TInputUnfinalized, TOutputUnfinalized, TInputRequest, TOutputRequest>

export type BlockTransformer<TInputValue, TOutputValue, TUnfinalized extends boolean, TRequest, TResult> = Omit<
    DataTransformer<Data<TInputValue, BlockRef>, Data<TOutputValue, BlockRef>, TUnfinalized, TRequest, TResult>,
    'cursorUtils'
>

export type BlockTransformerFactory<TInputValue, TOutputValue, TUnfinalized extends boolean, TRequest, TResult> = (
    opts: DataFactoryOptions<TUnfinalized>,
) => BlockTransformer<TInputValue, TOutputValue, TUnfinalized, TRequest, TResult>

export function createBlockTransformer<TInputValue, TOutputValue, TUnfinalized extends boolean, TRequest, TResult>(
    transformer: BlockTransformerFactory<TInputValue, TOutputValue, TUnfinalized, TRequest, TResult>,
): BlockDuplexFactory<TInputValue, TOutputValue, TUnfinalized, TUnfinalized, TRequest, TResult> {
    return createTransformer((opts) => {
        const blockTransformer = transformer(opts)
        return {
            ...blockTransformer,
            cursorUtils: BlockRefUtils,
        }
    })
}

export type BlockData<TValue> = Data<TValue, BlockRef>

export type BlockMessage<TValue, TUnfinalized extends boolean> = DataMessage<BlockData<TValue>, TUnfinalized>
