// import {
//     createSource,
//     createTarget,
//     stream,
//     type DataMessage,
//     type DataSource,
//     type DataTarget,
//     type DataTargetFactoryOptions,
//     type DataReadOptions,
//     type Stream,
// } from './core'
import {DataCursor, type DataCursorUtils} from './cursor'

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

// export type BlockSource<TValue, TRequest> = DataSource<BlockRef, TValue, TRequest>

// export type BlockSourceFactory<TValue, TRequest> = () => BlockSource<TValue, TRequest>

// export function createBlockSource<TValue, TRequest>(
//     source:
//         | Omit<BlockSource<TValue, TRequest>, 'cursorUtils'>
//         | (() => Omit<BlockSource<TValue, TRequest>, 'cursorUtils'>),
// ): BlockSourceFactory<TValue, TRequest> {
//     return () => {
//         const s =
//             typeof source === 'function'
//                 ? (source as () => Omit<BlockSource<TValue, TRequest>, 'cursorUtils'>)()
//                 : source
//         return createSource({
//             ...s,
//             cursorUtils: BlockRefUtils,
//         })
//     }
// }

// export type BlockTarget<TValue, TRequest, TResult> = DataTarget<BlockRef, TValue, TRequest, TResult>

// export type BlockTargetFactory<TValue, TRequest, TResult> = (
//     opts: DataTargetFactoryOptions,
// ) => BlockTarget<TValue, TRequest, TResult>

// export function createBlockTarget<TValue, TRequest, TResult>(
//     target: BlockTarget<TValue, TRequest, TResult> | BlockTargetFactory<TValue, TRequest, TResult>,
// ): BlockTargetFactory<TValue, TRequest, TResult> {
//     return createTarget(target)
// }

// export type BlockDuplex<TInputValue, TOutputValue, TInputRequest, TOutputRequest> = DataTarget<
//     BlockRef,
//     TInputValue,
//     TInputRequest,
//     Stream<BlockRef, TOutputValue, TOutputRequest>
// >

// export type BlockDuplexFactory<TInputValue, TOutputValue, TInputRequest, TOutputRequest> = (
//     opts: DataTargetFactoryOptions,
// ) => BlockDuplex<TInputValue, TOutputValue, TInputRequest, TOutputRequest>

// export type BlockTransformer<TInputValue, TOutputValue, TRequest, TResult> = {
//     unfinalized: boolean
//     transform: (
//         write: {
//             cursorUtils: DataCursorUtils<BlockRef>
//             read: (opts: DataReadOptions<BlockRef, TRequest>) => AsyncIterable<DataMessage<BlockRef, TInputValue>>
//         },
//         read: DataReadOptions<BlockRef, TResult>,
//     ) => AsyncIterableIterator<DataMessage<BlockRef, TOutputValue>>
// }

// export type BlockTransformerFactory<TInputValue, TOutputValue, TRequest, TResult> = (
//     opts: DataTargetFactoryOptions,
// ) => BlockTransformer<TInputValue, TOutputValue, TRequest, TResult>

// export function createBlockTransformer<TInputValue, TOutputValue, TRequest, TResult>(
//     transformerFactory: BlockTransformerFactory<TInputValue, TOutputValue, TRequest, TResult>,
// ): BlockDuplexFactory<TInputValue, TOutputValue, TRequest, TResult> {
//     return createTarget((opts: DataTargetFactoryOptions) => {
//         return {
//             unfinalized: opts.unfinalized,
//             write: (writeOpts) => {
//                 const transformer = transformerFactory({unfinalized: opts.unfinalized})
//                 return stream(() =>
//                     createSource<BlockRef, TOutputValue, TResult>({
//                         unfinalized: transformer.unfinalized,
//                         cursorUtils: BlockRefUtils,
//                         read: (readOpts: DataReadOptions<BlockRef, TResult>) =>
//                             transformer.transform(
//                                 {
//                                     cursorUtils: writeOpts.cursorUtils,
//                                     read: writeOpts.read,
//                                 },
//                                 {
//                                     cursor: readOpts.cursor,
//                                     request: readOpts.request,
//                                 },
//                             ),
//                     }),
//                 )
//             },
//         }
//     })
// }

// export type BlockMessage<TValue> = DataMessage<BlockRef, TValue>
