// import {
//     createSource,
//     createTarget,
//     stream,
//     type DataMessage,
//     type DataSource,
//     type DataTarget,
//     type DataTargetFactoryOptions,
//     type DataReadRequest,
//     type Stream,
// } from './core'
import { DataCursor } from './cursor'

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
    serialize: (ref: BlockRef) => JSON.stringify(ref),
    deserialize: (ref: string) => JSON.parse(ref),
}

// export type BlockSource<TValue, TQuery> = DataSource<BlockRef, TValue, TQuery>

// export type BlockSourceFactory<TValue, TQuery> = () => BlockSource<TValue, TQuery>

// export function createBlockSource<TValue, TQuery>(
//     source:
//         | Omit<BlockSource<TValue, TQuery>, 'cursorUtils'>
//         | (() => Omit<BlockSource<TValue, TQuery>, 'cursorUtils'>),
// ): BlockSourceFactory<TValue, TQuery> {
//     return () => {
//         const s =
//             typeof source === 'function'
//                 ? (source as () => Omit<BlockSource<TValue, TQuery>, 'cursorUtils'>)()
//                 : source
//         return createSource({
//             ...s,
//             cursorUtils: BlockRefUtils,
//         })
//     }
// }

// export type BlockTarget<TValue, TQuery, TResult> = DataTarget<BlockRef, TValue, TQuery, TResult>

// export type BlockTargetFactory<TValue, TQuery, TResult> = (
//     opts: DataTargetFactoryOptions,
// ) => BlockTarget<TValue, TQuery, TResult>

// export function createBlockTarget<TValue, TQuery, TResult>(
//     target: BlockTarget<TValue, TQuery, TResult> | BlockTargetFactory<TValue, TQuery, TResult>,
// ): BlockTargetFactory<TValue, TQuery, TResult> {
//     return createTarget(target)
// }

// export type BlockDuplex<TInputValue, TOutputValue, TInpuTQuery, TOutpuTQuery> = DataTarget<
//     BlockRef,
//     TInputValue,
//     TInpuTQuery,
//     Stream<BlockRef, TOutputValue, TOutpuTQuery>
// >

// export type BlockDuplexFactory<TInputValue, TOutputValue, TInpuTQuery, TOutpuTQuery> = (
//     opts: DataTargetFactoryOptions,
// ) => BlockDuplex<TInputValue, TOutputValue, TInpuTQuery, TOutpuTQuery>

// export type BlockTransformer<TInputValue, TOutputValue, TQuery, TResult> = {
//     unfinalized: boolean
//     transform: (
//         write: {
//             cursorUtils: DataCursorUtils<BlockRef>
//             read: (opts: DataReadRequest<BlockRef, TQuery>) => AsyncIterable<DataMessage<BlockRef, TInputValue>>
//         },
//         read: DataReadRequest<BlockRef, TResult>,
//     ) => AsyncIterableIterator<DataMessage<BlockRef, TOutputValue>>
// }

// export type BlockTransformerFactory<TInputValue, TOutputValue, TQuery, TResult> = (
//     opts: DataTargetFactoryOptions,
// ) => BlockTransformer<TInputValue, TOutputValue, TQuery, TResult>

// export function createBlockTransformer<TInputValue, TOutputValue, TQuery, TResult>(
//     transformerFactory: BlockTransformerFactory<TInputValue, TOutputValue, TQuery, TResult>,
// ): BlockDuplexFactory<TInputValue, TOutputValue, TQuery, TResult> {
//     return createTarget((opts: DataTargetFactoryOptions) => {
//         return {
//             unfinalized: opts.unfinalized,
//             write: (writeOpts) => {
//                 const transformer = transformerFactory({unfinalized: opts.unfinalized})
//                 return stream(() =>
//                     createSource<BlockRef, TOutputValue, TResult>({
//                         unfinalized: transformer.unfinalized,
//                         cursorUtils: BlockRefUtils,
//                         read: (readOpts: DataReadRequest<BlockRef, TResult>) =>
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
