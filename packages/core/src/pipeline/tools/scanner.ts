import type {DataDuplex, DataReadRequest, DataTargetFactoryOptions, DataBatchItem} from '../core'
import {createTransformer} from './transformer'

export function createScanner<TCursor, TValue, TAccumulator, TQuery>(
    reducer: (accumulator: TAccumulator, value: TValue) => TAccumulator,
    initialValue: TAccumulator,
): (opts: DataTargetFactoryOptions) => DataDuplex<TCursor, TCursor, TValue, TAccumulator, TQuery, TQuery> {
    return createTransformer<TCursor, TCursor, TValue, TAccumulator, TQuery, TQuery>((opts) => {
        return {
            cursorUtils: opts.cursorUtils,
            read: async function* (readOpts: DataReadRequest<TCursor, TQuery>) {
                let accumulator = initialValue

                for await (const message of opts.read(readOpts)) {
                    switch (message.type) {
                        case 'batch': {
                            const reducedData: DataBatchItem<TCursor, TAccumulator>[] = []

                            for (const item of message.data) {
                                accumulator = reducer(accumulator, item.value)
                                reducedData.push({
                                    cursor: item.cursor,
                                    value: accumulator,
                                })
                            }

                            yield {
                                type: 'batch',
                                cursor: message.cursor,
                                head: message.head,
                                finalizedHead: message.finalizedHead,
                                data: reducedData,
                            }
                            break
                        }
                        case 'fork':
                            // Reset accumulator on fork
                            accumulator = initialValue
                            yield message
                            break
                        default:
                            yield message
                    }
                }
            },
        }
    })
}
