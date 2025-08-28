import type {DataDuplex, DataReadRequest, DataTargetFactoryOptions} from '../core'
import {createTransformer} from './transformer'

export function createMapper<TCursor, TInValue, TOutValue, TQuery>(
    mapper: (input: TInValue) => TOutValue,
): (opts: DataTargetFactoryOptions) => DataDuplex<TCursor, TCursor, TInValue, TOutValue, TQuery, TQuery> {
    return createTransformer<TCursor, TCursor, TInValue, TOutValue, TQuery, TQuery>((opts) => {
        return {
            cursorUtils: opts.cursorUtils,
            read: async function* (readOpts: DataReadRequest<TCursor, TQuery>) {
                for await (const message of opts.read(readOpts)) {
                    switch (message.type) {
                        case 'batch':
                            yield {
                                type: 'batch',
                                cursor: message.cursor,
                                head: message.head,
                                finalizedHead: message.finalizedHead,
                                data: message.data.map((d) => ({
                                    cursor: d.cursor,
                                    value: mapper(d.value),
                                })),
                            }
                            break
                        default:
                            yield message
                    }
                }
            },
        }
    })
}
