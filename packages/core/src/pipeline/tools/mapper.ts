import type {DataDuplex, DataReadOptions, DataTargetFactoryOptions} from '../core'
import {createTransformer} from './transformer'

export function createMapper<TCursor, TInValue, TOutValue, TRequest>(
    mapper: (input: TInValue) => TOutValue,
): (opts: DataTargetFactoryOptions) => DataDuplex<TCursor, TInValue, TRequest, TCursor, TOutValue, TRequest> {
    return createTransformer<TCursor, TCursor, TInValue, TOutValue, TRequest, TRequest>((opts) => {
        return {
            cursorUtils: opts.cursorUtils,
            read: async function* (readOpts: DataReadOptions<TCursor, TRequest>) {
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
