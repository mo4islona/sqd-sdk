import type {DataDuplex, DataReadRequest, DataTargetFactoryOptions} from '../core'
import {createTransformer} from './transformer'

export function createFilter<TCursor, TValue, TQuery>(
    predicate: (value: TValue) => boolean,
): (opts: DataTargetFactoryOptions) => DataDuplex<TCursor, TCursor, TValue, TValue, TQuery, TQuery> {
    return createTransformer<TCursor, TCursor, TValue, TValue, TQuery, TQuery>((opts) => {
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
                                data: message.data.filter((d) => predicate(d.value)),
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
