import {createTarget, type DataTarget, type DataWriteContext} from '../core'

export function createForEach<TCursor, TValue, TQuery>(
    callback: (value: TValue) => void | Promise<void>,
): DataTarget<TCursor, TValue, TQuery, Promise<void>> {
    return createTarget({
        unfinalized: true,
        async write(context: DataWriteContext<TCursor, TValue, TQuery>): Promise<void> {
            for await (const message of context.read({cursor: undefined})) {
                switch (message.type) {
                    case 'batch': {
                        for (const item of message.data) {
                            await callback(item.value)
                        }
                        break
                    }
                    case 'fork':
                        return
                    default:
                        break
                }
            }
        },
    })
}
