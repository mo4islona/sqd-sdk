import {createTarget, type DataTarget, type DataWriteContext} from '../core'

export function createReducer<TCursor, TValue, TAccumulator, TQuery>(
    reducer: (accumulator: TAccumulator, value: TValue) => TAccumulator,
    initialValue: TAccumulator,
): DataTarget<TCursor, TValue, TQuery, Promise<TAccumulator>> {
    return createTarget({
        unfinalized: true,
        async write(context): Promise<TAccumulator> {
            let accumulator = initialValue

            for await (const message of context.read({cursor: undefined})) {
                switch (message.type) {
                    case 'batch': {
                        for (const item of message.data) {
                            accumulator = reducer(accumulator, item.value)
                        }
                        break
                    }
                    case 'fork':
                        return accumulator
                    default:
                        break
                }
            }

            return accumulator
        },
    })
}
