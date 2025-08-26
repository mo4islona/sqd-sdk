import {assert} from '../internal/misc'

export class ForkException<TCursor> extends Error {
    readonly isSqdForkException = true

    constructor(readonly cursors: TCursor[]) {
        assert(cursors.length > 0)
        const lastCursor = cursors[cursors.length - 1]
        super(`Fork exception at ${lastCursor}`)
    }

    override get name(): string {
        return 'ForkException'
    }
}

export const isForkException = <TCursor>(err: unknown): err is ForkException<TCursor> =>
    err instanceof Error && !!(err as Partial<ForkException<TCursor>>).isSqdForkException
