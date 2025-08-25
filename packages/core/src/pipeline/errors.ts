import {assert} from '../internal/misc'
import type {Data, DataFork} from './data'

export class ForkException<TId> extends Error {
    readonly isSqdForkException = true

    constructor(readonly fork: DataFork<TId>) {
        assert(fork.cursors.length > 0)
        const lastCursor = fork.cursors[fork.cursors.length - 1]
        super(`Fork exception at ${lastCursor}`)
    }

    override get name(): string {
        return 'ForkException'
    }
}

export const isForkException = <TId>(err: unknown): err is ForkException<TId> =>
    err instanceof Error && !!(err as Partial<ForkException<TId>>).isSqdForkException
