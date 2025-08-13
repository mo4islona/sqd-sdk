import {assert} from '../internal/misc'
import type {Data, DataFork} from './data'

export class ForkException<TId> extends Error {
    readonly isSqdForkException = true

    constructor(readonly fork: DataFork<TId>) {
        assert(fork.heads.length > 0)
        const lastRef = fork.heads[fork.heads.length - 1]
        super(`Fork exception at ${lastRef}`)
    }

    override get name(): string {
        return 'ForkException'
    }
}

export const isForkException = <TId>(err: unknown): err is ForkException<TId> =>
    err instanceof Error && !!(err as Partial<ForkException<TId>>).isSqdForkException
