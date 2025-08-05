export interface DataRef<T> {
    readonly value: T
    compare(other: DataRef<T>): DataRef.CompareResult
}

export namespace DataRef {
    export enum Compare {
        Equal = 0,
        Less = 1,
        Greater = 2,
        Fork = 3,
    }

    export class CompareResult {
        constructor(readonly value: Compare) {}

        get isFork() {
            return this.value === Compare.Fork
        }

        get isLess() {
            return this.value === Compare.Less
        }

        get isGreater() {
            return this.value === Compare.Greater
        }

        get isEqual() {
            return this.value === Compare.Equal
        }

        get isLessOrEqual() {
            return this.value === Compare.Less || this.value === Compare.Equal
        }

        get isGreaterOrEqual() {
            return this.value === Compare.Greater || this.value === Compare.Equal
        }
    }

    export const Less = new CompareResult(Compare.Less)
    export const Greater = new CompareResult(Compare.Greater)
    export const Equal = new CompareResult(Compare.Equal)
    export const Fork = new CompareResult(Compare.Fork)
}

export interface Data<V = unknown, R = unknown> {
    value: V
    ref: DataRef<R>
}

export interface DataBatch<T extends Data> {
    readonly data: T[]
    readonly finalizedHead: T['ref'] | undefined
    readonly head: T['ref']
    readonly offset: T['ref']
}

export interface DataFork<T extends Data> {
    readonly heads: T['ref'][]
}
