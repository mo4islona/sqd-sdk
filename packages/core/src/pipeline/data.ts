export interface DataRef<TValue> {
    readonly value: TValue
    compare(other: DataRef<TValue>): DataRef.CompareResult
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

export interface Data<TValue = unknown, TRef = unknown> {
    value: TValue
    ref: DataRef<TRef>
}

export interface DataBatch<TData extends Data> {
    readonly data: TData[]
    readonly finalizedHead: TData['ref'] | undefined
    readonly head: TData['ref']
    readonly offset: TData['ref']
}

export interface DataFork<TData extends Data> {
    readonly heads: TData['ref'][]
}
