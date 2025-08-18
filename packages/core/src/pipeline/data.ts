export interface DataRef<TValue> {
    compare(a: TValue, b: TValue): DataRef.CompareResult
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

export type Data<TValue = unknown, TId = unknown> = {
    value: TValue
    id: TId
}

export type DataId<TData extends Data> = TData['id']

export interface DataBatch<TData extends Data> {
    readonly data: TData[]
    readonly finalizedHead: DataId<TData> | undefined
    readonly head: DataId<TData>
    readonly offset: DataId<TData>
}

export interface DataFork<TId> {
    readonly heads: TId[]
}
