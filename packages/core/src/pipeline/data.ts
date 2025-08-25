export interface DataCursorUtils<TValue> {
    compare(a: TValue, b: TValue): DataCursor.CompareResult
    serialize(value: TValue): unknown
    deserialize(value: unknown): TValue
}

export namespace DataCursor {
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
    cursor: TId
}

export type DataCursor<TData extends Data> = TData['cursor']

export interface DataBatch<TData extends Data, TUnfinalized extends boolean = boolean> {
    readonly data: TData[]
    readonly finalizedHead: TUnfinalized extends false ? DataCursor<TData> : DataCursor<TData> | undefined
    readonly head: DataCursor<TData>
    readonly cursor: DataCursor<TData>
}

export interface DataFork<TCursor> {
    readonly cursors: TCursor[]
}
