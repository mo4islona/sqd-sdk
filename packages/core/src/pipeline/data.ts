export interface DataRef<T> {
    readonly value: T
    compare(other: DataRef<T>): DataRef.CompareResult
}

export namespace DataRef {
    export class CompareResult {
        constructor(readonly value: 'eq' | 'lt' | 'gt' | 'fk') {}

        get isFork() {
            return this.value === 'fk'
        }

        get isLess() {
            return this.value === 'lt'
        }

        get isGreater() {
            return this.value === 'gt'
        }

        get isEqual() {
            return this.value === 'eq'
        }

        get isLessOrEqual() {
            return this.value === 'lt' || this.value === 'eq'
        }

        get isGreaterOrEqual() {
            return this.value === 'gt' || this.value === 'eq'
        }
    }

    export const Less = new CompareResult('lt')
    export const Greater = new CompareResult('gt')
    export const Equal = new CompareResult('eq')
    export const Fork = new CompareResult('fk')
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
