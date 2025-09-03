import { type Range, type RangeRequest, applyRangeBound, mergeRangeRequests } from '@belopash/core/internal/range'
import type * as EVM from '@belopash/core/portal/evm'

// TODO: is it needed?
export type {
    DataRequest,
    TransactionRequest,
    TraceRequest,
    StateDiffRequest,
    LogRequest,
} from '@belopash/core/portal/evm'

export type RequestOptions<R> = { range?: Range; request: R }
export type LogRequestOptions = RequestOptions<EVM.LogRequest>
export type TransactionRequestOptions = RequestOptions<EVM.TransactionRequest>
export type TraceRequestOptions = RequestOptions<EVM.TraceRequest>
export type StateDiffRequestOptions = RequestOptions<EVM.StateDiffRequest>

export type EvmDataRequest = EVM.DataRequest

export type EvmDataRequestRange = RangeRequest<EvmDataRequest>

function mergeDeep<T extends object, U extends object>(obj1: T, obj2: U): T & U {
    const result: any = { ...obj1 }
    for (const key in obj2) {
        if (
            // biome-ignore lint/suspicious/noPrototypeBuiltins: <explanation>
            obj2.hasOwnProperty(key) &&
            typeof obj2[key] === 'object' &&
            obj2[key] !== null &&
            typeof result[key] === 'object' &&
            result[key] !== null
        ) {
            result[key] = mergeDeep(result[key], obj2[key])
        } else {
            result[key] = obj2[key]
        }
    }
    return result
}

export class EvmQueryBuilder {
    protected range: Range = { from: 0 }
    protected requests: RangeRequest<EvmDataRequest>[] = []
    protected fields: EVM.FieldSelection = {}

    merge(instance?: EvmQueryBuilder) {
        if (!instance) return this

        this.requests = [...instance.requests, ...this.requests]
        this.addFields(instance.getFields())
        this.setRange(instance.range)

        return this
    }

    addFields(fields: EVM.FieldSelection): this {
        this.fields = mergeDeep(this.fields, fields)
        return this
    }

    getFields() {
        return this.fields
    }

    private addRequest(type: keyof EVM.DataRequest, options: RequestOptions<any>): this {
        this.requests.push({
            range: options.range ?? { from: 0 },
            request: {
                [type]: [mapRequest(options)],
            },
        })
        return this
    }

    includeAllBlocks(range?: Range): this {
        this.requests.push({ range: range ?? { from: 0 }, request: { includeAllBlocks: true } })
        return this
    }

    addLog(options: LogRequestOptions): this {
        return this.addRequest('logs', options)
    }

    addTransaction(options: TransactionRequestOptions): this {
        return this.addRequest('transactions', options)
    }

    addTrace(options: TraceRequestOptions): this {
        return this.addRequest('traces', options)
    }

    addStateDiff(options: StateDiffRequestOptions): this {
        return this.addRequest('stateDiffs', options)
    }

    setRange(range: Range): this {
        this.range = range
        return this
    }

    calculateRanges(): EvmDataRequestRange[] {
        return applyRangeBound(mergeRangeRequests(this.requests, mergeDataRequests), this.range)
    }
}

export function mergeDataRequests(...requests: EVM.DataRequest[]): EVM.DataRequest {
    let res: EVM.DataRequest = {}
    for (let req of requests) {
        res.transactions = concaTQueryLists(res.transactions, req.transactions)
        res.logs = concaTQueryLists(res.logs, req.logs)
        res.traces = concaTQueryLists(res.traces, req.traces)
        res.stateDiffs = concaTQueryLists(res.stateDiffs, req.stateDiffs)
        if (res.includeAllBlocks || req.includeAllBlocks) {
            res.includeAllBlocks = true
        }
    }
    return res
}

export function mergeRequests(...requests: RangeRequest<EVM.DataRequest>[]): RangeRequest<EVM.DataRequest>[] {
    return mergeRangeRequests(requests, mergeDataRequests)
}

function concaTQueryLists<T extends object>(a?: T[], b?: T[]): T[] | undefined {
    let result = [...(a || []), ...(b || [])]
    return result.length ? result : undefined
}

function mapRequest<T>(options: RequestOptions<T>): T {
    return { ...options.request }
}
