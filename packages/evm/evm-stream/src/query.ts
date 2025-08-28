import type * as EVM from '@belopash/core/portal/evm'
import {applyRangeBound, mergeRangeRequests, type Range, type RangeRequest} from '@belopash/core/internal/range'

// TODO: is it needed?
export type {
    DataRequest,
    TransactionRequest,
    TraceRequest,
    StateDiffRequest,
    LogRequest,
} from '@belopash/core/portal/evm'

export type RequestOptions<R> = {range?: Range; request: R}
export type LogRequestOptions = RequestOptions<EVM.LogRequest>
export type TransactionRequestOptions = RequestOptions<EVM.TransactionRequest>
export type TraceRequestOptions = RequestOptions<EVM.TraceRequest>
export type StateDiffRequestOptions = RequestOptions<EVM.StateDiffRequest>

export type EvmDataRequest = EVM.DataRequest

export type EvmDataRequestRange = RangeRequest<EvmDataRequest>

export class EvmQueryBuilder<F extends EVM.FieldSelection = {block: {number: true; hash: true}}> {
    private range: Range = {from: 0}
    private requests: RangeRequest<EvmDataRequest>[] = []

    private addRequest(type: keyof EVM.DataRequest, options: RequestOptions<any>): this {
        this.requests.push({
            range: options.range ?? {from: 0},
            request: {
                [type]: [mapRequest(options)],
            },
        })
        return this
    }

    includeAllBlocks(range?: Range): this {
        this.requests.push({range: range ?? {from: 0}, request: {includeAllBlocks: true}})
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

    build(): EvmDataRequestRange[] {
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
    return {...options.request}
}
