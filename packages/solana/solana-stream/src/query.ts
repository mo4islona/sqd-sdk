import type * as Solana from '@belopash/core/portal/solana'
import {mergeSelection, type MergeSelection} from '@belopash/core/internal/selection'
import {applyRangeBound, mergeRangeRequests, type Range, type RangeRequest} from '@belopash/core/internal/range'

// TODO: is it needed?
export type {
    DataRequest,
    TransactionRequest,
    InstructionRequest,
    LogRequest,
    BalanceRequest,
    TokenBalanceRequest,
    RewardRequest,
} from '@belopash/core/portal/solana'

export type RequestOptions<R> = {range?: Range; request: R}
export type LogRequestOptions = RequestOptions<Solana.LogRequest>
export type TransactionRequestOptions = RequestOptions<Solana.TransactionRequest>
export type InstructionRequestOptions = RequestOptions<Solana.InstructionRequest>
export type TokenBalanceRequestOptions = RequestOptions<Solana.TokenBalanceRequest>
export type BalanceRequestOptions = RequestOptions<Solana.BalanceRequest>
export type RewardRequestOptions = RequestOptions<Solana.RewardRequest>

export type SolanaDataRequest = Solana.DataRequest

export type SolanaDataRequestRange = RangeRequest<SolanaDataRequest>

export class SolanaQueryBuilder<F extends Solana.FieldSelection = {block: {number: true; hash: true}}> {
    private range: Range = {from: 0}
    private requests: RangeRequest<SolanaDataRequest>[] = []

    private addRequest(type: keyof Solana.DataRequest, options: RequestOptions<any>): this {
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

    addReward(options: RewardRequestOptions): this {
        return this.addRequest('rewards', options)
    }

    addBalance(options: BalanceRequestOptions): this {
        return this.addRequest('balances', options)
    }

    addTokenBalance(options: TokenBalanceRequestOptions): this {
        return this.addRequest('tokenBalances', options)
    }

    addInstruction(options: InstructionRequestOptions): this {
        return this.addRequest('instructions', options)
    }

    setRange(range: Range): this {
        this.range = range
        return this
    }

    build(): SolanaDataRequestRange[] {
        return applyRangeBound(mergeRangeRequests(this.requests, mergeDataRequests), this.range)
    }
}

export function mergeDataRequests(...requests: Solana.DataRequest[]): Solana.DataRequest {
    let res: Solana.DataRequest = {}
    for (let req of requests) {
        res.transactions = concaTQueryLists(res.transactions, req.transactions)
        res.logs = concaTQueryLists(res.logs, req.logs)
        res.balances = concaTQueryLists(res.balances, req.balances)
        res.tokenBalances = concaTQueryLists(res.tokenBalances, req.tokenBalances)
        res.rewards = concaTQueryLists(res.rewards, req.rewards)
        res.instructions = concaTQueryLists(res.instructions, req.instructions)
        if (res.includeAllBlocks || req.includeAllBlocks) {
            res.includeAllBlocks = true
        }
    }
    return res
}

export function mergeRequests(...requests: RangeRequest<Solana.DataRequest>[]): RangeRequest<Solana.DataRequest>[] {
    return mergeRangeRequests(requests, mergeDataRequests)
}

function concaTQueryLists<T extends object>(a?: T[], b?: T[]): T[] | undefined {
    let result = [...(a || []), ...(b || [])]
    return result.length ? result : undefined
}

function mapRequest<T>(options: RequestOptions<T>): T {
    return {...options.request}
}
