import {maybeLast} from '@sqd-sdk/core/internal/misc'
import type * as base from './types'
import type * as EVM from '@sqd-sdk/core/portal/evm'
import {BlockHeader, Transaction, Log, TraceCreate, TraceCall, TraceSuicide, TraceReward, StateDiff} from './entities'

export function createBlock<F extends base.FieldSelection>(
    raw: EVM.Block<{
        block: {number: true; hash: true}
        transaction: {transactionIndex: true}
        log: {transactionIndex: true; logIndex: true}
        trace: {transactionIndex: true; traceAddress: true; type: true}
        stateDiff: {transactionIndex: true; address: true; key: true}
    }>,
): base.Block<F> {
    const block = {} as base.Block<F>

    block.header = new BlockHeader<F>(raw.header, block) as any

    block.transactions = []
    block.logs = []
    block.traces = []
    block.stateDiffs = []

    raw.transactions.sort((a, b) => a.transactionIndex - b.transactionIndex)
    raw.logs.sort((a, b) => a.logIndex - b.logIndex)
    raw.traces.sort(traceCompare)

    let txs: (base.Transaction<F> | undefined)[] = new Array((maybeLast(raw.transactions)?.transactionIndex ?? -1) + 1)

    for (let tx of raw.transactions) {
        const transaction = new Transaction<F>(tx, block) as any
        txs[tx.transactionIndex] = transaction
        block.transactions.push(transaction)
    }

    for (let rawLog of raw.logs) {
        const transaction = txs[rawLog.transactionIndex]
        const log = new Log<F>(rawLog, block, transaction) as any
        block.logs.push(log)
        transaction?.logs.push(log)
    }

    for (let i = 0; i < raw.traces.length; i++) {
        let rawTrace = raw.traces[i]
        const transaction = txs[rawTrace.transactionIndex]

        let trace: base.Trace<F>
        switch (rawTrace.type) {
            case 'create':
                trace = new TraceCreate<F>(rawTrace, block, transaction) as any
                break
            case 'call':
                trace = new TraceCall<F>(rawTrace, block, transaction) as any
                break
            case 'suicide':
                trace = new TraceSuicide<F>(rawTrace, block, transaction) as any
                break
            case 'reward':
                trace = new TraceReward<F>(rawTrace, block, transaction) as any
                break
            default:
                throw new Error(`Unknown trace type: ${(rawTrace as any).type}`)
        }

        block.traces.push(trace)
        transaction?.traces.push(trace)

        // Set up parent-child relationships
        for (let j = i + 1; j < raw.traces.length; j++) {
            let rawNext = raw.traces[j]
            if (isDescendent(rawTrace, rawNext)) {
                let nextTrace = block.traces[j] // Will be created in next iteration
                if (rawNext.traceAddress.length === rawTrace.traceAddress.length + 1) {
                    // Direct child - set parent when the child is created
                }
            } else {
                break
            }
        }
    }

    // Set up parent-child relationships after all traces are created
    for (let i = 0; i < block.traces.length; i++) {
        let trace = block.traces[i]
        for (let j = i + 1; j < block.traces.length; j++) {
            let nextTrace = block.traces[j]
            if (isDescendent(raw.traces[i], raw.traces[j])) {
                trace.children.push(nextTrace)
                if (raw.traces[j].traceAddress.length === raw.traces[i].traceAddress.length + 1) {
                    ;(nextTrace as any).parent = trace
                }
            } else {
                break
            }
        }
    }

    for (let rawStateDiff of raw.stateDiffs) {
        const transaction = txs[rawStateDiff.transactionIndex]
        const stateDiff = new StateDiff<F>(rawStateDiff, block, transaction) as any
        block.stateDiffs.push(stateDiff)
        transaction?.stateDiffs.push(stateDiff)
    }

    return block
}

interface TraceAddress {
    transactionIndex: number
    traceAddress: number[]
}

function traceCompare(a: TraceAddress, b: TraceAddress) {
    return a.transactionIndex - b.transactionIndex || addressCompare(a.traceAddress, b.traceAddress)
}

function addressCompare(a: number[], b: number[]): number {
    for (let i = 0; i < Math.min(a.length, b.length); i++) {
        let order = a[i] - b[i]
        if (order) return order
    }
    return a.length - b.length // this differs from substrate call ordering
}

function isDescendent(parent: TraceAddress, child: TraceAddress): boolean {
    if (parent.transactionIndex !== child.transactionIndex) return false
    if (parent.traceAddress.length >= child.traceAddress.length) return false
    for (let i = 0; i < parent.traceAddress.length; i++) {
        if (parent.traceAddress[i] !== child.traceAddress[i]) return false
    }
    return true
}
