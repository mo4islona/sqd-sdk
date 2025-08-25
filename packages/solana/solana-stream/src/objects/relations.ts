import {bisect, maybeLast} from '@sqd-sdk/core/internal/misc'
import type * as base from './types'
import type * as solana from '@sqd-sdk/core/portal/solana'
import {BlockHeader, Instruction, Transaction, LogMessage, Balance, Reward, TokenBalance} from './entities'

export function createBlock<F extends base.FieldSelection>(
    raw: solana.Block<{
        block: {number: true; hash: true}
        transaction: {transactionIndex: true}
        log: {transactionIndex: true; logIndex: true; instructionAddress: true}
        instruction: {transactionIndex: true; instructionAddress: true}
        balance: {transactionIndex: true; account: true}
        tokenBalance: {transactionIndex: true; account: true}
        reward: {pubkey: true}
    }>,
): base.Block<F> {
    const block = {} as base.Block<F>

    block.header = new BlockHeader<F>(raw.header, block) as any

    block.transactions = []
    block.instructions = []
    block.logs = []
    block.balances = []
    block.tokenBalances = []
    block.rewards = []

    raw.transactions.sort((a, b) => a.transactionIndex - b.transactionIndex)
    raw.instructions.sort(instructionCompare)
    raw.logs.sort((a, b) => a.transactionIndex - b.transactionIndex || a.logIndex - b.logIndex)

    let txs: (base.Transaction<F> | undefined)[] = new Array((maybeLast(raw.transactions)?.transactionIndex ?? -1) + 1)

    for (let tx of raw.transactions) {
        const transaction = new Transaction<F>(tx, block) as any
        txs[tx.transactionIndex] = transaction
        block.transactions.push(transaction)
    }

    for (let i = 0; i < raw.instructions.length; i++) {
        let tx = txs[raw.instructions[i].transactionIndex]
        let ins = new Instruction<F>(raw.instructions[i], block, tx) as any
        block.instructions.push(ins)
        tx?.instructions.push(ins)

        for (let j = i + 1; j < raw.instructions.length; j++) {
            let rawNext = raw.instructions[j]
            if (isInner(ins, rawNext)) {
                let nextIns = new Instruction<F>(rawNext, block, tx, ins) as any
                ins.inner.push(nextIns)
            } else {
                break
            }
        }
    }

    for (let rawLog of raw.logs) {
        const transaction = txs[rawLog.transactionIndex]

        let instruction: base.Instruction<F> | undefined
        for (let ins of block.instructions) {
            if (isInner(ins, rawLog) && ins.instructionAddress.length === rawLog.instructionAddress.length) {
                instruction = ins
                break
            }
        }

        const log = new LogMessage<F>(rawLog, block, transaction, instruction) as any
        block.logs.push(log)
        transaction?.logs.push(log)

        for (let ins of block.instructions) {
            if (isInner(ins, rawLog)) {
                ins.logs.push(log)
            }
        }
    }

    for (let rawBalance of raw.balances) {
        const transaction = txs[rawBalance.transactionIndex]
        const balance = new Balance<F>(rawBalance, block, transaction) as any
        block.balances.push(balance)
        transaction?.balances.push(balance)
    }

    for (let rawTokenBalance of raw.tokenBalances) {
        const transaction = txs[rawTokenBalance.transactionIndex]
        const tokenBalance = new TokenBalance<F>(rawTokenBalance, block, transaction) as any
        block.tokenBalances.push(tokenBalance)
        transaction?.tokenBalances.push(tokenBalance)
    }

    for (let rawReward of raw.rewards) {
        const reward = new Reward<F>(rawReward, block) as any
        block.rewards.push(reward)
    }

    return block
}

interface InstructionAddress {
    transactionIndex: number
    instructionAddress: number[]
}

function isInner(parent: InstructionAddress, inner: InstructionAddress): boolean {
    if (parent.transactionIndex !== inner.transactionIndex) return false
    if (parent.instructionAddress.length > inner.instructionAddress.length) return false
    for (let i = 0; i < parent.instructionAddress.length; i++) {
        if (parent.instructionAddress[i] !== inner.instructionAddress[i]) return false
    }
    return true
}

function instructionCompare(a: InstructionAddress, b: InstructionAddress): number {
    return a.transactionIndex - b.transactionIndex || addressCompare(a.instructionAddress, b.instructionAddress)
}

function addressCompare(a: number[], b: number[]): number {
    for (let i = 0; i < Math.min(a.length, b.length); i++) {
        let order = a[i] - b[i]
        if (order) return order
    }
    return a.length - b.length
}
