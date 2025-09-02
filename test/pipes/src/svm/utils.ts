// TODO: remove all any types

import assert from 'node:assert'
import {getInstructionData} from '@belopash/solana-stream'
import {PublicKey} from '@solana/web3.js'
import {toHex} from '@subsquid/util-internal-hex'
import * as token2022Program from './contracts/token-2022-program'
import * as tokenProgram from './contracts/token-program'

export type Instruction = any
export type Block = any

interface DecodedTransfer {
    accounts: {
        destination: string
        source: string
        authority?: string
        owner?: string
    }
    data: {
        amount: bigint
    }
}

export function getInstructionBalances(ins: Instruction, block: Block) {
    return block.tokenBalances?.filter((t: any) => t.transactionIndex === ins.transactionIndex) || []
}

export function getNextInstruction(instruction: Instruction, instructions: Instruction[]) {
    const index = instructions.findIndex((i) => i.instructionAddress === instruction.instructionAddress)
    return instructions[index + 1]
}

export function getTransactionHash(ins: Instruction, block: Block) {
    const tx = getTransaction(ins, block)
    return tx.signatures[0]
}

export function getTransactionAccount(ins: Instruction, block: Block) {
    const tx = getTransaction(ins, block)
    return tx.accountKeys[0]
}

export function getTransaction(ins: Instruction, block: Block) {
    const tx = block.transactions.find((t: any) => t.transactionIndex === ins.transactionIndex)
    assert(tx, 'transaction not found')

    return tx
}

/**
 * Get the inner token transfer instructions of a parent instruction at a given level
 * @param parent
 * @param instructions
 * @param level
 * @returns
 */
export function getInnerTransfersByLevel(parent: Instruction, instructions: Instruction[], level: number) {
    return instructions.filter((inner) => {
        if (inner.transactionIndex !== parent.transactionIndex) return false
        if (inner.instructionAddress.length !== parent.instructionAddress.length + level) return false
        if (inner.programId !== tokenProgram.programId && inner.programId !== token2022Program.programId) return false

        if (
            getInstructionD1(inner) !== tokenProgram.instructions.transfer.d1 &&
            getInstructionD1(inner) !== tokenProgram.instructions.transferChecked.d1 &&
            getInstructionD1(inner) !== token2022Program.instructions.transfer.d1 &&
            getInstructionD1(inner) !== token2022Program.instructions.transferChecked.d1
        )
            return false

        // All child instructions should have the same prefix as the parent
        return parent.instructionAddress.every((v: any, i: any) => v === inner.instructionAddress[i])
    })
}

export function getInstructionD1(instruction: Instruction) {
    return toHex(getInstructionData(instruction)).slice(0, 4)
}

/**
 * Returns decoded token transfers from direct child instructions
 * @param ins
 * @param block
 * @returns
 */
export function getDecodedInnerTransfers(ins: Instruction, block: Block): DecodedTransfer[] {
    return getInnerTransfersByLevel(ins, block.instructions, 1).map((t) => {
        const programId = t.programId
        const d1 = getInstructionD1(t)

        if (programId === tokenProgram.programId) {
            if (d1 === tokenProgram.instructions.transferChecked.d1) {
                return tokenProgram.instructions.transferChecked.decode(t)
            }
            if (d1 === tokenProgram.instructions.transfer.d1) {
                return tokenProgram.instructions.transfer.decode(t)
            }
        }
        if (programId === token2022Program.programId) {
            if (d1 === token2022Program.instructions.transferChecked.d1) {
                return token2022Program.instructions.transferChecked.decode(t)
            }
            if (d1 === token2022Program.instructions.transfer.d1) {
                return token2022Program.instructions.transfer.decode(t)
            }
        }

        throw new Error(`Unknown token transfer instruction: ${t.d1}`)
    })
}

/**
 * Convert a sqrtPrice in x64, commmon in concentrated liquidity protocols, to a human readable price
 * @param sqrtPriceX64
 * @param tokenADecimals
 * @param tokenBDecimals
 * @returns
 */
export function sqrtPriceX64ToPrice(sqrtPriceX64: bigint, tokenADecimals: number, tokenBDecimals: number): number {
    const price = (Number(sqrtPriceX64) / 2 ** 64) ** 2 * 10 ** (tokenADecimals - tokenBDecimals)
    return Number(price)
}

/**
 * Get all the logs of an instruction
 * @param ins
 * @param block
 * @returns
 */
export function getInstructionLogs(ins: Instruction, block: Block) {
    return (
        block.logs?.filter(
            (log: any) =>
                log.transactionIndex === ins.transactionIndex &&
                log.instructionAddress.length === ins.instructionAddress.length &&
                log.instructionAddress.every((v: any, i: any) => v === ins.instructionAddress[i]),
        ) || []
    )
}

/**
 * Sort two accounts by their public key
 * @param accountA
 * @param accountB
 * @returns
 */
export function sortAccounts(accountA: string, accountB: string): [string, string] {
    const aBytes = new PublicKey(accountA).toBytes()
    const bBytes = new PublicKey(accountB).toBytes()

    for (let i = 0; i < 32; i++) {
        if (aBytes[i] < bBytes[i]) {
            return [accountA, accountB]
        }
        if (aBytes[i] > bBytes[i]) {
            return [accountB, accountA]
        }
    }

    throw new Error('Accounts must be different')
}
