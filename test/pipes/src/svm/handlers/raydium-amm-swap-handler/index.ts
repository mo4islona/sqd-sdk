import * as raydiumCpmm from '../../contracts/raydium-cpmm'
import type {SolanaSwapTransfer} from '../../solana-swap-stream'
import type {Block, Instruction} from '../../utils'
import {RaydiumCpmmSwapBaseInputHandler} from './base-input-handler'
import {RaydiumCpmmSwapBaseOutputHandler} from './base-output-handler'

export const handlerRegistry = {
    [raydiumCpmm.instructions.swapBaseInput.d8]: RaydiumCpmmSwapBaseInputHandler,
    [raydiumCpmm.instructions.swapBaseOutput.d8]: RaydiumCpmmSwapBaseOutputHandler,
} as const

export function handleRaydiumAmm(instruction: Instruction, block: Block): SolanaSwapTransfer {
    const Handler = handlerRegistry[instruction.d8]

    if (!Handler) {
        throw new Error(`Unknown swap instruction: ${instruction.d8}`)
    }

    return new Handler(instruction, block).handleSwap()
}
