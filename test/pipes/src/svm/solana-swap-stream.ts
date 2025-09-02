import type {BlockRef} from '@belopash/core/pipeline'
import type {PortalClientOptions} from '@belopash/core/portal'
import {
    type Block,
    SolanaQueryBuilder,
    createSolanaPortalSource,
    getInstructionDescriptor,
} from '@belopash/solana-stream'
import {createCacheLayer} from '../cache-layer'
import * as meteoraDamm from './contracts/meteora-damm'
import * as meteoraDlmm from './contracts/meteora-dlmm'
import * as whirlpool from './contracts/orca-whirlpool'
import * as raydiumClmm from './contracts/raydium-clmm'
import * as raydiumAmm from './contracts/raydium-cpmm'
import {handleMeteoraDamm, handleMeteoraDlmm} from './handlers/meteora-swap-handler'
import {handleWhirlpool} from './handlers/orca-swap-handler'
import {handleRaydiumAmm} from './handlers/raydium-amm-swap-handler'
import {handleRaydiumClmm} from './handlers/raydium-clmm-swap-handler'

export type SwapType = 'orca_whirlpool' | 'meteora_damm' | 'meteora_dlmm' | 'raydium_clmm' | 'raydium_amm'

export interface TokenAmount {
    amount: bigint
    mint: string
    decimals: number
}

// TODO: the properties tokenA, tokenB, slippage and reserves are nullable
// because they were not implemented yet for Meteora. Once implemented the
// values should be required
export type SolanaSwap = {
    id: string
    type: SwapType
    account: string
    transaction: {hash: string; index: number}
    input: TokenAmount
    output: TokenAmount
    instruction: {address: number[]}
    block: BlockRef
    timestamp: Date
    poolAddress: string | null
    tokenA: string | null
    tokenB: string | null
    slippage: number | null
    reserves: {
        tokenA: TokenAmount
        tokenB: TokenAmount
    } | null
}

export type SolanaSwapTransfer = {
    type: SwapType
    account: string
    in: {amount: bigint; token: {postMint: string; postDecimals: number}}
    out: {amount: bigint; token: {postMint: string; postDecimals: number}}
    poolAddress: string | null
    tokenA: string | null
    tokenB: string | null
    slippage: number | null
    reserves: {
        tokenA: TokenAmount
        tokenB: TokenAmount
    } | null
}

function isPairAllowed(tokens: string[], tokenA: string, tokenB: string) {
    const isTokenAAllowed = tokens.includes(tokenA)
    const isTokenBAllowed = tokens.includes(tokenB)

    return isTokenAAllowed && isTokenBAllowed
}

type Selection = {
    block: {number: true; hash: true; timestamp: true}
    transaction: {transactionIndex: true; signatures: true; accountKeys: true; loadedAddresses: true}
    instruction: {transactionIndex: true; data: true; instructionAddress: true; programId: true; accounts: true}
}

function processDataMessage(blocks: (Block<Selection> & {swaps?: SolanaSwap[]})[], tokens: string[]) {
    for (const block of blocks) {
        block.swaps = []

        for (const ins of block.instructions) {
            let swap: SolanaSwapTransfer | null = null

            const tx = ins.getTransaction()
            const accountKeys = tx.accountKeys || []

            // FIXME: Defi Tuna instructions have multiple swaps and for some reason
            // we're not being able to decode innner instructions properly.
            if (accountKeys.includes('tuna4uSQZncNeeiAMKbstuxA9CUkHH6HmC64wgmnogD')) continue

            switch (ins.programId) {
                case whirlpool.programId:
                    if (whirlpool.instructions.swap.d8 === getInstructionDescriptor(ins)) {
                        swap = handleWhirlpool(ins, block)
                        break
                    }
                    break
                case meteoraDamm.programId:
                    switch (getInstructionDescriptor(ins)) {
                        case meteoraDamm.instructions.swap.d8:
                            swap = handleMeteoraDamm(ins, block)
                            break
                    }
                    break
                case meteoraDlmm.programId:
                    switch (getInstructionDescriptor(ins)) {
                        case meteoraDlmm.instructions.swap.d8:
                        case meteoraDlmm.instructions.swapExactOut.d8:
                            swap = handleMeteoraDlmm(ins, block)
                            break
                    }
                    break
                case raydiumAmm.programId:
                    switch (getInstructionDescriptor(ins)) {
                        case raydiumAmm.instructions.swapBaseInput.d8:
                        case raydiumAmm.instructions.swapBaseOutput.d8:
                            swap = handleRaydiumAmm(ins, block)
                            break
                    }
                    break
                case raydiumClmm.programId:
                    switch (getInstructionDescriptor(ins)) {
                        case raydiumClmm.instructions.swap.d8:
                        case raydiumClmm.instructions.swapV2.d8:
                            // TODO: should uncomment this line once swapRouterBaseIn instruction handler is implemented
                            // case raydiumClmm.instructions.swapRouterBaseIn.d8:
                            swap = handleRaydiumClmm(ins, block)
                            break
                    }
                    break
            }

            if (!swap || !isPairAllowed(tokens, swap.in.token.postMint, swap.out.token.postMint)) {
                continue
            }

            block.swaps.push({
                id: `${tx.id}/${ins.transactionIndex}`,
                type: swap.type,
                block: {
                    number: block.header.number,
                    hash: block.header.hash,
                },
                instruction: {
                    address: ins.instructionAddress,
                },
                input: {
                    amount: swap.in.amount,
                    mint: swap.in.token.postMint,
                    decimals: swap.in.token.postDecimals,
                },
                output: {
                    amount: swap.out.amount,
                    mint: swap.out.token.postMint,
                    decimals: swap.out.token.postDecimals,
                },
                account: tx.accountKeys[0],
                transaction: {
                    hash: tx.id,
                    index: ins.transactionIndex,
                },
                timestamp: new Date(block.header.timestamp * 1000),
                poolAddress: swap.poolAddress,
                tokenA: swap.tokenA,
                tokenB: swap.tokenB,
                slippage: swap.slippage,
                reserves: swap.reserves,
            })
        }
    }

    return blocks
}

export function createSolanaSwapsStream({
    portal,
    tokens = [],
    types = ['meteora_damm', 'meteora_dlmm', 'orca_whirlpool', 'raydium_amm', 'raydium_clmm'],
}: {
    portal: string | PortalClientOptions
    types?: SwapType[]
    tokens?: string[]
}) {
    const query = new SolanaQueryBuilder()
    for (const type of types) {
        switch (type) {
            case 'orca_whirlpool':
                query.addInstruction({
                    range: {from: 361491090},
                    request: {
                        programId: [whirlpool.programId], // where executed by Whirlpool program
                        d8: [whirlpool.instructions.swap.d8],
                        isCommitted: true,
                        innerInstructions: true,
                        transaction: true,
                        transactionTokenBalances: true,
                        logs: true,
                    },
                })
                continue
            case 'meteora_damm':
                query.addInstruction({
                    range: {from: 317617480},
                    request: {
                        programId: [meteoraDamm.programId],
                        d8: [meteoraDamm.instructions.swap.d8],
                        isCommitted: true,
                        innerInstructions: true,
                        transaction: true,
                        transactionTokenBalances: true,
                        logs: true,
                    },
                })
                continue
            case 'meteora_dlmm':
                query.addInstruction({
                    range: {from: 317617480},
                    request: {
                        programId: [meteoraDlmm.programId],
                        d8: [meteoraDlmm.instructions.swap.d8, meteoraDlmm.instructions.swapExactOut.d8],
                        isCommitted: true,
                        innerInstructions: true,
                        transaction: true,
                        transactionTokenBalances: true,
                        logs: true,
                    },
                })
                continue
            case 'raydium_clmm':
                query.addInstruction({
                    range: {from: 317617480},
                    request: {
                        programId: [raydiumClmm.programId],
                        d8: [
                            raydiumClmm.instructions.swap.d8,
                            raydiumClmm.instructions.swapV2.d8,
                            raydiumClmm.instructions.swapRouterBaseIn.d8,
                        ],
                        isCommitted: true,
                        innerInstructions: true,
                        transaction: true,
                        transactionTokenBalances: true,
                        logs: true,
                    },
                })
                break
            case 'raydium_amm':
                query.addInstruction({
                    range: {from: 317617480},
                    request: {
                        programId: [raydiumAmm.programId],
                        d1: [raydiumAmm.instructions.swapBaseInput.d8, raydiumAmm.instructions.swapBaseOutput.d8],
                        isCommitted: true,
                        innerInstructions: true,
                        transaction: true,
                        transactionTokenBalances: true,
                        logs: true,
                    },
                })
                break
            default:
                throw new Error(`unknown type ${type}`)
        }
    }

    return createSolanaPortalSource({
        portal: typeof portal === 'string' ? {url: portal} : portal,
        fields: {
            block: {
                number: true,
                hash: true,
                timestamp: true,
            },
            transaction: {
                transactionIndex: true,
                signatures: true,
                accountKeys: true,
                loadedAddresses: true,
            },
            instruction: {
                transactionIndex: true,
                data: true,
                instructionAddress: true,
                programId: true,
                accounts: true,
            },
            tokenBalance: {
                transactionIndex: true,
                account: true,
                preMint: true,
                postMint: true,
                preAmount: true,
                postAmount: true,
                preDecimals: true,
                postDecimals: true,
            },
            log: {
                transactionIndex: true,
                instructionAddress: true,
                message: true,
                logIndex: true,
            },
        },
        query: query.build(),
    }).map((blocks) => processDataMessage(blocks, tokens))
}
