import type * as Solana from '@sqd-sdk/core/portal/solana'
import type {Simplify} from '@sqd-sdk/core/internal/types/misc'
import type {Trues} from '@sqd-sdk/core/internal/selection'
import type {Hex} from '@sqd-sdk/core/internal/types/primitive'

export type AddressTableLookup = Solana.AddressTableLookup
export type BalanceFields = Solana.BalanceFields
export type BalanceFieldSelection = Solana.BalanceFieldSelection
export type BlockHeaderFields = Solana.BlockHeaderFields
export type BlockHeaderFieldSelection = Solana.BlockHeaderFieldSelection
export type Discriminator = Solana.Discriminator
export type FieldSelection = Solana.FieldSelection
export type InstructionFields = Solana.InstructionFields
export type InstructionFieldSelection = Solana.InstructionFieldSelection
export type LogMessageFields = Solana.LogMessageFields
export type LogMessageFieldSelection = Solana.LogMessageFieldSelection
export type PostTokenBalanceFields = Solana.PostTokenBalanceFields
export type PrePostTokenBalanceFields = Solana.PrePostTokenBalanceFields
export type PreTokenBalanceFields = Solana.PreTokenBalanceFields
export type RewardFields = Solana.RewardFields
export type RewardFieldSelection = Solana.RewardFieldSelection
export type TokenBalanceFields = Solana.TokenBalanceFields
export type TokenBalanceFieldSelection = Solana.TokenBalanceFieldSelection
export type TransactionFields = Solana.TransactionFields
export type TransactionFieldSelection = Solana.TransactionFieldSelection

export type BlockPartial<F extends FieldSelection = Trues<FieldSelection>> = Solana.Block<F>

export type Block<F extends FieldSelection = Trues<FieldSelection>> = {
    header: BlockHeader<F>
    transactions: Transaction<F>[]
    instructions: Instruction<F>[]
    logs: LogMessage<F>[]
    balances: Balance<F>[]
    tokenBalances: TokenBalance<F>[]
    rewards: Reward<F>[]
}

export type BlockHeader<F extends FieldSelection = Trues<FieldSelection>> = Solana.BlockHeader<NonNullable<F['block']>>

export type Transaction<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Solana.Transaction<NonNullable<F['transaction']>> & {
        block: Block<F>
        instructions: Instruction<F>[]
        logs: LogMessage<F>[]
        balances: Balance<F>[]
        tokenBalances: TokenBalance<F>[]
    }
>

export type Instruction<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Solana.Instruction<NonNullable<F['instruction']>> &
        (NonNullable<F['instruction']>['data'] extends true
            ? {d1: Hex; d2: Hex; d4: Hex; d8: Hex}
            : Record<never, never>) & {
            block: Block<F>
            transaction?: Transaction<F>
            parent?: Instruction<F>
            inner: Instruction<F>[]
            logs: LogMessage<F>[]
        }
>

export type LogMessage<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Solana.LogMessage<NonNullable<F['log']>> & {
        block: Block<F>
        transaction?: Transaction<F>
        instruction?: Instruction<F>
    }
>

export type Balance<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Solana.Balance<NonNullable<F['balance']>> & {
        block: Block<F>
        transaction?: Transaction<F>
    }
>

export type TokenBalance<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Solana.TokenBalance<NonNullable<F['tokenBalance']>> & {
        block: Block<F>
        transaction?: Transaction<F>
    }
>

export type Reward<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Solana.Reward<NonNullable<F['reward']>> & {
        block: Block<F>
    }
>

export const REQUIRED_FIELDS = {
    block: {
        number: true,
        hash: true,
        parentHash: true,
    },
    transaction: {
        transactionIndex: true,
    },
    log: {
        transactionIndex: true,
        instructionAddress: true,
        logIndex: true,
    },
    instruction: {
        transactionIndex: true,
        instructionAddress: true,
    },
    balance: {
        transactionIndex: true,
    },
    tokenBalance: {
        transactionIndex: true,
    },
} as const satisfies FieldSelection

export type RequiredFieldSelection = typeof REQUIRED_FIELDS
