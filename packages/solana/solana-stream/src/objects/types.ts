import type * as Solana from '@sqd-sdk/core/portal/solana'
import type {Simplify} from '@sqd-sdk/core/internal/types/misc'
import type {Trues} from '@sqd-sdk/core/internal/selection'
import type {Hex} from '@sqd-sdk/core/internal/types/primitive'

type Id = {id: string}

export type FieldSelection = Solana.FieldSelection

export type AddressTableLookup = Solana.AddressTableLookup
export type Discriminator = Solana.Discriminator

export type Block<F extends FieldSelection = Trues<FieldSelection>> = {
    header: BlockHeader<F>
    transactions: Transaction<F>[]
    instructions: Instruction<F>[]
    logs: LogMessage<F>[]
    balances: Balance<F>[]
    tokenBalances: TokenBalance<F>[]
    rewards: Reward<F>[]
}

export type BlockHeader<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Id & Solana.BlockHeader<NonNullable<F['block']>>
>

export type Transaction<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Id &
        Solana.Transaction<NonNullable<F['transaction']>> & {
            block: Block<F>
            instructions: Instruction<F>[]
            logs: LogMessage<F>[]
            balances: Balance<F>[]
            tokenBalances: TokenBalance<F>[]
        }
>

export type Instruction<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Id &
        Solana.Instruction<NonNullable<F['instruction']>> &
        (NonNullable<F['instruction']>['data'] extends true
            ? {d1: Hex; d2: Hex; d4: Hex; d8: Hex}
            : Record<never, never>) & {
            block: Block<F>
            transaction?: Transaction<F>
            getTransaction: () => Transaction<F>
            parent?: Instruction<F>
            getParent: () => Instruction<F>
            inner: Instruction<F>[]
            logs: LogMessage<F>[]
        }
>

export type LogMessage<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Id &
        Solana.LogMessage<NonNullable<F['log']>> & {
            block: Block<F>
            transaction?: Transaction<F>
            getTransaction: () => Transaction<F>
            instruction?: Instruction<F>
            getInstruction: () => Instruction<F>
        }
>

export type Balance<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Id &
        Solana.Balance<NonNullable<F['balance']>> & {
            block: Block<F>
            transaction?: Transaction<F>
            getTransaction: () => Transaction<F>
        }
>

export type TokenBalance<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Id &
        Solana.TokenBalance<NonNullable<F['tokenBalance']>> & {
            block: Block<F>
            transaction?: Transaction<F>
            getTransaction: () => Transaction<F>
        }
>

export type Reward<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Id &
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
        account: true,
        transactionIndex: true,
    },
    tokenBalance: {
        account: true,
        transactionIndex: true,
    },
    reward: {
        pubkey: true,
    },
} as const satisfies FieldSelection

export type RequiredFieldSelection = typeof REQUIRED_FIELDS
