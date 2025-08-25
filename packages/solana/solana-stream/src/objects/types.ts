import type {Simplify} from '@sqd-sdk/core/internal/types/misc'
import type {Trues, Select, Selector} from '@sqd-sdk/core/internal/selection'
import type {Hex, Base58} from '@sqd-sdk/core/internal/types/primitive'
import type {BlockRef} from '@sqd-sdk/core/portal'
import type {Data} from '@sqd-sdk/core/pipeline'

type Id = {id: string}

export type BlockHeaderFields = {
    hash: Base58
    number: number
    height: number
    parentNumber: number
    parentHash: Base58
    timestamp: number
}

export type AddressTableLookup = {
    accountKey: Base58
    readonlyIndexes: number[]
    writableIndexes: number[]
}

export type TransactionFields = {
    /** Transaction position in block */
    transactionIndex: number
    version: 'legacy' | number
    // transaction message
    accountKeys: Base58[]
    addressTableLookups: AddressTableLookup[]
    numReadonlySignedAccounts: number
    numReadonlyUnsignedAccounts: number
    numRequiredSignatures: number
    recentBlockhash: Base58
    signatures: Base58[]
    // meta fields
    err: null | object
    computeUnitsConsumed: bigint
    fee: bigint
    loadedAddresses?: {
        readonly: Base58[]
        writable: Base58[]
    }
    hasDroppedLogMessages: boolean
}

export type InstructionFields = {
    transactionIndex: number
    instructionAddress: number[]
    programId: Base58
    accounts: Base58[]
    data: Base58
    // execution result extracted from logs
    computeUnitsConsumed?: bigint
    error?: unknown
    /** `true` when transaction completed successfully, `false` otherwise */
    isCommitted: boolean
    hasDroppedLogMessages: boolean
}

export type LogMessageFields = {
    transactionIndex: number
    logIndex: number
    instructionAddress: number[]
    programId: Base58
    kind: 'log' | 'data' | 'other'
    message: string
}

export type BalanceFields = {
    transactionIndex: number
    account: Base58
    pre: bigint
    post: bigint
}

export type PreTokenBalanceFields = {
    transactionIndex: number
    account: Base58

    preProgramId?: Base58
    preMint: Base58
    preDecimals: number
    preOwner?: Base58
    preAmount: bigint

    postProgramId?: undefined
    postMint?: undefined
    postDecimals?: undefined
    postOwner?: undefined
    postAmount?: undefined
}

export type PostTokenBalanceFields = {
    transactionIndex: number
    account: Base58

    preProgramId?: undefined
    preMint?: undefined
    preDecimals?: undefined
    preOwner?: undefined
    preAmount?: undefined

    postProgramId?: Base58
    postMint: Base58
    postDecimals: number
    postOwner?: Base58
    postAmount: bigint
}

export type PrePostTokenBalanceFields = {
    transactionIndex: number
    account: Base58
    preProgramId?: Base58
    preMint: Base58
    preDecimals: number
    preOwner?: Base58
    preAmount: bigint
    postProgramId?: Base58
    postMint: Base58
    postDecimals: number
    postOwner?: Base58
    postAmount: bigint
}

export type TokenBalanceFields = PreTokenBalanceFields | PostTokenBalanceFields | PrePostTokenBalanceFields

export type RewardFields = {
    pubkey: Base58
    lamports: bigint
    postBalance: bigint
    rewardType?: string
    commission?: number
}

export type RequiredFieldSelection = {
    block: {
        number: true
        hash: true
    }
    transaction: {
        transactionIndex: true
    }
    log: {
        transactionIndex: true
        instructionAddress: true
        logIndex: true
    }
    instruction: {
        transactionIndex: true
        instructionAddress: true
    }
    balance: {
        account: true
        transactionIndex: true
    }
    tokenBalance: {
        account: true
        transactionIndex: true
    }
    reward: {
        pubkey: true
    }
}

export type FieldSelection = {
    block?: Selector<Exclude<keyof BlockHeaderFields, keyof RequiredFieldSelection['block']>>
    transaction?: Selector<Exclude<keyof TransactionFields, keyof RequiredFieldSelection['transaction']>>
    instruction?: Selector<Exclude<keyof InstructionFields, keyof RequiredFieldSelection['instruction']>>
    log?: Selector<Exclude<keyof LogMessageFields, keyof RequiredFieldSelection['log']>>
    balance?: Selector<Exclude<keyof BalanceFields, keyof RequiredFieldSelection['balance']>>
    tokenBalance?: Selector<Exclude<keyof TokenBalanceFields, keyof RequiredFieldSelection['tokenBalance']>>
    reward?: Selector<Exclude<keyof RewardFields, keyof RequiredFieldSelection['reward']>>
}

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
    Id &
        Select<BlockHeaderFields, NonNullable<F['block']> & RequiredFieldSelection['block']> & {
            readonly block: Block<F>
        }
>

export type Transaction<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Id &
        Select<TransactionFields, NonNullable<F['transaction']> & RequiredFieldSelection['transaction']> & {
            readonly block: Block<F>
            readonly instructions: Instruction<F>[]
            readonly logs: LogMessage<F>[]
            readonly balances: Balance<F>[]
            readonly tokenBalances: TokenBalance<F>[]
        }
>

export type Instruction<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Id &
        Select<InstructionFields, NonNullable<F['instruction']> & RequiredFieldSelection['instruction']> &
        (NonNullable<F['instruction']>['data'] extends true
            ? {d1: Hex; d2: Hex; d4: Hex; d8: Hex}
            : Record<never, never>) & {
            readonly block: Block<F>
            readonly transaction?: Transaction<F>
            readonly getTransaction: () => Transaction<F>
            readonly parent?: Instruction<F>
            readonly getParent: () => Instruction<F>
            readonly inner: Instruction<F>[]
            readonly logs: LogMessage<F>[]
        }
>

export type LogMessage<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Id &
        Select<LogMessageFields, NonNullable<F['log']> & RequiredFieldSelection['log']> & {
            readonly block: Block<F>
            readonly transaction?: Transaction<F>
            readonly getTransaction: () => Transaction<F>
            readonly instruction?: Instruction<F>
            readonly getInstruction: () => Instruction<F>
        }
>

export type Balance<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Id &
        Select<BalanceFields, NonNullable<F['balance']> & RequiredFieldSelection['balance']> & {
            readonly block: Block<F>
            readonly transaction?: Transaction<F>
            readonly getTransaction: () => Transaction<F>
        }
>

export type PreTokenBalance<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Id &
        Select<PreTokenBalanceFields, NonNullable<F['tokenBalance']> & RequiredFieldSelection['tokenBalance']> & {
            readonly block: Block<F>
            readonly transaction?: Transaction<F>
            readonly getTransaction: () => Transaction<F>
        }
>

export type PostTokenBalance<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Id &
        Select<PostTokenBalanceFields, NonNullable<F['tokenBalance']> & RequiredFieldSelection['tokenBalance']> & {
            readonly block: Block<F>
            readonly transaction?: Transaction<F>
            readonly getTransaction: () => Transaction<F>
        }
>

export type PrePostTokenBalance<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Id &
        Select<PrePostTokenBalanceFields, NonNullable<F['tokenBalance']> & RequiredFieldSelection['tokenBalance']> & {
            readonly block: Block<F>
            readonly transaction?: Transaction<F>
            readonly getTransaction: () => Transaction<F>
        }
>

export type TokenBalance<F extends FieldSelection = Trues<FieldSelection>> =
    | PreTokenBalance<F>
    | PostTokenBalance<F>
    | PrePostTokenBalance<F>

export type Reward<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Id &
        Select<RewardFields, NonNullable<F['reward']> & RequiredFieldSelection['reward']> & {
            readonly block: Block<F>
        }
>
