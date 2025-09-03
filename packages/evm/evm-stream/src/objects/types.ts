import type { Select, Selector, Trues } from '@belopash/core/internal/selection'
import type { Simplify } from '@belopash/core/internal/types/misc'
import type { Hex } from '@belopash/core/internal/types/primitive'
import type * as EVM from '@belopash/core/portal/evm'

type Id = { id: string }

type AddPrefix<Prefix extends string, S> = S extends string ? `${Prefix}${Capitalize<S>}` : never

export type BlockHeaderFields = {
    number: number
    hash: Hex
    parentHash: Hex
    timestamp: number
    transactionsRoot: Hex
    receiptsRoot: Hex
    stateRoot: Hex
    logsBloom: Hex
    sha3Uncles: Hex
    extraData: Hex
    miner: Hex
    nonce: Hex
    mixHash: Hex
    size: number
    gasLimit: bigint
    gasUsed: bigint
    difficulty: bigint
    totalDifficulty?: bigint
    baseFeePerGas: bigint
    blobGasUsed: bigint
    excessBlobGas: bigint
    l1BlockNumber?: number
}

export type TransactionFields = {
    transactionIndex: number
    hash: Hex
    nonce: number
    from: Hex
    to?: Hex
    input: Hex
    value: bigint
    gas: bigint
    gasPrice: bigint
    maxFeePerGas?: bigint
    maxPriorityFeePerGas?: bigint
    v: bigint
    r: Hex
    s: Hex
    yParity?: number
    chainId?: number
    sighash?: Hex
    contractAddress?: Hex
    gasUsed: bigint
    cumulativeGasUsed: bigint
    effectiveGasPrice: bigint
    type: number
    status: number
    blobVersionedHashes?: Hex[]
    l1Fee?: bigint
    l1FeeScalar?: number
    l1GasPrice?: bigint
    l1GasUsed?: bigint
    l1BlobBaseFee?: bigint
    l1BlobBaseFeeScalar?: number
    l1BaseFeeScalar?: number
}

export type LogFields = {
    logIndex: number
    transactionIndex: number
    transactionHash: Hex
    address: Hex
    data: Hex
    topics: Hex[]
}

export type TraceType = 'create' | 'call' | 'suicide' | 'reward'

export type TraceBaseFields = {
    type: TraceType
    transactionIndex: number
    traceAddress: number[]
    subtraces: number
    error: string | null
    revertReason?: string
}

export type TraceCreateActionFields = {
    from: Hex
    value: bigint
    gas: bigint
    init: Hex
}

export type TraceCreateResultFields = {
    gasUsed: bigint
    code?: Hex
    address: Hex
}

export type TraceCallActionFields = {
    callType: string
    from: Hex
    to: Hex
    value?: bigint
    gas: bigint
    input: Hex
    sighash?: Hex
}

export type TraceCallResultFields = {
    gasUsed: bigint
    output?: Hex
}

export type TraceSuicideActionFields = {
    address: Hex
    refundAddress: Hex
    balance: bigint
}

export type TraceRewardActionFields = {
    author: Hex
    value: bigint
    type: string
}

// Combined trace fields using prefixed naming like the portal
export type TraceFields = TraceBaseFields & {
    // Create action fields (prefixed with 'create')
    createFrom?: Hex
    createValue?: bigint
    createGas?: bigint
    createInit?: Hex
    // Create result fields (prefixed with 'createResult')
    createResultGasUsed?: bigint
    createResultCode?: Hex
    createResultAddress?: Hex
    // Call action fields (prefixed with 'call')
    callCallType?: string
    callFrom?: Hex
    callTo?: Hex
    callValue?: bigint
    callGas?: bigint
    callInput?: Hex
    callSighash?: Hex
    // Call result fields (prefixed with 'callResult')
    callResultGasUsed?: bigint
    callResultOutput?: Hex
    // Suicide action fields (prefixed with 'suicide')
    suicideAddress?: Hex
    suicideRefundAddress?: Hex
    suicideBalance?: bigint
    // Reward action fields (prefixed with 'reward')
    rewardAuthor?: Hex
    rewardValue?: bigint
    rewardType?: string
}

export type StateDiffFields = {
    transactionIndex: number
    address: Hex
    key: 'balance' | 'code' | 'nonce' | Hex
    kind: '+' | '-' | '*' | '='
    prev?: Hex
    next?: Hex
}

export const REQUIRED_FIELDS = {
    block: {
        number: true,
        hash: true,
        // parentHash: true,
        timestamp: true,
    },
    transaction: {
        transactionIndex: true,
    },
    log: {
        transactionIndex: true,
        logIndex: true,
    },
    trace: {
        type: true,
        traceAddress: true,
        transactionIndex: true,
    },
    stateDiff: {
        transactionIndex: true,
        address: true,
        key: true,
    },
} as const

export type RequiredFieldSelection = typeof REQUIRED_FIELDS

export type FieldSelection = {
    block?: Selector<keyof BlockHeaderFields>
    transaction?: Selector<keyof TransactionFields>
    log?: Selector<keyof LogFields>
    trace?: Selector<
        | keyof TraceBaseFields
        | AddPrefix<'create', keyof TraceCreateActionFields>
        | AddPrefix<'createResult', keyof TraceCreateResultFields>
        | AddPrefix<'call', keyof TraceCallActionFields>
        | AddPrefix<'callResult', keyof TraceCallResultFields>
        | AddPrefix<'suicide', keyof TraceSuicideActionFields>
        | AddPrefix<'reward', keyof TraceRewardActionFields>
    >
    stateDiff?: Selector<keyof StateDiffFields>
}

export type Block<F extends FieldSelection = Trues<FieldSelection>> = {
    header: BlockHeader<F>
    transactions: Transaction<F>[]
    logs: Log<F>[]
    traces: Trace<F>[]
    stateDiffs: StateDiff<F>[]
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
            readonly logs: Log<F>[]
            readonly traces: Trace<F>[]
            readonly stateDiffs: StateDiff<F>[]
        }
>

export type Log<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Id &
        Select<LogFields, NonNullable<F['log']> & RequiredFieldSelection['log']> & {
            readonly block: Block<F>
            readonly transaction?: Transaction<F>
        }
>

type RemoveKeysPrefix<Prefix extends string, T> = {
    [K in keyof T as K extends `${Prefix}${infer S}` ? Uncapitalize<S> : never]: T[K]
}

export type TraceCreate<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Id &
        Select<TraceBaseFields & { type: 'create' }, NonNullable<F['trace']> & RequiredFieldSelection['trace']> & {
            action: Select<TraceCreateActionFields, RemoveKeysPrefix<'create', NonNullable<F['trace']>>>
            result?: Select<TraceCreateResultFields, RemoveKeysPrefix<'createResult', NonNullable<F['trace']>>>
            readonly block: Block<F>
            readonly transaction?: Transaction<F>
            readonly parent?: Trace<F>
            readonly children: Trace<F>[]
        }
>

export type TraceCall<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Id &
        Select<TraceBaseFields & { type: 'call' }, NonNullable<F['trace']> & RequiredFieldSelection['trace']> & {
            action: Select<TraceCallActionFields, RemoveKeysPrefix<'call', NonNullable<F['trace']>>>
            result?: Select<TraceCallResultFields, RemoveKeysPrefix<'callResult', NonNullable<F['trace']>>>
            readonly block: Block<F>
            readonly transaction?: Transaction<F>
            readonly parent?: Trace<F>
            readonly children: Trace<F>[]
        }
>

export type TraceSuicide<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Id &
        Select<TraceBaseFields & { type: 'suicide' }, NonNullable<F['trace']> & RequiredFieldSelection['trace']> & {
            action: Select<TraceSuicideActionFields, RemoveKeysPrefix<'suicide', NonNullable<F['trace']>>>
            readonly block: Block<F>
            readonly transaction?: Transaction<F>
            readonly parent?: Trace<F>
            readonly children: Trace<F>[]
        }
>

export type TraceReward<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Id &
        Select<TraceBaseFields & { type: 'reward' }, NonNullable<F['trace']> & RequiredFieldSelection['trace']> & {
            action: Select<TraceRewardActionFields, RemoveKeysPrefix<'reward', NonNullable<F['trace']>>>
            readonly block: Block<F>
            readonly transaction?: Transaction<F>
            readonly parent?: Trace<F>
            readonly children: Trace<F>[]
        }
>

export type Trace<F extends FieldSelection = Trues<FieldSelection>> = F extends any
    ? TraceCreate<F> | TraceCall<F> | TraceSuicide<F> | TraceReward<F>
    : never

export type StateDiff<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Id &
        Select<StateDiffFields, NonNullable<F['stateDiff']> & RequiredFieldSelection['stateDiff']> & {
            readonly block: Block<F>
            readonly transaction?: Transaction<F>
        }
>

export type BlockPartial<F extends FieldSelection = Trues<FieldSelection>> = EVM.Block<any>

export function blockFromPartial<F extends FieldSelection>(partial: BlockPartial<F>): Block<F> {
    // Simple pass-through for now, relations will be set up later
    return partial as any as Block<F>
}
