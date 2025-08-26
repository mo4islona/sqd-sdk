import type {Hex} from '@sqd-sdk/core/internal/types/primitive'
import type * as EVM from '@sqd-sdk/core/portal/evm'
import type * as base from './types'

export class Block<F extends base.FieldSelection> {
    header!: base.BlockHeader<F>
    transactions: base.Transaction<F>[] = []
    logs: base.Log<F>[] = []
    traces: base.Trace<F>[] = []
    stateDiffs: base.StateDiff<F>[] = []

    constructor(header: base.BlockHeader<F>) {
        this.header = header
    }
}

export class BlockHeader<F extends base.FieldSelection> {
    id: string

    number!: number
    hash!: Hex
    parentHash!: Hex
    timestamp!: number
    transactionsRoot!: Hex
    receiptsRoot!: Hex
    stateRoot!: Hex
    logsBloom!: Hex
    sha3Uncles!: Hex
    extraData!: Hex
    miner!: Hex
    nonce!: Hex
    mixHash!: Hex
    size!: number
    gasLimit!: bigint
    gasUsed!: bigint
    difficulty!: bigint
    totalDifficulty?: bigint
    baseFeePerGas!: bigint
    blobGasUsed!: bigint
    excessBlobGas!: bigint
    l1BlockNumber?: number

    #block!: base.Block<F>

    constructor(raw: EVM.BlockHeader<{number: true; hash: true}>, block: base.Block<F>) {
        Object.assign(this, raw)
        this.id = formatId(raw)
        this.#block = block
    }

    get block(): base.Block<F> {
        return this.#block
    }
}

export class Transaction<F extends base.FieldSelection> {
    id: string

    transactionIndex!: number
    hash!: Hex
    nonce!: number
    from!: Hex
    to?: Hex
    input!: Hex
    value!: bigint
    gas!: bigint
    gasPrice!: bigint
    maxFeePerGas?: bigint
    maxPriorityFeePerGas?: bigint
    v!: bigint
    r!: Hex
    s!: Hex
    yParity?: number
    chainId?: number
    sighash?: Hex
    contractAddress?: Hex
    gasUsed!: bigint
    cumulativeGasUsed!: bigint
    effectiveGasPrice!: bigint
    type!: number
    status!: number
    blobVersionedHashes?: Hex[]
    l1Fee?: bigint
    l1FeeScalar?: number
    l1GasPrice?: bigint
    l1GasUsed?: bigint
    l1BlobBaseFee?: bigint
    l1BlobBaseFeeScalar?: number
    l1BaseFeeScalar?: number

    constructor(raw: EVM.Transaction<{transactionIndex: true}>, block: base.Block<F>) {
        Object.assign(this, raw)
        this.id = formatId(block.header, raw.transactionIndex)
        this.#block = block
    }

    #block: base.Block<F>

    get block(): base.Block<F> {
        return this.#block
    }

    #logs?: base.Log<F>[]

    get logs(): base.Log<F>[] {
        if (this.#logs == null) {
            this.#logs = []
        }
        return this.#logs
    }

    #traces?: base.Trace<F>[]

    get traces(): base.Trace<F>[] {
        if (this.#traces == null) {
            this.#traces = []
        }
        return this.#traces
    }

    #stateDiffs?: base.StateDiff<F>[]

    get stateDiffs(): base.StateDiff<F>[] {
        if (this.#stateDiffs == null) {
            this.#stateDiffs = []
        }
        return this.#stateDiffs
    }
}

export class Log<F extends base.FieldSelection> {
    id: string

    logIndex!: number
    transactionIndex!: number
    transactionHash!: Hex
    address!: Hex
    data!: Hex
    topics!: Hex[]

    constructor(
        raw: EVM.Log<{transactionIndex: true; logIndex: true}>,
        block: base.Block<F>,
        transaction?: base.Transaction<F>,
    ) {
        Object.assign(this, raw)
        this.id = formatId(block.header, raw.logIndex)
        this.#block = block
        this.#transaction = transaction
    }

    #block: base.Block<F>
    #transaction?: base.Transaction<F>

    get block(): base.Block<F> {
        return this.#block
    }

    get transaction(): base.Transaction<F> | undefined {
        return this.#transaction
    }

    getTransaction(): base.Transaction<F> {
        if (this.#transaction == null) {
            throw new Error('Transaction is not set on log')
        }
        return this.#transaction
    }
}

class TraceBase<F extends base.FieldSelection> {
    id: string

    type!: base.TraceType
    transactionIndex!: number
    traceAddress!: number[]
    subtraces!: number
    error!: string | null
    revertReason?: string

    constructor(
        raw: EVM.Trace<{type: true; transactionIndex: true; traceAddress: true}>,
        block: base.Block<F>,
        transaction?: base.Transaction<F>,
        parent?: base.Trace<F>,
    ) {
        Object.assign(this, raw)
        this.id = formatId(block.header, raw.transactionIndex, ...raw.traceAddress)
        this.#block = block
        this.#transaction = transaction
        this.#parent = parent
    }

    #block: base.Block<F>
    #transaction?: base.Transaction<F>
    #parent?: base.Trace<F>
    #children?: base.Trace<F>[]

    get block(): base.Block<F> {
        return this.#block
    }

    get transaction(): base.Transaction<F> | undefined {
        return this.#transaction
    }

    getTransaction(): base.Transaction<F> {
        if (this.#transaction == null) {
            throw new Error('Transaction is not set on trace')
        }
        return this.#transaction
    }

    get parent(): base.Trace<F> | undefined {
        return this.#parent
    }

    getParent(): base.Trace<F> {
        if (this.#parent == null) {
            throw new Error('Parent trace is not set')
        }
        return this.#parent
    }

    get children(): base.Trace<F>[] {
        if (this.#children == null) {
            this.#children = []
        }
        return this.#children
    }

    set children(value: base.Trace<F>[]) {
        this.#children = value
    }
}

export class TraceCreate<F extends base.FieldSelection> extends TraceBase<F> {
    type = 'create' as const
    action!: base.TraceCreateActionFields
    result?: base.TraceCreateResultFields
}

export class TraceCall<F extends base.FieldSelection> extends TraceBase<F> {
    type = 'call' as const
    action!: base.TraceCallActionFields
    result?: base.TraceCallResultFields
}

export class TraceSuicide<F extends base.FieldSelection> extends TraceBase<F> {
    type = 'suicide' as const
    action!: base.TraceSuicideActionFields
}

export class TraceReward<F extends base.FieldSelection> extends TraceBase<F> {
    type = 'reward' as const
    action!: base.TraceRewardActionFields
}

export type Trace<F extends base.FieldSelection> = TraceCreate<F> | TraceCall<F> | TraceSuicide<F> | TraceReward<F>

export class StateDiff<F extends base.FieldSelection> {
    id: string

    transactionIndex!: number
    address!: Hex
    key!: 'balance' | 'code' | 'nonce' | Hex
    kind!: '+' | '-' | '*' | '='
    prev?: Hex | null
    next?: Hex | null

    constructor(
        raw: EVM.StateDiff<{transactionIndex: true; address: true; key: true}>,
        block: base.Block<F>,
        transaction?: base.Transaction<F>,
    ) {
        Object.assign(this, raw)
        this.id = `${formatId(block.header, raw.transactionIndex)}-${raw.address}-${raw.key}`
        this.#block = block
        this.#transaction = transaction
    }

    #block: base.Block<F>
    #transaction?: base.Transaction<F>

    get block(): base.Block<F> {
        return this.#block
    }

    get transaction(): base.Transaction<F> | undefined {
        return this.#transaction
    }

    getTransaction(): base.Transaction<F> {
        if (this.#transaction == null) {
            throw new Error('Transaction is not set on state diff')
        }
        return this.#transaction
    }
}

function formatId(block: {number: number; hash: string}, ...address: number[]): string {
    let no = block.number.toString().padStart(12, '0')
    let hash = block.hash.slice(0, 5)
    let id = `${no}-${hash}`
    for (let index of address) {
        id += `-${index.toString().padStart(6, '0')}`
    }
    return id
}
