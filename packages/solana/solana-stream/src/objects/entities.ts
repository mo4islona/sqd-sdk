import type {Base58, Hex} from '@belopash/core/internal/types/primitive'
import {getInstructionDescriptor} from '../instruction'
import type * as solana from '@belopash/core/portal/solana'
import type * as base from './types'

export class BlockHeader<F extends base.FieldSelection> {
    id: string

    hash!: string
    number!: number
    height!: number
    parentNumber!: number
    parentHash!: string
    timestamp!: number

    #block!: base.Block<F>

    constructor(raw: solana.BlockHeader<{hash: true; number: true}>, block: base.Block<F>) {
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
    version!: number | 'legacy'
    accountKeys!: Base58[]
    addressTableLookups!: base.AddressTableLookup[]
    numReadonlySignedAccounts!: number
    numReadonlyUnsignedAccounts!: number
    numReadOnlyAccounts!: number
    computeUnitsConsumed!: bigint
    err!: object | null
    signatures!: Base58[]
    recentBlockhash!: Base58
    numRequiredSignatures!: number
    fee!: bigint
    hasDroppedLogMessages!: boolean
    loadedAddresses?: {
        readonly: Base58[]
        writable: Base58[]
    }

    constructor(raw: solana.Transaction<{transactionIndex: true}>, block: base.Block<F>) {
        Object.assign(this, raw)
        this.id = formatId(block.header, raw.transactionIndex)
        this.#block = block
    }

    #block: base.Block<F>

    get block(): base.Block<F> {
        return this.#block
    }

    #instructions?: base.Instruction<F>[]

    get instructions(): base.Instruction<F>[] {
        if (this.#instructions == null) {
            this.#instructions = []
        }
        return this.#instructions
    }

    #balances?: base.Balance<F>[]

    get balances(): base.Balance<F>[] {
        if (this.#balances == null) {
            this.#balances = []
        }
        return this.#balances
    }

    #tokenBalances?: base.TokenBalance<F>[]

    get tokenBalances(): base.TokenBalance<F>[] {
        if (this.#tokenBalances == null) {
            this.#tokenBalances = []
        }
        return this.#tokenBalances
    }

    #logs?: base.LogMessage<F>[]

    get logs(): base.LogMessage<F>[] {
        if (this.#logs == null) {
            this.#logs = []
        }
        return this.#logs
    }
}

export class Instruction<F extends base.FieldSelection> {
    id: string
    accounts!: Base58[]
    data!: Hex
    programId!: Base58
    transactionIndex!: number
    instructionAddress!: number[]
    computeUnitsConsumed!: bigint
    error?: unknown
    isCommitted!: boolean
    hasDroppedLogMessages!: boolean

    constructor(
        raw: solana.Instruction<{transactionIndex: true; instructionAddress: true}>,
        block: base.Block<F>,
        transaction?: base.Transaction<F>,
        parent?: base.Instruction<F>,
    ) {
        Object.assign(this, raw)
        this.id = formatId(block.header, raw.transactionIndex, ...raw.instructionAddress)
        this.#block = block
        this.#transaction = transaction
        this.#parent = parent
    }

    #block: base.Block<F>
    #transaction?: base.Transaction<F>
    #parent?: base.Instruction<F>

    #inner?: base.Instruction<F>[]
    #logs?: base.LogMessage<F>[]

    #d1?: string
    #d2?: string
    #d4?: string
    #d8?: string

    get block(): base.Block<F> {
        return this.#block
    }

    get transaction(): base.Transaction<F> | undefined {
        return this.#transaction
    }

    getTransaction(): base.Transaction<F> {
        if (this.#transaction == null) {
            throw new Error('Transaction is not set on instruction')
        }
        return this.#transaction
    }

    get inner(): base.Instruction<F>[] {
        if (this.#inner == null) {
            this.#inner = []
        }
        return this.#inner
    }

    set inner(instructions: base.Instruction<F>[]) {
        this.#inner = instructions
    }

    get parent(): base.Instruction<F> | undefined {
        return this.#parent
    }

    getParent(): base.Instruction<F> {
        if (this.#parent == null) {
            throw new Error('Parent instruction is not set')
        }
        return this.#parent
    }

    get logs(): base.LogMessage<F>[] {
        if (this.#logs == null) {
            this.#logs = []
        }
        return this.#logs
    }

    get d1(): Hex {
        this.#d1 ??= this.d8.slice(0, 4)
        return this.#d1
    }

    get d2(): Hex {
        this.#d2 ??= this.d8.slice(0, 6)
        return this.#d2
    }

    get d4(): Hex {
        this.#d4 ??= this.d8.slice(0, 10)
        return this.#d4
    }

    get d8(): Hex {
        if (this.#d8) {
            return this.#d8
        }
        if (this.data == null) {
            throw new Error('.data field is not available')
        }
        this.#d8 = getInstructionDescriptor(this)
        return this.#d8
    }
}

export class LogMessage<F extends base.FieldSelection> {
    id: string
    transactionIndex!: number
    logIndex!: number
    instructionAddress!: number[]
    programId!: Base58
    kind!: 'log' | 'data' | 'other'
    message!: string

    constructor(
        raw: solana.LogMessage<{transactionIndex: true; logIndex: true}>,
        block: base.Block<F>,
        transaction?: base.Transaction<F>,
        instruction?: base.Instruction<F>,
    ) {
        Object.assign(this, raw)
        this.id = formatId(block.header, raw.transactionIndex, raw.logIndex)
        this.#block = block
        this.#transaction = transaction
        this.#instruction = instruction
    }

    #block: base.Block<F>
    #transaction?: base.Transaction<F>
    #instruction?: base.Instruction<F>

    get block(): base.Block<F> {
        return this.#block
    }

    get transaction(): base.Transaction<F> | undefined {
        return this.#transaction
    }

    getTransaction(): base.Transaction<F> {
        if (this.#transaction == null) {
            throw new Error('Transaction is not set on log message')
        }
        return this.#transaction
    }

    get instruction(): base.Instruction<F> | undefined {
        return this.#instruction
    }

    getInstruction(): base.Instruction<F> {
        if (this.#instruction == null) {
            throw new Error('Instruction is not set on log message')
        }
        return this.#instruction
    }
}

export class Balance<F extends base.FieldSelection> {
    id: string
    transactionIndex!: number
    account!: Base58
    pre!: bigint
    post!: bigint

    constructor(
        raw: solana.Balance<{transactionIndex: true; account: true}>,
        block: base.Block<F>,
        transaction?: base.Transaction<F>,
    ) {
        Object.assign(this, raw)
        this.id = `${formatId(block.header, raw.transactionIndex)}-${raw.account}`
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
            throw new Error('Transaction is not set on balance change record')
        }
        return this.#transaction
    }
}

export class TokenBalance<F extends base.FieldSelection> {
    id!: string
    transactionIndex!: number
    account!: Base58
    preAmount!: bigint
    preDecimals!: number
    preMint!: Base58
    preOwner!: Base58
    preProgramId!: Base58
    postAmount!: bigint
    postDecimals!: number
    postMint!: Base58
    postOwner!: Base58
    postProgramId!: Base58

    constructor(
        raw: solana.TokenBalance<{transactionIndex: true; account: true}>,
        block: base.Block<F>,
        transaction?: base.Transaction<F>,
    ) {
        Object.assign(this, raw)
        this.id = `${formatId(block.header, raw.transactionIndex)}-${raw.account}`
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
            throw new Error('Transaction is not set on pre post token balance record')
        }
        return this.#transaction
    }
}

export class Reward<F extends base.FieldSelection> {
    id!: string
    pubkey!: Base58
    commission?: number
    lamports!: bigint
    postBalance!: bigint
    rewardType?: string

    constructor(raw: solana.Reward<{pubkey: true}>, block: base.Block<F>) {
        Object.assign(this, raw)
        this.id = `${formatId(block.header)}-${raw.pubkey}`
        this.#block = block
    }

    #block!: base.Block<F>

    get block(): base.Block<F> {
        return this.#block
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
