import type {Base58, Hex} from '@sqd-sdk/core/internal/types/primitive'
import {getInstructionDescriptor} from '../instruction'
import type * as solana from '@sqd-sdk/core/portal/solana'
import type * as base from './types'

export function blockFromPartial<F extends base.FieldSelection>(src: solana.Block<F>): base.Block<F> {
    const block = {} as base.Block<F>

    block.header = Object.assign(new BlockHeader(), src.header)

    const transactions = new Array(src.transactions.length)
    for (let i = 0; i < src.transactions.length; i++) {
        transactions[i] = Object.assign(new Transaction(), src.transactions[i])
    }
    block.transactions = transactions

    const instructions = new Array(src.instructions.length)
    for (let i = 0; i < src.instructions.length; i++) {
        instructions[i] = Object.assign(new Instruction(), src.instructions[i])
    }
    block.instructions = instructions

    const logs = new Array(src.logs.length)
    for (let i = 0; i < src.logs.length; i++) {
        logs[i] = Object.assign(new LogMessage(), src.logs[i])
    }
    block.logs = logs

    const balances = new Array(src.balances.length)
    for (let i = 0; i < src.balances.length; i++) {
        balances[i] = Object.assign(new Balance(), src.balances[i])
    }
    block.balances = balances

    const tokenBalances = new Array(src.tokenBalances.length)
    for (let i = 0; i < src.tokenBalances.length; i++) {
        tokenBalances[i] = Object.assign(new TokenBalance(), src.tokenBalances[i])
    }
    block.tokenBalances = tokenBalances

    const rewards = new Array(src.rewards.length)
    for (let i = 0; i < src.rewards.length; i++) {
        rewards[i] = Object.assign(new Reward(), src.rewards[i])
    }
    block.rewards = rewards

    return block
}

export class BlockHeader<F extends base.FieldSelection> implements base.BlockHeader {
    #id?: string

    get id(): string {
        if (this.#id == null) {
            this.#id = formatId(this, this.height)
        }
        return this.#id
    }

    set id(value: string) {
        this.#id = value
    }

    hash!: string
    number!: number
    height!: number
    parentNumber!: number
    parentHash!: string
    timestamp!: number
}

export class Transaction implements base.Transaction {
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

    #id?: string

    get id(): string {
        if (this.#id == null) {
            this.#id = formatId(this.block.header, this.transactionIndex)
        }
        return this.#id
    }

    set id(value: string) {
        this.#id = value
    }

    #block!: base.Block

    get block(): base.Block {
        return this.#block
    }

    set block(value: base.Block) {
        this.#block = value
    }

    #instructions?: base.Instruction[]

    get instructions(): base.Instruction[] {
        if (this.#instructions == null) {
            this.#instructions = []
        }
        return this.#instructions
    }

    set instructions(value: base.Instruction[]) {
        this.#instructions = value
    }

    #balances?: base.Balance[]

    get balances(): base.Balance[] {
        if (this.#balances == null) {
            this.#balances = []
        }
        return this.#balances
    }

    set balances(value: base.Balance[]) {
        this.#balances = value
    }

    #tokenBalances?: base.TokenBalance[]

    get tokenBalances(): base.TokenBalance[] {
        if (this.#tokenBalances == null) {
            this.#tokenBalances = []
        }
        return this.#tokenBalances
    }

    set tokenBalances(value: base.TokenBalance[]) {
        this.#tokenBalances = value
    }

    #logs?: base.LogMessage[]

    get logs(): base.LogMessage[] {
        if (this.#logs == null) {
            this.#logs = []
        }
        return this.#logs
    }

    set logs(value: base.LogMessage[]) {
        this.#logs = value
    }
}

export class Instruction implements base.Instruction {
    accounts!: Base58[]
    data!: Hex
    programId!: Base58
    transactionIndex!: number
    instructionAddress!: number[]
    computeUnitsConsumed!: bigint
    error?: unknown
    isCommitted!: boolean
    hasDroppedLogMessages!: boolean

    #id?: string
    get id(): string {
        if (this.#id == null) {
            this.#id = formatId(this.block.header, this.transactionIndex, ...this.instructionAddress)
        }
        return this.#id
    }
    set id(value: string) {
        this.#id = value
    }

    #block!: base.Block
    #transaction?: base.Transaction
    #inner?: base.Instruction[]
    #parent?: base.Instruction
    #logs?: base.LogMessage[]
    #d1?: string
    #d2?: string
    #d4?: string
    #d8?: string

    get block(): base.Block {
        return this.#block
    }

    set block(value: base.Block) {
        this.#block = value
    }

    get transaction(): base.Transaction | undefined {
        return this.#transaction
    }

    set transaction(value: base.Transaction | undefined) {
        this.#transaction = value
    }

    getTransaction(): base.Transaction {
        if (this.#transaction == null) {
            throw new Error('Transaction is not set on instruction')
        }
        return this.#transaction
    }

    get inner(): base.Instruction[] {
        if (this.#inner == null) {
            this.#inner = []
        }
        return this.#inner
    }

    set inner(instructions: base.Instruction[]) {
        this.#inner = instructions
    }

    get parent(): base.Instruction | undefined {
        return this.#parent
    }

    getParent(): base.Instruction {
        if (this.#parent == null) {
            throw new Error('Parent instruction is not set')
        }
        return this.#parent
    }

    set parent(value: base.Instruction | undefined) {
        this.#parent = value
    }

    get logs(): base.LogMessage[] {
        if (this.#logs == null) {
            this.#logs = []
        }
        return this.#logs
    }

    set logs(value: base.LogMessage[]) {
        this.#logs = value
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

export class LogMessage implements base.LogMessage {
    transactionIndex!: number
    logIndex!: number
    instructionAddress!: number[]
    programId!: Base58
    kind!: 'log' | 'data' | 'other'
    message!: string

    #id?: string

    get id(): string {
        if (this.#id == null) {
            this.#id = formatId(this.block.header, this.transactionIndex, this.logIndex)
        }
        return this.#id
    }

    set id(value: string) {
        this.#id = value
    }

    #block!: base.Block
    #transaction?: base.Transaction
    #instruction?: base.Instruction

    get block(): base.Block {
        return this.#block
    }

    set block(value: base.Block) {
        this.#block = value
    }

    get transaction(): base.Transaction | undefined {
        return this.#transaction
    }

    set transaction(value: base.Transaction | undefined) {
        this.#transaction = value
    }

    getTransaction(): base.Transaction {
        if (this.#transaction == null) {
            throw new Error('Transaction is not set on log message')
        }
        return this.#transaction
    }

    get instruction(): base.Instruction | undefined {
        return this.#instruction
    }

    set instruction(value: base.Instruction | undefined) {
        this.#instruction = value
    }

    getInstruction(): base.Instruction {
        if (this.#instruction == null) {
            throw new Error('Instruction is not set on log message')
        }
        return this.#instruction
    }
}

export class Balance implements base.Balance {
    transactionIndex!: number
    account!: Base58
    pre!: bigint
    post!: bigint

    #id?: string

    get id(): string {
        if (this.#id == null) {
            this.#id = `${formatId(this.block.header, this.transactionIndex)}-${this.account}`
        }
        return this.#id
    }

    set id(value: string) {
        this.#id = value
    }

    #block!: base.Block
    #transaction?: base.Transaction

    get block(): base.Block {
        return this.#block
    }

    set block(value: base.Block) {
        this.#block = value
    }

    get transaction(): base.Transaction | undefined {
        return this.#transaction
    }

    set transaction(value: base.Transaction | undefined) {
        this.#transaction = value
    }

    getTransaction(): base.Transaction {
        if (this.#transaction == null) {
            throw new Error('Transaction is not set on balance change record')
        }
        return this.#transaction
    }
}

export class TokenBalance implements base.TokenBalance {
    transactionIndex!: number
    account!: Base58
    postAmount?: bigint | undefined
    postDecimals?: number | undefined
    postMint?: Base58 | undefined
    postOwner?: Base58 | undefined
    preAmount?: bigint | undefined
    preDecimals?: number | undefined
    preMint?: Base58 | undefined
    preOwner?: Base58 | undefined
    preProgramId?: Base58 | undefined
    postProgramId?: Base58 | undefined

    #id?: string

    get id(): string {
        if (this.#id == null) {
            this.#id = `${formatId(this.block.header, this.transactionIndex)}-${this.account}`
        }
        return this.#id
    }

    set id(value: string) {
        this.#id = value
    }

    #block!: base.Block
    #transaction?: base.Transaction

    get block(): base.Block {
        return this.#block
    }

    set block(value: base.Block) {
        this.#block = value
    }

    get transaction(): base.Transaction | undefined {
        return this.#transaction
    }

    set transaction(value: base.Transaction | undefined) {
        this.#transaction = value
    }

    getTransaction(): base.Transaction {
        if (this.#transaction == null) {
            throw new Error('Transaction is not set on balance change record')
        }
        return this.#transaction
    }
}

export class Reward implements base.Reward {
    pubkey!: Base58
    commission?: number
    lamports!: bigint
    postBalance!: bigint
    rewardType?: string

    #id?: string

    get id(): string {
        if (this.#id == null) {
            this.#id = `${formatId(this.block.header)}-${this.pubkey}`
        }
        return this.#id
    }

    #block!: base.Block

    get block(): base.Block {
        return this.#block
    }

    set block(value: base.Block) {
        this.#block = value
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
