import {
    ANY,
    ANY_OBJECT,
    array,
    B58,
    BIG_NAT,
    BOOLEAN,
    constant,
    NAT,
    nullable,
    object,
    oneOf,
    option,
    STRING,
    withDefault,
    type Validator,
} from '~/validation'
import {project, type ObjectValidatorShape} from '../common'
import type {FieldSelection, Block, BlockHeaderFields, TransactionFields, AddressTableLookup} from './query'
import type {
    InstructionFields,
    LogMessageFields,
    BalanceFields,
    PreTokenBalanceFields,
    PostTokenBalanceFields,
    PrePostTokenBalanceFields,
    RewardFields,
} from './query'

export function getBlockSchema<F extends FieldSelection>(fields: F): Validator<Block<F>, unknown> {
    let header = object(project(BlockHeaderShape, {...fields.block, number: true, hash: true}))

    let transaction = object(project(TransactionShape, fields.transaction))

    let instruction = object(project(InstructionShape, fields.instruction))

    let balance = object(project(BalanceShape, fields.balance))

    let tokenBalance = oneOf({
        pre: object(project(PreTokenBalanceShape, fields.tokenBalance)),
        post: object(project(PostTokenBalanceShape, fields.tokenBalance)),
        prePost: object(project(PrePostTokenBalanceShape, fields.tokenBalance)),
    })

    let logMessage = object(project(LogMessageShape, fields.log))

    let reward = object(project(RewardShape, fields.reward))

    return object({
        header,
        transactions: withDefault([], array(transaction)),
        instructions: withDefault([], array(instruction)),
        logs: withDefault([], array(logMessage)),
        balances: withDefault([], array(balance)),
        tokenBalances: withDefault([], array(tokenBalance)),
        rewards: withDefault([], array(reward)),
    }) as Validator<Block<F>, unknown>
}

const BlockHeaderShape: ObjectValidatorShape<BlockHeaderFields> = {
    hash: B58,
    number: NAT,
    height: NAT,
    parentNumber: NAT,
    parentHash: B58,
    timestamp: NAT,
}

const AddressTableLookupValidator: Validator<AddressTableLookup> = object({
    accountKey: B58,
    readonlyIndexes: array(NAT),
    writableIndexes: array(NAT),
})

const TransactionShape: ObjectValidatorShape<TransactionFields> = {
    transactionIndex: NAT,
    version: oneOf({legacy: constant('legacy'), versionNumber: NAT}),
    accountKeys: array(B58),
    addressTableLookups: array(AddressTableLookupValidator),
    numReadonlySignedAccounts: NAT,
    numReadonlyUnsignedAccounts: NAT,
    numRequiredSignatures: NAT,
    recentBlockhash: B58,
    signatures: array(B58),
    err: nullable(ANY_OBJECT),
    computeUnitsConsumed: BIG_NAT,
    fee: BIG_NAT,
    loadedAddresses: option(object({readonly: array(B58), writable: array(B58)})),
    hasDroppedLogMessages: BOOLEAN,
}

const InstructionShape: ObjectValidatorShape<InstructionFields> = {
    transactionIndex: NAT,
    instructionAddress: array(NAT),
    programId: B58,
    accounts: array(B58),
    data: B58,
    computeUnitsConsumed: option(BIG_NAT),
    error: ANY,
    isCommitted: BOOLEAN,
    hasDroppedLogMessages: BOOLEAN,
}

const LogMessageShape: ObjectValidatorShape<LogMessageFields> = {
    transactionIndex: NAT,
    logIndex: NAT,
    instructionAddress: array(NAT),
    programId: B58,
    kind: oneOf({log: constant('log'), data: constant('data'), other: constant('other')}),
    message: STRING,
}

const BalanceShape: ObjectValidatorShape<BalanceFields> = {
    transactionIndex: NAT,
    account: B58,
    pre: BIG_NAT,
    post: BIG_NAT,
}

const PreTokenBalanceShape: ObjectValidatorShape<PreTokenBalanceFields> = {
    transactionIndex: NAT,
    account: B58,
    preProgramId: option(B58),
    preMint: B58,
    preDecimals: NAT,
    preOwner: option(B58),
    preAmount: BIG_NAT,
}

const PostTokenBalanceShape: ObjectValidatorShape<PostTokenBalanceFields> = {
    transactionIndex: NAT,
    account: B58,
    postProgramId: option(B58),
    postMint: B58,
    postDecimals: NAT,
    postOwner: option(B58),
    postAmount: BIG_NAT,
}

const PrePostTokenBalanceShape: ObjectValidatorShape<PrePostTokenBalanceFields> = {
    transactionIndex: NAT,
    account: B58,
    preProgramId: option(B58),
    preMint: B58,
    preDecimals: NAT,
    preOwner: option(B58),
    preAmount: BIG_NAT,
    postProgramId: option(B58),
    postMint: B58,
    postDecimals: NAT,
    postOwner: option(B58),
    postAmount: BIG_NAT,
}

const RewardShape: ObjectValidatorShape<RewardFields> = {
    pubkey: B58,
    lamports: BIG_NAT,
    postBalance: BIG_NAT,
    rewardType: option(STRING),
    commission: option(NAT),
}
