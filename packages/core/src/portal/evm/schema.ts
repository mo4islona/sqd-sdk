import {
    array,
    BYTES,
    constant,
    NAT,
    oneOf,
    option,
    STRING,
    taggedUnion,
    withDefault,
    type Validator,
    object,
    QTY,
    nullable,
} from '~/validation'
import {project, type ObjectValidatorShape} from '../common'
import type {
    FieldSelection,
    Block,
    BlockHeaderFields,
    LogFields,
    TransactionFields,
    Trace,
    TraceBaseFields,
    TraceCreateActionFields,
    TraceCreateResultFields,
    TraceCallActionFields,
    TraceCallResultFields,
    TraceSuicideActionFields,
    TraceRewardActionFields,
    StateDiff,
    StateDiffBaseFields,
    StateDiffAddFields,
    StateDiffDeleteFields,
    StateDiffChangeFields,
    StateDiffNoChangeFields,
} from './query'

export const getBlockSchema = <F extends FieldSelection>(fields: F): Validator<Block<F>, unknown> => {
    let header = object(project(BlockHeaderShape, {...fields.block, number: true, hash: true}))
    let log = object(project(LogShape, fields.log))
    let transaction = object(project(TransactionShape, fields.transaction))
    let trace = getTraceFrameValidator(fields.trace)
    let stateDiff = getStateDiffValidator(fields.stateDiff)

    return object({
        header,
        logs: withDefault([], array(log)),
        transactions: withDefault([], array(transaction)),
        traces: withDefault([], array(trace)),
        stateDiffs: withDefault([], array(stateDiff)),
    }) as Validator<Block<F>, unknown>
}

const BlockHeaderShape: ObjectValidatorShape<BlockHeaderFields> = {
    number: NAT,
    hash: BYTES,
    parentHash: BYTES,
    timestamp: NAT,
    transactionsRoot: BYTES,
    receiptsRoot: BYTES,
    stateRoot: BYTES,
    logsBloom: BYTES,
    sha3Uncles: BYTES,
    extraData: BYTES,
    miner: BYTES,
    nonce: BYTES,
    mixHash: BYTES,
    size: NAT,
    gasLimit: QTY,
    gasUsed: QTY,
    difficulty: QTY,
    totalDifficulty: option(QTY),
    baseFeePerGas: QTY,
    blobGasUsed: QTY,
    excessBlobGas: QTY,
    l1BlockNumber: option(NAT),
}

const LogShape: ObjectValidatorShape<LogFields> = {
    logIndex: NAT,
    transactionIndex: NAT,
    transactionHash: BYTES,
    address: BYTES,
    data: BYTES,
    topics: array(BYTES),
}

const TransactionShape: ObjectValidatorShape<TransactionFields> = {
    transactionIndex: NAT,
    hash: BYTES,
    nonce: NAT,
    from: BYTES,
    to: option(BYTES),
    input: BYTES,
    value: QTY,
    gas: QTY,
    gasPrice: QTY,
    maxFeePerGas: option(QTY),
    maxPriorityFeePerGas: option(QTY),
    v: QTY,
    r: BYTES,
    s: BYTES,
    yParity: option(NAT),
    chainId: option(NAT),
    sighash: option(BYTES),
    contractAddress: option(BYTES),
    gasUsed: QTY,
    cumulativeGasUsed: QTY,
    effectiveGasPrice: QTY,
    type: NAT,
    status: NAT,
    blobVersionedHashes: option(array(BYTES)),
    l1Fee: option(QTY),
    l1FeeScalar: option(NAT),
    l1GasPrice: option(QTY),
    l1GasUsed: option(QTY),
    l1BlobBaseFee: option(QTY),
    l1BlobBaseFeeScalar: option(NAT),
    l1BaseFeeScalar: option(NAT),
}

function getTraceFrameValidator<T extends FieldSelection['trace']>(
    fields: T,
): Validator<Trace<NonNullable<T>>, unknown> {
    let BaseShape = project(TraceBaseShape, fields)

    let createAction = object(
        project(TraceCreateActionShape, {
            from: fields?.createFrom,
            value: fields?.createValue,
            gas: fields?.createGas,
            init: fields?.createInit,
        }),
    )

    let createResult = object(
        project(TraceCreateResultShape, {
            gasUsed: fields?.createResultGasUsed,
            code: fields?.createResultCode,
            address: fields?.createResultAddress,
        }),
    )

    let create = object({
        ...BaseShape,
        type: constant('create'),
        action: withDefault({}, createAction),
        result: isEmpty(createResult) ? undefined : option(createResult),
    })

    let callAction = object(
        project(TraceCallActionShape, {
            callType: fields?.callCallType,
            from: fields?.callFrom,
            to: fields?.callTo,
            value: fields?.callValue,
            gas: fields?.callGas,
            input: fields?.callInput,
            sighash: fields?.callSighash,
        }),
    )

    let callResult = object(
        project(TraceCallResultShape, {
            gasUsed: fields?.callResultGasUsed,
            output: fields?.callResultOutput,
        }),
    )

    let call = object({
        ...BaseShape,
        type: constant('call'),
        action: withDefault({}, callAction),
        result: isEmpty(callResult) ? undefined : option(callResult),
    })

    let suicideAction = object(
        project(TraceSuicideActionShape, {
            address: fields?.suicideAddress,
            refundAddress: fields?.suicideRefundAddress,
            balance: fields?.suicideBalance,
        }),
    )

    let suicide = object({
        ...BaseShape,
        type: constant('suicide'),
        action: withDefault({}, suicideAction),
    })

    let rewardAction = object(
        project(TraceRewardActionShape, {
            author: fields?.rewardAuthor,
            value: fields?.rewardValue,
            type: fields?.rewardType,
        }),
    )

    let reward = object({
        ...BaseShape,
        type: constant('reward'),
        action: withDefault({}, rewardAction),
    })

    return taggedUnion('type', {
        create,
        call,
        suicide,
        reward,
    }) as Validator<Trace<NonNullable<T>>, unknown>
}

const TraceBaseShape: ObjectValidatorShape<TraceBaseFields> = {
    type: oneOf({
        create: constant('create'),
        call: constant('call'),
        suicide: constant('suicide'),
        reward: constant('reward'),
    }),
    transactionIndex: NAT,
    traceAddress: array(NAT),
    subtraces: NAT,
    error: nullable(STRING),
    revertReason: option(STRING),
}

const TraceCreateActionShape: ObjectValidatorShape<TraceCreateActionFields> = {
    from: BYTES,
    value: QTY,
    gas: QTY,
    init: BYTES,
}

const TraceCreateResultShape: ObjectValidatorShape<TraceCreateResultFields> = {
    gasUsed: QTY,
    code: option(BYTES),
    address: BYTES,
}

const TraceCallActionShape: ObjectValidatorShape<TraceCallActionFields> = {
    callType: STRING,
    from: BYTES,
    to: BYTES,
    value: option(QTY),
    gas: QTY,
    input: BYTES,
    sighash: option(BYTES),
}

const TraceCallResultShape: ObjectValidatorShape<TraceCallResultFields> = {
    gasUsed: QTY,
    output: option(BYTES),
}

const TraceSuicideActionShape: ObjectValidatorShape<TraceSuicideActionFields> = {
    address: BYTES,
    refundAddress: BYTES,
    balance: QTY,
}

const TraceRewardActionShape: ObjectValidatorShape<TraceRewardActionFields> = {
    author: BYTES,
    value: QTY,
    type: STRING,
}

function getStateDiffValidator<T extends FieldSelection['stateDiff']>(
    fields: T,
): Validator<StateDiff<NonNullable<T>>, unknown> {
    return taggedUnion('kind', {
        '+': object(project(StateDiffAddShape, fields)),
        '-': object(project(StateDiffDeleteShape, fields)),
        '*': object(project(StateDiffChangeShape, fields)),
        '=': object(project(StateDiffNoChangeShape, fields)),
    }) as Validator<StateDiff<NonNullable<T>>, unknown>
}

const StateDiffBaseShape: ObjectValidatorShape<StateDiffBaseFields> = {
    kind: oneOf({
        '+': constant('+'),
        '-': constant('-'),
        '*': constant('*'),
        '=': constant('='),
    }),
    transactionIndex: NAT,
    address: BYTES,
    key: STRING,
}

const StateDiffAddShape: ObjectValidatorShape<StateDiffAddFields> = {
    ...StateDiffBaseShape,
    kind: constant('+'),
    next: BYTES,
}

const StateDiffDeleteShape: ObjectValidatorShape<StateDiffDeleteFields> = {
    ...StateDiffBaseShape,
    kind: constant('-'),
    prev: BYTES,
}

const StateDiffChangeShape: ObjectValidatorShape<StateDiffChangeFields> = {
    ...StateDiffBaseShape,
    kind: constant('*'),
    prev: BYTES,
    next: BYTES,
}

const StateDiffNoChangeShape: ObjectValidatorShape<StateDiffNoChangeFields> = {
    ...StateDiffBaseShape,
    kind: constant('='),
}

export function isEmpty(obj: object): boolean {
    for (let _ in obj) {
        return false
    }
    return true
}
