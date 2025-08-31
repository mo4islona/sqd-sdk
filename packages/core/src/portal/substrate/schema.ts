import {
    ANY,
    ANY_OBJECT,
    array,
    BIG_NAT,
    BOOLEAN,
    BYTES,
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
import type {
    ExtrinsicFields,
    CallFields,
    EventFields,
    Block,
    BlockHeaderFields,
    FieldSelection,
    ExtrinsicSignature,
} from './query'

export function getBlockSchema<F extends FieldSelection>(fields: F): Validator<Block<F>, unknown> {
    const header = object(project(BlockHeaderShape, {...fields.block, number: true, hash: true}))
    const extrinsic = object(project(ExtrinsicShape, fields.extrinsic))
    const call = object(project(CallShape, fields.call))

    const event = object(project(EventShape, fields.event))

    return object({
        header,
        events: withDefault([], array(event)),
        calls: withDefault([], array(call)),
        extrinsics: withDefault([], array(extrinsic)),
    }) as Validator<Block<F>, unknown>
}

const BlockHeaderShape: ObjectValidatorShape<BlockHeaderFields> = {
    number: NAT,
    hash: BYTES,
    parentHash: BYTES,
    stateRoot: BYTES,
    extrinsicsRoot: BYTES,
    digest: object({
        logs: array(BYTES),
    }),
    specName: STRING,
    specVersion: NAT,
    implName: STRING,
    implVersion: NAT,
    timestamp: NAT,
    validator: BYTES,
}

const ExtrinsicSignatureValidator: Validator<ExtrinsicSignature, unknown> = object({
    address: BYTES,
    signature: ANY_OBJECT,
    signedExtensions: ANY_OBJECT,
})

const ExtrinsicShape: ObjectValidatorShape<ExtrinsicFields> = {
    index: NAT,
    version: NAT,
    signature: option(ExtrinsicSignatureValidator),
    fee: BIG_NAT,
    tip: BIG_NAT,
    error: nullable(STRING),
    success: BOOLEAN,
    hash: BYTES,
}

const CallShape: ObjectValidatorShape<CallFields> = {
    extrinsicIndex: NAT,
    address: array(NAT),
    name: STRING,
    args: ANY,
    origin: ANY,
    error: ANY,
    success: option(BOOLEAN),
}

const EventShape: ObjectValidatorShape<EventFields> = {
    index: NAT,
    name: STRING,
    args: ANY,
    phase: oneOf({
        Initialization: constant('Initialization'),
        ApplyExtrinsic: constant('ApplyExtrinsic'),
        Finalization: constant('Finalization'),
    }),
    extrinsicIndex: NAT,
    callAddress: array(NAT),
    topics: array(BYTES),
}
