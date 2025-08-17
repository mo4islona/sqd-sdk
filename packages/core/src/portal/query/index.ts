import {unexpectedCase} from '../../internal/misc'
import type {Simplify} from './common'
import * as evm from './evm'
import * as solana from './solana'
import * as substrate from './substrate'
import type {Validator} from '../../validation'

export type {PortalBlock, PortalQuery} from './common'
export type {evm, solana, substrate}

export type Query = evm.Query | solana.Query | substrate.Query

export type GetBlock<Q extends Query> = Q extends evm.Query
    ? evm.Block<Q['fields']>
    : Q extends solana.Query
    ? solana.Block<Q['fields']>
    : substrate.Block<Q['fields']>

export function createQuery<Q extends Query>(query: Q): Simplify<Q & Query> {
    return {
        ...query,
        type: query.type,
        fields: query.fields,
    }
}

const BLOCK_SCHEMAS = new WeakMap<object, Map<string, Validator<any, any>>>()

export function getBlockSchema<Q extends Query>(query: Q): Validator<GetBlock<Q>, any> {
    let fieldsSchema = BLOCK_SCHEMAS.get(query.fields)
    if (!fieldsSchema) {
        fieldsSchema = new Map()
        BLOCK_SCHEMAS.set(query.fields, fieldsSchema)
    }

    let schema = fieldsSchema.get(query.type)
    if (!schema) {
        const type = query.type
        switch (type) {
            case 'solana':
                schema = solana.getBlockSchema(query.fields)
                break
            case 'evm':
                schema = evm.getBlockSchema(query.fields)
                break
            case 'substrate':
                schema = substrate.getBlockSchema(query.fields)
                break
            default:
                throw unexpectedCase(type)
        }

        fieldsSchema.set(type, schema)
    }

    return schema
}
