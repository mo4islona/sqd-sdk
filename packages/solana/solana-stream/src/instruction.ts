import type {Hex} from '@sqd-sdk/core/internal/types/primitive'
import {toHex} from '@sqd-sdk/core/internal/hex'
import bs58 from 'bs58'

// FIXME: there is a bug with bs58
const bs58decode = bs58.decode || (bs58 as any).default.decode

export const DATA_SYM = Symbol('DATA')
export const D8_SYM = Symbol('D8')

interface Instruction {
    data: Hex
    [DATA_SYM]?: Uint8Array
    [D8_SYM]?: Hex
}

export function getInstructionData(i: Instruction): Uint8Array {
    if (i[DATA_SYM]) return i[DATA_SYM]
    i[DATA_SYM] = bs58decode(i.data)
    return i[DATA_SYM]
}

export function getInstructionDescriptor(i: Instruction): string {
    if (i[D8_SYM]) return i[D8_SYM]
    let bytes = toHex(getInstructionData(i))
    i[D8_SYM] = bytes.slice(0, 18)
    return i[D8_SYM]
}
