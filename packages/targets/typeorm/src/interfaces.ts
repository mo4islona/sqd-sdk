export interface HashAndHeight {
    number: number
    hash: string
}

export interface DatabaseState {
    number: number
    hash: string
    top: HashAndHeight[]
    nonce: number
}

export interface FinalTxInfo {
    prevHead: HashAndHeight
    nextHead: HashAndHeight
}

export interface HotTxInfo {
    finalizedHead: HashAndHeight
    baseHead: HashAndHeight
    newBlocks: HashAndHeight[]
}
