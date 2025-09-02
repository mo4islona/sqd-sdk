import {createSolanaSwapsStream} from './solana-swap-stream'

/*
 1) We have to copy `range: {from: 317617480}` for all logs/instructions. We cant use strings like 317,617,480 / 317_617_480
 2) Types stop working if you extract a function Message
 3) read: async function* (opts: DataReadRequest<BlockRef, SolanaDataRequestRange[]>) {  ???

 */

async function main() {
    const stream = createSolanaSwapsStream({
        portal: 'https://portal.sqd.dev/datasets/solana-mainnet',
        types: ['orca_whirlpool'],
    })

    for await (const blocks of stream) {
        for (const {swaps, header} of blocks) {
            console.log(`${swaps?.length} swaps for slot #${header.number}`)
        }
    }
}

void main()
