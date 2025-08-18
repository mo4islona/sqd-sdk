import {createFuture, type Future} from '@sqd-sdk/core/internal/async'

export class Mutex {
    private waitQueue: Array<Future<void>> = []

    async acquire(): Promise<void> {
        this.waitQueue.push(createFuture())
        return this.waitQueue.length > 1 ? this.waitQueue[this.waitQueue.length - 2]?.promise() : undefined
    }

    release(): void {
        if (this.waitQueue.length === 0) return
        const future = this.waitQueue.shift()
        future?.resolve()
    }

    async wait(): Promise<void> {
        if (this.waitQueue.length === 0) return
        return this.waitQueue[0]?.promise()
    }
}
