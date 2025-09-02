import {createLogger, createTracker} from '@belopash/core'
import {maybeLast} from '@belopash/core/internal'

function createTimer() {
    let start: number | undefined = undefined

    return {
        start: () => {
            start = Date.now()
        },
        stop: () => {
            if (start == null) return 0
            let elapsed = Date.now() - start
            start = undefined
            return elapsed
        },
    }
}

export function createProgressTracker<
    TCursor extends {number: number; hash: string},
    TValue extends {header: {timestamp: number}}[],
    TQuery,
>(prefix?: string) {
    const logger = createLogger(`sqd:${prefix}`)

    const readTimer = createTimer()
    const writeTimer = createTimer()
    const logIntervalMs = 5_000

    let stats:
        | {
              cursor: TCursor | undefined
              head: TCursor | undefined
              finalizedHead: TCursor | undefined
              lastBlockTime: number | undefined
              avgReadTime: number | undefined
              lastReadTime: number | undefined
              avgWriteTime: number | undefined
              lastWriteTime: number | undefined
              startNumber: number | undefined
              startTimeMs: number | undefined
              targetNumber: number | undefined
              lastLogTimeMs: number | undefined
              lastLogTotalBlocks: number | undefined
              totalBlocks: number | undefined
              lastBatchSize: number | undefined
              avgBatchSize: number | undefined
              avgBlocksPerSec: number | undefined
          }
        | undefined = undefined

    let interval: NodeJS.Timeout | null = null

    const emitLog = () => {
        if (!stats?.cursor || !stats.head) return

        const now = Date.now()
        const headNumber = stats.head?.number ?? stats.cursor.number
        const finalizedNumber = stats.finalizedHead?.number
        const remainingBlocks = Math.max(0, headNumber - stats.cursor.number)
        const percentVal = (1 - remainingBlocks / headNumber) * 100
        const percent = Math.max(0, Math.min(100, percentVal))
        const etaSec =
            stats.avgBlocksPerSec && stats.avgBlocksPerSec > 0 ? remainingBlocks / stats.avgBlocksPerSec : undefined
        const percentStr = `${percent.toFixed(2)}%`
        const etaStr = etaSec == null ? 'n/a' : `${etaSec.toFixed(0)}s`

        // Compute windowed throughput since last log; if nothing happened, it decays to 0
        const windowMs = now - (stats.lastLogTimeMs ?? now)
        const windowBlocks = (stats.totalBlocks ?? 0) - (stats.lastLogTotalBlocks ?? 0)
        const windowBlocksPerSec = windowMs > 0 ? (windowBlocks * 1000) / windowMs : 0
        stats.avgBlocksPerSec =
            stats.avgBlocksPerSec == null ? windowBlocksPerSec : (stats.avgBlocksPerSec + windowBlocksPerSec) / 2

        logger.info(
            {
                lag: `${((now - (stats.lastBlockTime ?? now)) / 1000).toFixed(2)}s`,
                batchSize: stats.lastBatchSize ?? 0,
                blocksPerSec: Number(windowBlocksPerSec.toFixed(2)),
                avgBlocksPerSec: Number((stats.avgBlocksPerSec ?? 0).toFixed(2)),
                avgBatchSize: Number((stats.avgBatchSize ?? 0).toFixed(2)),
                avgReadTime: `${((stats.avgReadTime ?? 0) / 1000).toFixed(2)}s`,
                lastReadTime: `${((stats.lastReadTime ?? 0) / 1000).toFixed(2)}s`,
                avgWriteTime: `${((stats.avgWriteTime ?? 0) / 1000).toFixed(2)}s`,
                lastWriteTime: `${((stats.lastWriteTime ?? 0) / 1000).toFixed(2)}s`,
                totalBlocks: stats.totalBlocks,
            },
            `progress: ${stats.cursor.number} / ${headNumber} (${finalizedNumber ?? 0}) — ${percentStr}, ETA: ${etaStr}`,
        )

        // Update window markers
        stats.lastLogTimeMs = now
        stats.lastLogTotalBlocks = stats.totalBlocks ?? 0
    }

    return createTracker<TCursor, TValue, TQuery>({
        beforeRead: async () => {
            if (!stats) {
                stats = {
                    cursor: undefined,
                    head: undefined,
                    finalizedHead: undefined,
                    lastBlockTime: undefined,
                    avgReadTime: undefined,
                    avgWriteTime: undefined,
                    lastReadTime: undefined,
                    lastWriteTime: undefined,
                    startNumber: undefined,
                    startTimeMs: Date.now(),
                    targetNumber: undefined,
                    lastLogTimeMs: Date.now(),
                    lastLogTotalBlocks: 0,
                    totalBlocks: 0,
                    lastBatchSize: undefined,
                    avgBatchSize: undefined,
                    avgBlocksPerSec: undefined,
                }
            }

            if (!interval) {
                interval = setInterval(emitLog, logIntervalMs)
            }

            readTimer.start()
        },
        afterRead: async () => {
            if (!stats) return

            const elapsed = readTimer.stop()

            stats.avgReadTime = stats.avgReadTime == null ? elapsed : (stats.avgReadTime + elapsed) / 2
            stats.lastReadTime = elapsed

            await new Promise((resolve) => setTimeout(resolve, 0))
        },
        beforeWrite: () => {
            writeTimer.start()
        },
        afterWrite: (message) => {
            if (!stats) return

            const lastItem = maybeLast(message.data)
            const lastBlock = lastItem ? maybeLast(lastItem.value) : undefined

            const elapsed = writeTimer.stop()
            stats.avgWriteTime = stats.avgWriteTime == null ? elapsed : (stats.avgWriteTime + elapsed) / 2
            stats.lastWriteTime = elapsed
            stats.lastBlockTime = (lastBlock?.header.timestamp ?? 0) * 1000
            stats.head = message.head
            stats.finalizedHead = message.finalizedHead
            stats.cursor = lastItem?.cursor ?? stats.cursor
            if (stats.targetNumber == null) {
                stats.targetNumber = message.finalizedHead?.number ?? message.head.number
            }

            const batchSize = message.data.length
            stats.totalBlocks = (stats.totalBlocks ?? 0) + batchSize
            stats.lastBatchSize = batchSize
            stats.avgBatchSize = stats.avgBatchSize == null ? batchSize : (stats.avgBatchSize + batchSize) / 2

            const cycleMs = (stats.lastReadTime ?? 0) + (stats.lastWriteTime ?? 0)
            const instBlocksPerSec = cycleMs > 0 ? (batchSize * 1000) / cycleMs : 0
            stats.avgBlocksPerSec =
                stats.avgBlocksPerSec == null ? instBlocksPerSec : (stats.avgBlocksPerSec + instBlocksPerSec) / 2
        },
        afterEnd: () => {
            if (interval) {
                clearInterval(interval)
            }
            // emitLog()
        },
    })
}
