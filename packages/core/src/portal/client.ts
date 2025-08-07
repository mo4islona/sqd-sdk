import {type HttpResponse, type BaseHttpClient, HttpError} from '../http-client'
import {addErrorContext, last, unexpectedCase, wait, withAbort, withErrorContext} from '../internal/misc'
import {createFuture, type Future} from '../internal/async'
import {Throttler} from '../internal/throttler'

export interface PortalClientOptions {
    /**
     * The URL of the portal dataset.
     */
    url: string

    /**
     * Optional custom HTTP client to use.
     */
    http: BaseHttpClient

    /**
     * Minimum number of bytes to return.
     * @default 10_485_760 (10MB)
     */
    minBytes?: number

    /**
     * Maximum number of bytes to return.
     * @default minBytes
     */
    maxBytes?: number

    /**
     * Maximum time between stream data in milliseconds for return.
     * @default 300
     */
    maxIdleTime?: number

    /**
     * Maximum wait time in milliseconds for return.
     * @default 5_000
     */
    maxWaitTime?: number

    /**
     * Interval for polling the head in milliseconds.
     * @default 0
     */
    headPollInterval?: number
}

export interface PortalRequestOptions {
    headers?: HeadersInit
    retryAttempts?: number
    retrySchedule?: number[]
    httpTimeout?: number
    bodyTimeout?: number
    abort?: AbortSignal
}

export interface PortalStreamOptions {
    request?: Omit<PortalRequestOptions, 'abort'>

    minBytes?: number
    maxBytes?: number
    maxIdleTime?: number
    maxWaitTime?: number

    headPollInterval?: number
}

export type BlockRef = {
    readonly hash?: string
    readonly number: number
}

export type PortalStreamData<B> = {
    blocks: B[]
    finalizedHead?: BlockRef
    bytes: number
}

export interface PortalStream<B> extends AsyncIterable<PortalStreamData<B>> {}

export type PortalQuery = {
    type: string
    fromBlock?: number
    toBlock?: number
    parentBlockHash?: string
    [key: string]: unknown
}

export type PortalBlock = {
    header: {
        number: number
        hash?: string
    }
}

export class PortalClient {
    private url: URL
    private client: BaseHttpClient
    private headPollInterval: number
    private minBytes: number
    private maxBytes: number
    private maxIdleTime: number
    private maxWaitTime: number

    constructor(options: PortalClientOptions) {
        this.url = new URL(options.url)
        this.client = options.http
        this.headPollInterval = options.headPollInterval ?? 0
        this.minBytes = options.minBytes ?? 10 * 1024 * 1024
        this.maxBytes = options.maxBytes ?? this.minBytes
        this.maxIdleTime = options.maxIdleTime ?? 300
        this.maxWaitTime = options.maxWaitTime ?? 5_000
    }

    private getDatasetUrl(path: string): string {
        let u = new URL(this.url)
        if (this.url.pathname.endsWith('/')) {
            u.pathname += path
        } else {
            u.pathname += `/${path}`
        }
        return u.toString()
    }

    async getHead(options?: PortalRequestOptions): Promise<BlockRef | undefined> {
        const res = await this.client.request<BlockRef>(this.getDatasetUrl('head'), {...options, method: 'GET'})
        return res.body ?? undefined
    }

    async getFinalizedHead(options?: PortalRequestOptions): Promise<BlockRef | undefined> {
        const res = await this.client.request<BlockRef>(this.getDatasetUrl('finalized-head'), {
            ...options,
            method: 'GET',
        })
        return res.body ?? undefined
    }

    getFinalizedStream<Q extends PortalQuery = PortalQuery, R extends PortalBlock = any>(
        query: Q,
        options?: PortalStreamOptions
    ): PortalStream<R> {
        return createPortalStream(query, this.getStreamOptions(options), async (q, o) =>
            this.getStreamRequest('finalized-stream', q, o)
        )
    }

    getStream<Q extends PortalQuery = PortalQuery, R extends PortalBlock = any>(
        query: Q,
        options?: PortalStreamOptions
    ): PortalStream<R> {
        return createPortalStream(query, this.getStreamOptions(options), async (q, o) =>
            this.getStreamRequest('stream', q, o)
        )
    }

    private getStreamOptions(options?: PortalStreamOptions) {
        let {
            headPollInterval = this.headPollInterval,
            minBytes = this.minBytes,
            maxBytes = this.maxBytes,
            maxIdleTime = this.maxIdleTime,
            maxWaitTime = this.maxWaitTime,
            request = {},
        } = options ?? {}

        return {
            headPollInterval,
            minBytes,
            maxBytes,
            maxIdleTime,
            maxWaitTime,
            request,
        }
    }

    private async getStreamRequest(path: string, query: PortalQuery, options?: PortalRequestOptions) {
        try {
            let res = await this.client
                .request<ReadableStream | undefined>(this.getDatasetUrl(path), {
                    ...options,
                    method: 'POST',
                    json: query,
                    stream: true,
                })
                .catch(
                    withErrorContext({
                        query: query,
                    })
                )

            switch (res.status) {
                case 200: {
                    return {
                        finalizedHead: getFinalizedHeadHeader(res.headers),
                        stream: res.body
                            ?.pipeThrough(new TextDecoderStream('utf8'))
                            ?.pipeThrough(new LineSplitStream('\n')),
                    }
                }
                case 204:
                    return {
                        finalizedHead: getFinalizedHeadHeader(res.headers),
                    }
                default:
                    throw unexpectedCase(res.status)
            }
        } catch (e: unknown) {
            if (isForkHttpError(e) && query.fromBlock != null && query.parentBlockHash != null) {
                e = new ForkException(e.response.body.lastBlocks, {
                    number: query.fromBlock - 1,
                    hash: query.parentBlockHash,
                })
            }

            throw addErrorContext(e as any, {
                query,
            })
        }
    }
}

function isForkHttpError(err: unknown): err is HttpError {
    if (!(err instanceof HttpError)) return false
    if (err.response.status !== 409) return false
    if (err.response.body.lastBlocks == null) return false
    return true
}

function createPortalStream<Q extends PortalQuery = PortalQuery, R extends PortalBlock = any>(
    query: Q,
    options: Required<PortalStreamOptions>,
    requestStream: (
        query: Q,
        options?: PortalRequestOptions
    ) => Promise<{finalizedHead?: BlockRef; stream?: ReadableStream<string[]> | null | undefined}>
): PortalStream<R> {
    let {headPollInterval, request, ...bufferOptions} = options

    let buffer = new PortalStreamBuffer<R>(bufferOptions)

    let {fromBlock = 0, toBlock, parentBlockHash} = query

    const ingest = async () => {
        if (buffer.signal.aborted) return
        if (toBlock != null && fromBlock > toBlock) return

        let res = await requestStream(
            {
                ...query,
                fromBlock,
                parentBlockHash,
            },
            {
                ...request,
                abort: buffer.signal,
            }
        )

        const finalizedHead = res.finalizedHead

        // we are on head
        if (!('stream' in res)) {
            await buffer.put({blocks: [], bytes: 0, finalizedHead})
            buffer.flush()
            await wait(headPollInterval, buffer.signal)
            return ingest()
        }

        // no data left on this range
        if (res.stream == null) return

        let reader = res.stream.getReader()
        try {
            while (true) {
                let data = await reader.read()
                if (data.done) break

                let blocks: R[] = []
                let bytes = 0

                for (let line of data.value) {
                    let block = JSON.parse(line) as R
                    blocks.push(block)
                    bytes += line.length

                    fromBlock = block.header.number + 1
                    parentBlockHash = block.header.hash
                }

                await buffer.put({blocks, bytes, finalizedHead})
            }

            buffer.flush()
        } catch (err) {
            if (buffer.signal.aborted || isStreamAbortedError(err)) {
                // ignore
            } else {
                throw err
            }
        } finally {
            await reader?.cancel().catch(() => {})
        }

        return ingest()
    }

    ingest().then(
        () => buffer.close(),
        (err) => buffer.fail(err)
    )

    return buffer.iterate()
}

class PortalStreamBuffer<B> {
    private buffer: PortalStreamData<B> | undefined
    private state: 'pending' | 'ready' | 'failed' | 'closed' = 'pending'
    private error: unknown

    private readyFuture: Future<void> = createFuture()
    private takeFuture: Future<void> = createFuture()
    private putFuture: Future<void> = createFuture()

    private idleTimeout: ReturnType<typeof setTimeout> | undefined
    private waitTimeout: ReturnType<typeof setTimeout> | undefined

    private minBytes: number
    private maxBytes: number
    private maxIdleTime: number
    private maxWaitTime: number

    private abortController = new AbortController()

    get signal() {
        return this.abortController.signal
    }

    constructor(options: {maxWaitTime: number; maxBytes: number; maxIdleTime: number; minBytes: number}) {
        this.maxWaitTime = options.maxWaitTime
        this.minBytes = options.minBytes
        this.maxBytes = Math.max(options.maxBytes, options.minBytes)
        this.maxIdleTime = options.maxIdleTime
    }

    async take(): Promise<PortalStreamData<B> | undefined> {
        if (this.state === 'pending') {
            this.waitTimeout = setTimeout(() => this._ready(), this.maxWaitTime)
        }

        await Promise.all([this.readyFuture.promise(), this.putFuture.promise()])

        if (this.state === 'failed') {
            throw this.error
        }

        let result = this.buffer
        this.buffer = undefined

        if (this.state === 'closed') {
            return result
        }

        if (result == null) {
            throw new Error('Buffer is empty')
        }

        this.takeFuture.resolve()

        this.readyFuture = createFuture()
        this.putFuture = createFuture()
        this.takeFuture = createFuture()
        this.state = 'pending'

        return result
    }

    async put(data: PortalStreamData<B>) {
        if (this.state === 'closed' || this.state === 'failed') {
            throw new Error('Buffer is closed')
        }

        if (this.idleTimeout != null) {
            clearTimeout(this.idleTimeout)
            this.idleTimeout = undefined
        }

        if (this.buffer == null) {
            this.buffer = {blocks: [], bytes: 0}
        }

        this.buffer.bytes += data.bytes
        this.buffer.blocks.push(...data.blocks)
        this.buffer.finalizedHead = data.finalizedHead

        this.putFuture.resolve()

        if (this.buffer.bytes >= this.minBytes) {
            this.readyFuture.resolve()
        }

        if (this.buffer.bytes >= this.maxBytes) {
            await this.takeFuture.promise()
        }

        if (this.state === 'pending') {
            this.idleTimeout = setTimeout(() => this._ready(), this.maxIdleTime)
        }
    }

    flush() {
        if (this.buffer == null) return
        this._ready()
    }

    close() {
        if (this.state === 'closed' || this.state === 'failed') return
        this.state = 'closed'
        this._cleanup()
    }

    fail(err: any) {
        if (this.state === 'closed' || this.state === 'failed') return
        this.state = 'failed'
        this.error = err
        this._cleanup()
    }

    iterate() {
        return {
            [Symbol.asyncIterator]: (): AsyncIterator<PortalStreamData<B>> => {
                return {
                    next: async (): Promise<IteratorResult<PortalStreamData<B>>> => {
                        const value = await this.take()
                        if (value == null) {
                            return {done: true, value: undefined}
                        }
                        return {done: false, value}
                    },
                    return: async (): Promise<IteratorResult<PortalStreamData<B>>> => {
                        this.close()
                        return {done: true, value: undefined}
                    },
                    throw: async (error?: any): Promise<IteratorResult<PortalStreamData<B>>> => {
                        this.fail(error)
                        throw error
                    },
                }
            },
        }
    }

    private _ready() {
        if (this.state === 'pending') {
            this.state = 'ready'
            this.readyFuture.resolve()
        }
        if (this.idleTimeout != null) {
            clearTimeout(this.idleTimeout)
            this.idleTimeout = undefined
        }
        if (this.waitTimeout != null) {
            clearTimeout(this.waitTimeout)
            this.waitTimeout = undefined
        }
    }

    private _cleanup() {
        if (this.idleTimeout != null) {
            clearTimeout(this.idleTimeout)
            this.idleTimeout = undefined
        }
        if (this.waitTimeout != null) {
            clearTimeout(this.waitTimeout)
            this.waitTimeout = undefined
        }
        this.readyFuture.resolve()
        this.putFuture.resolve()
        this.takeFuture.resolve()
        this.abortController.abort()
    }
}

class LineSplitStream implements ReadableWritablePair<string[], string> {
    private line = ''
    private transform: TransformStream<string, string[]>

    get readable() {
        return this.transform.readable
    }
    get writable() {
        return this.transform.writable
    }

    constructor(separator: string) {
        this.transform = new TransformStream({
            transform: (chunk, controller) => {
                let lines = chunk.split(separator)
                if (lines.length === 1) {
                    this.line += lines[0]
                } else {
                    let result: string[] = []
                    lines[0] = this.line + lines[0]
                    this.line = lines.pop() || ''
                    result.push(...lines)
                    controller.enqueue(result)
                }
            },
            flush: (controller) => {
                if (this.line) {
                    controller.enqueue([this.line])
                    this.line = ''
                }
                // NOTE: not needed according to the spec, but done the same way in nodejs sources
                controller.terminate()
            },
        })
    }
}

export class ForkException extends Error {
    readonly name = 'ForkError'

    constructor(readonly lastBlocks: BlockRef[], readonly head: BlockRef) {
        let parent = last(lastBlocks)
        super(
            `expected ${head.number + 1} to have parent ${parent.number}#${parent.hash}, but got ${head.number}#${
                head.hash
            }`
        )
    }
}

export function isForkException(err: unknown): err is ForkException {
    if (err instanceof ForkException) return true
    if (err instanceof Error && err.name === 'ForkError') return true
    return false
}

function getFinalizedHeadHeader(headers: HttpResponse['headers']) {
    let finalizedHeadHash = headers.get('X-Sqd-Finalized-Head-Hash')
    let finalizedHeadNumber = headers.get('X-Sqd-Finalized-Head-Number')

    return finalizedHeadHash != null && finalizedHeadNumber != null
        ? {
              hash: finalizedHeadHash,
              number: Number.parseInt(finalizedHeadNumber),
          }
        : undefined
}

function isStreamAbortedError(err: unknown) {
    if (!(err instanceof Error)) return false
    if (!('code' in err)) return false
    switch (err.code) {
        case 'ABORT_ERR':
        case 'ERR_STREAM_PREMATURE_CLOSE':
        case 'ECONNRESET':
            return true
        default:
            return false
    }
}
