import type {Data, DataBatch} from '../data'
import {DataSource, DataTarget, type DataDuplex, type DataDuplexFactory} from '../core'
import {DataRef} from '../data'
import type {DataFork} from '../data'
import {ForkException} from '../errors'
import {SyncQueue, createFuture} from '../../internal/async'

export interface TransformerFactoryOptions<TIn extends Data> {
    offset: TIn['id'] | undefined
    ref: DataRef<TIn['id']>
}

export interface TransformerSetup<TIn extends Data, TOut extends Data> {
    /**
     * The input offset to start consuming upstream from.
     * Usually derived from the downstream requested offset.
     */
    offset: TIn['id'] | undefined

    /**
     * The DataRef for the output id type.
     */
    ref: DataRef<TOut['id']>

    /**
     * Transform an incoming upstream batch into an outgoing downstream batch.
     */
    transform(batch: DataBatch<TIn>): Promise<DataBatch<TOut>> | DataBatch<TOut>

    /**
     * Optional fork mapper to propagate forks downstream.
     * If omitted, the fork will be forwarded as-is (assuming compatible id types).
     */
    fork?(fork: DataFork<TIn['id']>): Promise<DataFork<TOut['id']>> | DataFork<TOut['id']>
}

export interface TransformerConfig<TIn extends Data, TOut extends Data> {
    transformer: (
        opts: TransformerFactoryOptions<TIn>
    ) => Promise<TransformerSetup<TIn, TOut>> | TransformerSetup<TIn, TOut>
}

/**
 * Create a duplex that transforms batches from TIn to TOut, preserving the unfinalized flag.
 *
 * The returned factory produces a duplex bound to a concrete ref/unfinalized context
 * provided by the upstream source via pipeThrough.
 */
export function transformer<TIn extends Data, TOut extends Data, TUnfinalized extends boolean>(
    config: TransformerConfig<TIn, TOut>
): DataDuplexFactory<TIn, TOut, TUnfinalized, TUnfinalized> {
    return (opts): DataDuplex<TIn, TOut, TUnfinalized, TUnfinalized> => {
        type InId = TIn['id']
        type OutId = TOut['id']

        let queue = new SyncQueue<DataBatch<TOut>>()

        // Setup is created lazily when the downstream source is read the first time.
        let setupFuture = createFuture<void>()
        let setup: TransformerSetup<TIn, TOut> | undefined
        let requestedUpstreamOffset: InId | undefined
        let pendingFork: ForkException<OutId> | undefined

        // Proxy ref that delegates to the real output ref once setup is initialized.
        let realOutRef: DataRef<OutId> | undefined
        const proxyOutRef: DataRef<OutId> = {
            compare: (a: OutId, b: OutId) => (realOutRef ? realOutRef.compare(a, b) : DataRef.Equal),
        }

        const target = new DataTarget<TIn, TUnfinalized>({
            unfinalized: opts.unfinalized as any,
            ref: opts.ref,
            writer: async () => {
                return {
                    async next(batch?: DataBatch<TIn>) {
                        // Ensure setup is ready (await downstream reader providing its desired offset)
                        await setupFuture.promise()
                        const s = setup as TransformerSetup<TIn, TOut>

                        if (batch == null) {
                            // initial call: return starting input offset
                            return {done: false, value: s.offset}
                        }

                        // If downstream requested a restart with a new offset, instruct upstream to restart
                        if (requestedUpstreamOffset !== undefined) {
                            const newOffset = requestedUpstreamOffset
                            requestedUpstreamOffset = undefined
                            return {done: false, value: newOffset}
                        }

                        const outBatch = await s.transform(batch)
                        await queue.put(outBatch)
                        // Continue from the upstream batch offset
                        return {done: false, value: batch.offset}
                    },
                    async fork(fork) {
                        // Transform and propagate fork downstream; instruct upstream to restart from last head
                        const s = setup
                        const outFork = s?.fork ? await s.fork(fork) : (fork as unknown as DataFork<OutId>)
                        pendingFork = new ForkException<OutId>(outFork)
                        const upstreamOffset = fork.heads[fork.heads.length - 1] as InId | undefined
                        return {done: false, value: upstreamOffset}
                    },
                    async return() {
                        queue.close()
                        return {done: true, value: undefined}
                    },
                }
            },
        })

        const source = new DataSource<TOut, TUnfinalized>({
            unfinalized: opts.unfinalized as any,
            ref: proxyOutRef,
            reader: async (readOpts) => {
                // Recreate setup on each reader creation so we can map downstream offset to upstream offset
                const s = await config.transformer({offset: readOpts.offset as InId | undefined, ref: opts.ref})
                setup = s
                realOutRef = s.ref
                requestedUpstreamOffset = s.offset
                pendingFork = undefined
                // Reset output stream buffer
                queue = new SyncQueue<DataBatch<TOut>>()
                // Resolve the initial setup once
                setupFuture.resolve()

                return {
                    async next() {
                        if (pendingFork) {
                            const err = pendingFork
                            pendingFork = undefined
                            throw err
                        }

                        const value = await queue.take()
                        if (value == null) return {done: true as const, value: undefined}
                        return {done: false as const, value}
                    },
                    async return() {
                        // Do not close the shared queue here; allow target to restart the stream from a new offset
                        return {done: true as const, value: undefined}
                    },
                }
            },
        })

        return {target, source}
    }
}
