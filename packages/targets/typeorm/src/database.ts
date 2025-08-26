import {createLogger} from '@sqd-sdk/core/logger'
import {assert, assertNotNull, maybeLast} from '@sqd-sdk/core/internal/misc'
import {DataSource, type EntityManager} from 'typeorm'
import {Store} from './store'
import {StateManager} from './utils/stateManager'
import {createOrmConfig} from '@subsquid/typeorm-config'
import {ChangeTracker, rollbackBlock} from './utils/hot'
import type {DatabaseState, HashAndHeight} from './interfaces'
import {def} from '@sqd-sdk/core/internal/def'
import {createTarget, type DataBatchMessage, type DataForkMessage} from '@sqd-sdk/core/pipeline'

export type IsolationLevel = 'SERIALIZABLE' | 'READ COMMITTED' | 'REPEATABLE READ'

export interface TypeormDatabaseOptions {
    isolationLevel?: IsolationLevel
    stateSchema?: string
    projectDir?: string

    /**
     * If true, will batch write operations
     * @default true
     */
    postponeWriteOperations?: boolean

    /**
     * If true, will cache entities on request
     * @default true
     */
    cacheEntities?: boolean

    /**
     * If true, will reset the state on commit
     * @default true
     */
    resetOnCommit?: boolean
}

const StateManagerSymbol = Symbol('StateManager')

export class TypeormDatabase {
    protected statusSchema: string
    protected isolationLevel: IsolationLevel
    protected postponeWriteOperations: boolean
    protected cacheEntities: boolean
    protected resetOnCommit: boolean
    protected con?: DataSource & {
        [StateManagerSymbol]?: StateManager
    }
    protected projectDir: string

    constructor(options?: TypeormDatabaseOptions) {
        this.statusSchema = options?.stateSchema || 'squid_processor'
        this.isolationLevel = options?.isolationLevel || 'SERIALIZABLE'
        this.postponeWriteOperations = options?.postponeWriteOperations ?? true
        this.cacheEntities = options?.cacheEntities ?? true
        this.resetOnCommit = options?.resetOnCommit ?? true
        this.projectDir = options?.projectDir || process.cwd()
    }

    async connect(): Promise<HashAndHeight | undefined> {
        assert(this.con == null, 'already connected')

        let cfg = createOrmConfig({projectDir: this.projectDir})
        this.con = new DataSource(cfg)

        await this.con.initialize()

        try {
            return await this.con.transaction('SERIALIZABLE', (em) => this.initTransaction(em))
        } catch (e: any) {
            await this.con.destroy().catch(() => {}) // ignore error
            this.con = undefined
            throw e
        }
    }

    async disconnect(): Promise<void> {
        await this.con?.destroy().catch(() => {}) // ignore error
        this.con = undefined
    }

    private async initTransaction(em: EntityManager): Promise<HashAndHeight | undefined> {
        let schema = this.escapedSchema()

        await em.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`)
        await em.query(
            `CREATE TABLE IF NOT EXISTS ${schema}.status (id int4 primary key, number int4 not null, hash text DEFAULT '0x', nonce int4 DEFAULT 0)`,
        )
        await em.query(
            // for databases created by prev version of typeorm store
            `ALTER TABLE ${schema}.status ADD COLUMN IF NOT EXISTS hash text DEFAULT '0x'`,
        )
        await em.query(
            // for databases created by prev version of typeorm store
            `ALTER TABLE ${schema}.status ADD COLUMN IF NOT EXISTS nonce int DEFAULT 0`,
        )
        await em.query(`CREATE TABLE IF NOT EXISTS ${schema}.hot_block (number int4 primary key, hash text not null)`)
        await em.query(
            `CREATE TABLE IF NOT EXISTS ${schema}.hot_change_log (block_height int4 not null references ${schema}.hot_block on delete cascade, index int4 not null, change jsonb not null, PRIMARY KEY (block_height, index))`,
        )

        let status: (HashAndHeight & {nonce: number})[] = await em.query(
            `SELECT number, hash, nonce FROM ${schema}.status WHERE id = 0`,
        )
        if (status.length === 0) {
            return undefined
        }

        let top: HashAndHeight[] = await em.query(`SELECT number, hash FROM ${schema}.hot_block ORDER BY number`)

        let state = assertStateInvariants({...status[0], top})
        return state.top.length > 0 ? state.top[state.top.length - 1] : {number: state.number, hash: state.hash}
    }

    private async getState(em: EntityManager): Promise<DatabaseState> {
        let schema = this.escapedSchema()

        let status: (HashAndHeight & {nonce: number})[] = await em.query(
            `SELECT number, hash, nonce FROM ${schema}.status WHERE id = 0`,
        )

        assert(status.length === 1)

        let top: HashAndHeight[] = await em.query(`SELECT hash, number FROM ${schema}.hot_block ORDER BY number`)

        return assertStateInvariants({...status[0], top})
    }

    transact(
        batch: DataBatchMessage<HashAndHeight, unknown>,
        cb: (store: Store, sliceBeg: number, sliceEnd: number) => Promise<void>,
    ): Promise<HashAndHeight> {
        return this.submit(async (em) => {
            let state = await this.getState(em)

            let unfinalizedIndex = 0
            if (batch.finalizedHead) {
                unfinalizedIndex = batch.data.findIndex((b) => b.cursor.number > batch.finalizedHead!.number)
            }

            if (unfinalizedIndex < 0) {
                const finalizedRef = maybeLast(batch.data)?.cursor ?? state

                await this.deleteHotBlocks(em, finalizedRef.number)
                await this.performUpdates((store) => cb(store, 0, batch.data.length), em)
                await this.updateStatus(em, state.nonce, finalizedRef)
            } else {
                if (batch.finalizedHead) {
                    await this.deleteHotBlocks(em, batch.finalizedHead.number)
                }

                if (unfinalizedIndex > 0) {
                    await this.performUpdates((store) => cb(store, 0, unfinalizedIndex), em)
                }

                for (let i = unfinalizedIndex; i < batch.data.length; i++) {
                    let b = batch.data[i].cursor
                    await this.insertHotBlock(em, b)
                    await this.performUpdates(
                        (store) => cb(store, i, i + 1),
                        em,
                        new ChangeTracker(em, this.statusSchema, b.number),
                    )
                }

                await this.updateStatus(em, state.nonce, batch.finalizedHead ?? batch.data[unfinalizedIndex - 1].cursor)
            }

            return batch.cursor
        })
    }

    fork(fork: DataForkMessage<HashAndHeight>): Promise<HashAndHeight> {
        return this.submit(async (em) => {
            let state = await this.getState(em)
            let chain = [state, ...state.top]
            let rollbackPos = findRollbackIndex(chain, fork.cursors)

            for (let i = chain.length - 1; i >= rollbackPos; i--) {
                await rollbackBlock(this.statusSchema, em, chain[i].number)
            }

            return chain[chain.length - 1]
        })
    }

    private deleteHotBlocks(em: EntityManager, finalizedHeight: number): Promise<void> {
        return em.query(`DELETE FROM ${this.escapedSchema()}.hot_block WHERE number <= $1`, [finalizedHeight])
    }

    private insertHotBlock(em: EntityManager, block: HashAndHeight): Promise<void> {
        return em.query(`INSERT INTO ${this.escapedSchema()}.hot_block (number, hash) VALUES ($1, $2)`, [
            block.number,
            block.hash,
        ])
    }

    private async updateStatus(em: EntityManager, nonce: number, next: HashAndHeight): Promise<void> {
        let schema = this.escapedSchema()

        let result: [data: any[], rowsChanged: number] = await em.query(
            `UPDATE ${schema}.status SET number = $1, hash = $2, nonce = nonce + 1 WHERE id = 0 AND nonce = $3`,
            [next.number, next.hash, nonce],
        )

        let rowsChanged = result[1]

        // Will never happen if isolation level is SERIALIZABLE or REPEATABLE_READ,
        // but occasionally people use multiprocessor setups and READ_COMMITTED.
        assert(rowsChanged === 1, RACE_MSG)
    }

    private async performUpdates(
        cb: (store: Store) => Promise<void>,
        em: EntityManager,
        changeWriter?: ChangeTracker,
    ): Promise<void> {
        let store = new Store({
            em,
            state: this.getStateManager(),
            logger: this.getLogger().child('store'),
            changes: changeWriter,
            postponeWriteOperations: this.postponeWriteOperations,
            cacheEntities: this.cacheEntities,
        })

        try {
            await cb(store)

            if (this.resetOnCommit) {
                await store.flush()
            } else {
                await store.sync()
            }
        } finally {
            Object.defineProperty(store, 'isClosed', {value: true})
        }
    }

    private async submit<T>(tx: (em: EntityManager) => Promise<T>): Promise<T> {
        let retries = 3
        while (true) {
            try {
                let con = this.con
                assert(con != null, 'not connected')
                return await con.transaction(this.isolationLevel, tx)
            } catch (e: any) {
                if (e.code === '40001' && retries) {
                    retries -= 1
                } else {
                    throw e
                }
            }
        }
    }

    private escapedSchema(): string {
        let con = assertNotNull(this.con)
        return con.driver.escape(this.statusSchema)
    }

    @def
    private getLogger() {
        return createLogger('sqd:db')
    }

    private getStateManager() {
        let connection = assertNotNull(this.con)
        let stateManager = connection[StateManagerSymbol]
        if (stateManager == null) {
            stateManager = new StateManager({
                connection,
                logger: this.getLogger().child('state'),
            })
            connection[StateManagerSymbol] = stateManager
        }

        return stateManager
    }
}

const RACE_MSG = 'status table was updated by foreign process, make sure no other processor is running'

function assertStateInvariants(state: DatabaseState): DatabaseState {
    let number = state.number

    // Sanity check. Who knows what driver will return?
    assert(Number.isSafeInteger(number))

    assertChainContinuity(state, state.top)

    return state
}

function assertChainContinuity(base: HashAndHeight, chain: HashAndHeight[]) {
    let prev = base
    for (let b of chain) {
        assert(b.number > prev.number, 'blocks must form a continues chain')
        prev = b
    }
}

export function createTypeormTarget<TValue>(
    databaseOpts: TypeormDatabaseOptions,
    handler: (store: Store, batch: TValue[]) => Promise<void>,
) {
    return createTarget<HashAndHeight, TValue, never, Promise<void>>({
        unfinalized: true,
        write: async ({cursorUtils, read}) => {
            let db = new TypeormDatabase(databaseOpts)

            const cursor = await db.connect()

            async function process(cursor: HashAndHeight | undefined) {
                for await (const message of read({cursor})) {
                    switch (message.type) {
                        case 'batch': {
                            await db.transact(message, (store, sliceBeg, sliceEnd) =>
                                handler(
                                    store,
                                    message.data.slice(sliceBeg, sliceEnd).map((d) => d.value),
                                ),
                            )
                            break
                        }
                        case 'fork': {
                            const cursor = await db.fork(message)
                            return process(cursor)
                        }
                    }
                }
            }

            await process(cursor)

            await db.disconnect()
        },
    })
}

function findRollbackIndex(chainA: HashAndHeight[], chainB: HashAndHeight[]) {
    let aIndex = 0
    let bIndex = 0
    let lastCommonIndex = -1

    while (aIndex < chainA.length && bIndex < chainB.length) {
        const blockA = chainA[aIndex]
        const blockB = chainB[bIndex]

        if (blockA.number < blockB.number) {
            aIndex++
            continue
        }

        if (blockA.number > blockB.number) {
            bIndex++
            continue
        }

        if (blockA.number === blockB.number && blockA.hash !== blockB.hash) {
            return lastCommonIndex
        }

        lastCommonIndex = aIndex
        aIndex++
        bIndex++
    }

    return lastCommonIndex
}
