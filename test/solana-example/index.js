const { PortalClient } = require('@belopash/core/portal-client')
const { HttpClient } = require('@belopash/core/http-client')

const portal = new PortalClient({
    url: 'https://portal.sqd.dev/datasets/solana-beta',
    http: new HttpClient({
        retryAttempts: Number.POSITIVE_INFINITY,
    }),
    minBytes: 100 * 1024 * 1024,
})

portal.getHead().then((h) => {
    console.log(h)
})