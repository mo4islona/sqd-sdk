async function main() {
    const generator = async function* () {
        yield 1
        yield 2
        yield 3
        throw new Error('test')
    }

    const stream = generator()

    while (true) {
        await stream.next().then(
            (r) => {
                console.log('next', r)
                if (r.done) {
                    process.exit(0)
                }
            },
            (err) => {
                console.log('err', err)
            }
        )
    }
}

main()
