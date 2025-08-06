async function main() {
    try {
        for await (const i of {
            [Symbol.asyncIterator]() {
                return {
                    next: async (): Promise<{done: boolean; value: number}> => {
                        throw new Error('test')
                    },
                    return: async (): Promise<{value: undefined; done: true}> => {
                        console.log('return')
                        return {value: undefined, done: true}
                    },
                }
            },
        }) {
        }
    } catch (err) {
        console.log(err)

        while (true) {
            await new Promise((resolve) => setTimeout(resolve, 1000))
        }
    }
}

main()
