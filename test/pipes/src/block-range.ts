export function parseBlockFormatting(block: string | number) {
    if (typeof block === 'number') return block
    /**
     * Remove commas and underscores
     * 1_000_000 -> 1000000
     * 1,000,000 -> 1000000
     */
    const value = Number(block.replace(/[_,]/g, ''))
    if (Number.isNaN(value)) {
        throw new Error(
            `Can't parse a block number from string "${block}". Valid examples: "1000000", "1_000_000", "1,000,000"`,
        )
    }

    return value
}

function parseBlock(block: string | number, offset?: number) {
    if (typeof block === 'number') return block

    if (block.startsWith('+') && offset) {
        return offset + parseBlockFormatting(block.substring(1))
    }

    return parseBlockFormatting(block)
}

export function parseRange(opts: { range?: BlockRange; defaultFrom?: number | string }): {
    from: number
    to?: number
} {
    const defaultFrom = parseBlock(opts.defaultFrom || '0')

    if (!opts.range) {
        return { from: defaultFrom }
    }

    const from = parseBlock(opts.range.from || '0')
    const to = opts.range.to ? parseBlock(opts.range.to, from) : undefined

    return { from, to }
}

export type BlockRange = { from?: number | string; to?: number | string }
