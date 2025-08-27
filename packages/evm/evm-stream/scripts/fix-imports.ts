#!/usr/bin/env -S pnpm tsx
import 'zx/globals'

import path from 'node:path'
import {existsSync} from 'node:fs'
import {parse, print, visit} from 'recast'
import parser from 'recast/parsers/typescript'

function resolvePathAlias(importPath: string, file: string) {
    if (importPath.startsWith('~/')) {
        const relativePath = path.relative(path.dirname(file), path.resolve('dist.new', importPath.slice(2)))
        importPath = relativePath.startsWith('.') ? relativePath : `./${relativePath}`
    }

    return importPath
}

function fixImportPath(importPath: string, file: string, ext: string) {
    const resolved = resolvePathAlias(importPath, file)

    // Only adjust local imports
    if (!resolved.startsWith('.')) {
        return resolved
    }

    // If it already has an extension, normalize it to the target one
    if (/\.(?:js|ts|cjs|mjs|mts|cts|jsx|tsx)$/.test(resolved)) {
        return resolved.replace(/\.(?:js|ts|cjs|mjs|mts|cts|jsx|tsx)$/, ext)
    }

    // Otherwise, try resolving to a file or an index file
    const importerDir = path.dirname(file)
    const absoluteBase = path.resolve(importerDir, resolved)

    if (existsSync(absoluteBase + ext)) {
        return `${resolved}${ext}`
    }

    if (existsSync(path.join(absoluteBase, `index${ext}`))) {
        const base = resolved.endsWith('/') ? resolved.slice(0, -1) : resolved
        return `${base}/index${ext}`
    }

    return resolved
}

const cjsFiles = await glob('dist.new/**/*.{cjs,d.cts}')

await Promise.all(
    cjsFiles.map(async (file) => {
        const code = parse(await fs.readFile(file, 'utf8'), {parser})

        visit(code, {
            visitImportDeclaration(path) {
                path.value.source.value = fixImportPath(path.value.source.value, file, '.cjs')
                this.traverse(path)
            },
            visitExportAllDeclaration(path) {
                path.value.source.value = fixImportPath(path.value.source.value, file, '.cjs')
                this.traverse(path)
            },
            visitExportNamedDeclaration(path) {
                if (path.value.source) {
                    path.value.source.value = fixImportPath(path.value.source.value, file, '.cjs')
                }
                this.traverse(path)
            },
            visitCallExpression(path) {
                if (path.value.callee.type === 'Identifier' && path.value.callee.name === 'require') {
                    path.value.arguments[0].value = fixImportPath(path.value.arguments[0].value, file, '.cjs')
                }
                this.traverse(path)
            },
            visitTSImportType(path) {
                path.value.argument.value = resolvePathAlias(path.value.argument.value, file)
                this.traverse(path)
            },
            visitAwaitExpression(path) {
                if (print(path.value).code.startsWith(`await import("./`)) {
                    path.value.argument.arguments[0].value = fixImportPath(
                        path.value.argument.arguments[0].value,
                        file,
                        '.cjs',
                    )
                }
                this.traverse(path)
            },
        })

        await fs.writeFile(file, print(code).code)
    }),
)

const esmFiles = await glob('dist.new/**/*.{js,d.ts}')

await Promise.all(
    esmFiles.map(async (file) => {
        const code = parse(await fs.readFile(file, 'utf8'), {parser})

        visit(code, {
            visitImportDeclaration(path) {
                path.value.source.value = fixImportPath(path.value.source.value, file, '.js')
                this.traverse(path)
            },
            visitExportAllDeclaration(path) {
                path.value.source.value = fixImportPath(path.value.source.value, file, '.js')
                this.traverse(path)
            },
            visitExportNamedDeclaration(path) {
                if (path.value.source) {
                    path.value.source.value = fixImportPath(path.value.source.value, file, '.js')
                }
                this.traverse(path)
            },
            visitTSImportType(path) {
                path.value.argument.value = fixImportPath(path.value.argument.value, file, '.js')
                this.traverse(path)
            },
            visitAwaitExpression(path) {
                if (print(path.value).code.startsWith(`await import("./`)) {
                    path.value.argument.arguments[0].value = fixImportPath(
                        path.value.argument.arguments[0].value,
                        file,
                        '.js',
                    )
                }
                this.traverse(path)
            },
        })

        await fs.writeFile(file, print(code).code)
    }),
)
