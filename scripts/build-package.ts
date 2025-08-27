#!/usr/bin/env -S pnpm tsx
import 'zx/globals'
import path from 'node:path'
import {existsSync} from 'node:fs'
import {parse, print, visit} from 'recast'
import parser from 'recast/parsers/typescript'

async function fixImports(pkgDir: string) {
    function resolvePathAlias(importPath: string, file: string) {
        if (importPath.startsWith('~/')) {
            const relativePath = path.relative(path.dirname(file), path.resolve(pkgDir, 'lib.new', importPath.slice(2)))
            importPath = relativePath.startsWith('.') ? relativePath : `./${relativePath}`
        }
        return importPath
    }

    function fixImportPath(importPath: string, file: string, ext: string) {
        const resolved = resolvePathAlias(importPath, file)
        if (!resolved.startsWith('.')) return resolved
        if (/\.(?:js|ts|cjs|mjs|mts|cts|jsx|tsx)$/.test(resolved)) {
            return resolved.replace(/\.(?:js|ts|cjs|mjs|mts|cts|jsx|tsx)$/, ext)
        }
        const importerDir = path.dirname(file)
        const absoluteBase = path.resolve(importerDir, resolved)
        if (existsSync(absoluteBase + ext)) return `${resolved}${ext}`
        if (existsSync(path.join(absoluteBase, `index${ext}`))) {
            const base = resolved.endsWith('/') ? resolved.slice(0, -1) : resolved
            return `${base}/index${ext}`
        }
        return resolved
    }

    const cjsFiles = await glob(path.join(pkgDir, 'lib.new/**/*.{cjs,d.cts}'))
    await Promise.all(
        cjsFiles.map(async (file) => {
            const code = parse(await fs.readFile(file, 'utf8'), {parser})
            visit(code, {
                visitImportDeclaration(p) {
                    p.value.source.value = fixImportPath(p.value.source.value, file, '.cjs')
                    this.traverse(p)
                },
                visitExportAllDeclaration(p) {
                    p.value.source.value = fixImportPath(p.value.source.value, file, '.cjs')
                    this.traverse(p)
                },
                visitExportNamedDeclaration(p) {
                    if (p.value.source) p.value.source.value = fixImportPath(p.value.source.value, file, '.cjs')
                    this.traverse(p)
                },
                visitCallExpression(p) {
                    if (p.value.callee.type === 'Identifier' && p.value.callee.name === 'require') {
                        p.value.arguments[0].value = fixImportPath(p.value.arguments[0].value, file, '.cjs')
                    }
                    this.traverse(p)
                },
                visitTSImportType(p) {
                    p.value.argument.value = fixImportPath(p.value.argument.value, file, '.cjs')
                    this.traverse(p)
                },
                visitAwaitExpression(p) {
                    if (print(p.value).code.startsWith(`await import("./`)) {
                        p.value.argument.arguments[0].value = fixImportPath(
                            p.value.argument.arguments[0].value,
                            file,
                            '.cjs',
                        )
                    }
                    this.traverse(p)
                },
            })
            await fs.writeFile(file, print(code).code)
        }),
    )

    const esmFiles = await glob(path.join(pkgDir, 'lib.new/**/*.{js,d.ts}'))
    await Promise.all(
        esmFiles.map(async (file) => {
            const code = parse(await fs.readFile(file, 'utf8'), {parser})
            visit(code, {
                visitImportDeclaration(p) {
                    p.value.source.value = fixImportPath(p.value.source.value, file, '.js')
                    this.traverse(p)
                },
                visitExportAllDeclaration(p) {
                    p.value.source.value = fixImportPath(p.value.source.value, file, '.js')
                    this.traverse(p)
                },
                visitExportNamedDeclaration(p) {
                    if (p.value.source) p.value.source.value = fixImportPath(p.value.source.value, file, '.js')
                    this.traverse(p)
                },
                visitTSImportType(p) {
                    p.value.argument.value = fixImportPath(p.value.argument.value, file, '.js')
                    this.traverse(p)
                },
                visitAwaitExpression(p) {
                    if (print(p.value).code.startsWith(`await import("./`)) {
                        p.value.argument.arguments[0].value = fixImportPath(
                            p.value.argument.arguments[0].value,
                            file,
                            '.js',
                        )
                    }
                    this.traverse(p)
                },
            })
            await fs.writeFile(file, print(code).code)
        }),
    )
}

async function updateExports(pkgDir: string, pkg: any) {
    pkg.main = './index.cjs'
    pkg.module = './index.js'
    pkg.types = './index.d.ts'
    const entries = await glob(path.join(pkgDir, 'src/**/*.ts'))
    pkg.exports = entries.reduce((acc: any, rawEntry: string) => {
        const entry = rawEntry.match(/src\/(.*)\.ts/)![1]!
        const exportsEntry = entry === 'index' ? '.' : `./${entry.replace(/\/index$/, '')}`
        const importEntry = `./${entry}.js`
        const requireEntry = `./${entry}.cjs`
        acc[exportsEntry] = {
            import: {default: importEntry, types: `./${entry}.d.ts`},
            require: {default: requireEntry, types: `./${entry}.d.cts`},
        }
        return acc
    }, {})
}

async function buildPackage(pkgDir: string) {
    const pkgPath = path.join(pkgDir, 'package.json')
    const pkg = await fs.readJSON(pkgPath)

    await fs.remove(path.join(pkgDir, 'lib.new'))
    await $`rollup -c ${path.join(pkgDir, 'rollup.config.js')}`.stdio('pipe', 'pipe', 'pipe')

    await fixImports(pkgDir)

    await updateExports(pkgDir, pkg)

    // Duplicate .d.ts to .d.cts for CJS type resolution
    const dtsFiles = await glob(path.join(pkgDir, 'lib.new/**/*.d.ts'))
    await Promise.all(
        dtsFiles.map(async (file) => {
            const content = await fs.readFile(file, 'utf8')
            const ctsPath = file.replace(/\.d\.ts$/, '.d.cts')
            await fs.writeFile(ctsPath, content)
        }),
    )

    // Rewrite import paths inside .d.cts to .cjs extensions
    const dctsFiles = await glob(path.join(pkgDir, 'lib.new/**/*.d.cts'))
    await Promise.all(
        dctsFiles.map(async (file) => {
            const code = parse(await fs.readFile(file, 'utf8'), {parser})
            visit(code, {
                visitImportDeclaration(p) {
                    p.value.source.value = p.value.source.value && p.value.source.value[0] === '.'
                        ? p.value.source.value.replace(/\.(?:js|mjs)$/, '.cjs')
                        : p.value.source.value
                    this.traverse(p)
                },
                visitExportAllDeclaration(p) {
                    p.value.source.value = p.value.source.value && p.value.source.value[0] === '.'
                        ? p.value.source.value.replace(/\.(?:js|mjs)$/, '.cjs')
                        : p.value.source.value
                    this.traverse(p)
                },
                visitExportNamedDeclaration(p) {
                    if (p.value.source) {
                        p.value.source.value = p.value.source.value && p.value.source.value[0] === '.'
                            ? p.value.source.value.replace(/\.(?:js|mjs)$/, '.cjs')
                            : p.value.source.value
                    }
                    this.traverse(p)
                },
                visitTSImportType(p) {
                    const v = p.value.argument && (p.value.argument as any).value
                    if (typeof v === 'string' && v.startsWith('.')) {
                        ;(p.value.argument as any).value = v.replace(/\.(?:js|mjs)$/, '.cjs')
                    }
                    this.traverse(p)
                },
            })
            await fs.writeFile(file, print(code).code)
        }),
    )

    // Write exports into lib package.json
    await fs.writeJSON(path.join(pkgDir, 'lib.new/package.json'), pkg, {spaces: 4})

    // If core package, also mirror exports into root manifest with ./lib/* prefixes
    const pkgForRoot = JSON.parse(JSON.stringify(pkg))
    pkgForRoot.main = './lib/index.cjs'
    pkgForRoot.module = './lib/index.js'
    pkgForRoot.types = './lib/index.d.ts'
    if (pkgForRoot.exports && typeof pkgForRoot.exports === 'object') {
        for (const key of Object.keys(pkgForRoot.exports)) {
            const exp = pkgForRoot.exports[key]
            if (exp && typeof exp === 'object') {
                const prefix = (p: string) => (p?.startsWith('./') ? `./lib/${p.slice(2)}` : p)
                if (typeof exp.import?.default === 'string') exp.import.default = prefix(exp.import.default)
                if (typeof exp.import?.types === 'string') exp.import.types = prefix(exp.import.types)
                if (typeof exp.require?.default === 'string') exp.require.default = prefix(exp.require.default)
                if (typeof exp.require?.types === 'string') exp.require.types = prefix(exp.require.types)
            }
        }
    }
    await fs.writeJSON(pkgPath, pkgForRoot, {spaces: 4})

    await fs.remove(path.join(pkgDir, 'lib'))
    await fs.rename(path.join(pkgDir, 'lib.new'), path.join(pkgDir, 'lib'))
}

const pkgDir = process.cwd()
await buildPackage(pkgDir)
