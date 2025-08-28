#!/usr/bin/env -S pnpm tsx
import 'zx/globals'
import path from 'node:path'
import {existsSync} from 'node:fs'
import {parse, print, visit, types} from 'recast'
import parser from 'recast/parsers/typescript'

const EXTENSIONS = {
    esm: '.js',
    cjs: '.cjs',
    dts: '.d.ts',
    dcts: '.d.cts',
} as const

const GLOB_PATTERNS = {
    cjsFiles: '**/*.cjs',
    esmFiles: '**/*.js',
    dtsFiles: '**/*.d.ts',
    dctsFiles: '**/*.d.cts',
    srcFiles: 'src/**/*.ts',
} as const

// Types
interface PackageJson {
    name?: string
    main?: string
    module?: string
    types?: string
    exports?: PackageExports
    [key: string]: any
}

interface PackageExports {
    [key: string]: {
        import: {default: string; types: string}
        require: {default: string; types: string}
    }
}

interface InlineImportOccurrence {
    start: number
    end: number
    name: string
    mod: string
}

interface FileGlobs {
    cjsFiles: string[]
    esmFiles: string[]
    dtsFiles: string[]
    dctsFiles: string[]
    srcFiles: string[]
}

// Utility functions
function validatePackageDirectory(pkgDir: string): void {
    const pkgPath = path.join(pkgDir, 'package.json')
    const rollupConfig = path.join(pkgDir, 'rollup.config.js')

    if (!existsSync(pkgPath)) {
        throw new Error(`package.json not found in ${pkgDir}`)
    }
    if (!existsSync(rollupConfig)) {
        throw new Error(`rollup.config.js not found in ${pkgDir}`)
    }
}

function resolvePathAlias(importPath: string, file: string, pkgDir: string): string {
    if (importPath.startsWith('~/')) {
        const relativePath = path.relative(path.dirname(file), path.resolve(pkgDir, 'lib.new', importPath.slice(2)))
        return relativePath.startsWith('.') ? relativePath : `./${relativePath}`
    }
    return importPath
}

function fixImportPath(importPath: string, file: string, ext: string, pkgDir: string): string {
    const resolved = resolvePathAlias(importPath, file, pkgDir)
    if (!resolved.startsWith('.')) return resolved

    const regex = /\.(?:js|ts|cjs|mjs|mts|cts|jsx|tsx)$/
    if (regex.test(resolved)) {
        return resolved.replace(regex, ext)
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

async function getAllFiles(pkgDir: string): Promise<FileGlobs> {
    const libNewDir = path.join(pkgDir, 'lib.new')

    const [cjsFiles, esmFiles, dtsFiles, dctsFiles, srcFiles] = await Promise.all([
        glob(path.join(libNewDir, GLOB_PATTERNS.cjsFiles)),
        glob(path.join(libNewDir, GLOB_PATTERNS.esmFiles)),
        glob(path.join(libNewDir, GLOB_PATTERNS.dtsFiles)),
        glob(path.join(libNewDir, GLOB_PATTERNS.dctsFiles)),
        glob(path.join(pkgDir, GLOB_PATTERNS.srcFiles)),
    ])

    return {cjsFiles, esmFiles, dtsFiles, dctsFiles, srcFiles}
}

function createImportVisitor(file: string, ext: string, pkgDir: string) {
    return {
        visitImportDeclaration(this: any, p: any) {
            p.value.source.value = fixImportPath(p.value.source.value, file, ext, pkgDir)
            this.traverse(p)
        },
        visitExportAllDeclaration(this: any, p: any) {
            p.value.source.value = fixImportPath(p.value.source.value, file, ext, pkgDir)
            this.traverse(p)
        },
        visitExportNamedDeclaration(this: any, p: any) {
            if (p.value.source) {
                p.value.source.value = fixImportPath(p.value.source.value, file, ext, pkgDir)
            }
            this.traverse(p)
        },
        visitCallExpression(this: any, p: any) {
            if (p.value.callee.type === 'Identifier' && p.value.callee.name === 'require') {
                p.value.arguments[0].value = fixImportPath(p.value.arguments[0].value, file, ext, pkgDir)
            }
            this.traverse(p)
        },
        visitTSImportType(this: any, p: any) {
            p.value.argument.value = fixImportPath(p.value.argument.value, file, ext, pkgDir)
            this.traverse(p)
        },
        visitAwaitExpression(this: any, p: any) {
            if (/await import\("\.\//.test(print(p.value).code)) {
                p.value.argument.arguments[0].value = fixImportPath(
                    p.value.argument.arguments[0].value,
                    file,
                    ext,
                    pkgDir,
                )
            }
            this.traverse(p)
        },
    }
}

async function processCodeFiles(files: string[], ext: string, pkgDir: string): Promise<void> {
    await Promise.all(
        files.map(async (file) => {
            try {
                const code = parse(await fs.readFile(file, 'utf8'), {parser})
                visit(code, createImportVisitor(file, ext, pkgDir))
                await fs.writeFile(file, print(code).code)
            } catch (error) {
                console.error(`❌ Error processing file ${file}`)
                throw error
            }
        }),
    )
}

async function fixImports(pkgDir: string, fileGlobs: FileGlobs): Promise<void> {
    // Process CJS and ESM files in parallel
    await Promise.all([
        processCodeFiles(fileGlobs.cjsFiles, EXTENSIONS.cjs, pkgDir),
        processCodeFiles(fileGlobs.esmFiles, EXTENSIONS.esm, pkgDir),
    ])

    // Process .d.ts files - hoist inline import types
    await processDtsFiles(fileGlobs.dtsFiles, pkgDir)
}

function collectInlineImports(content: string): {
    occs: InlineImportOccurrence[]
    moduleToNames: Map<string, Set<string>>
} {
    const moduleToNames = new Map<string, Set<string>>()
    const occs: InlineImportOccurrence[] = []

    // Reset regex lastIndex to avoid issues with global regex
    const regex = /import\(["'](.+?)["']\)\.([A-Za-z_$][\w$]*)/g
    let match: RegExpExecArray | null

    match = regex.exec(content)
    while (match !== null) {
        const mod = match[1]
        const name = match[2]
        const start = match.index
        const end = match.index + match[0].length
        const before = content.slice(Math.max(0, start - 12), start)

        // Skip typeof import("...").X occurrences
        if (/typeof\s*$/.test(before)) {
            match = regex.exec(content)
            continue
        }

        occs.push({start, end, name, mod})
        if (!moduleToNames.has(mod)) moduleToNames.set(mod, new Set())
        moduleToNames.get(mod)!.add(name)

        match = regex.exec(content)
    }

    return {occs, moduleToNames}
}

function parseExistingImports(content: string): Map<string, Set<string>> {
    const existing = new Map<string, Set<string>>()
    const regex = /\n\s*import\s+(?:type\s+)?\{([^}]+)\}\s+from\s+["'](.+?)["'];?/g
    let match: RegExpExecArray | null

    match = regex.exec(content)
    while (match !== null) {
        const names = match[1]
            .split(',')
            .map((s) => s.trim().split(/\s+as\s+/)[0])
            .filter(Boolean)
        const mod = match[2]

        if (!existing.has(mod)) existing.set(mod, new Set())
        const set = existing.get(mod)!
        for (const name of names) set.add(name)

        match = regex.exec(content)
    }

    return existing
}

function generateMissingImports(
    moduleToNames: Map<string, Set<string>>,
    existing: Map<string, Set<string>>,
    file: string,
    pkgDir: string,
): {content: string; importLines: string[]} {
    const importLines: string[] = []
    const escapeRegExp = (s: string) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
    let content = ''

    for (const [mod, names] of moduleToNames) {
        const already = existing.get(mod) || new Set<string>()
        const missing = Array.from(names)
            .filter((n) => !already.has(n))
            .sort()
        if (!missing.length) continue

        const target = fixImportPath(mod, file, EXTENSIONS.esm, pkgDir)
        importLines.push(`import type { ${missing.join(', ')} } from "${target}";`)
    }

    return {content, importLines}
}

async function processDtsFiles(dtsFiles: string[], pkgDir: string): Promise<void> {
    await Promise.all(
        dtsFiles.map(async (file) => {
            try {
                let content = await fs.readFile(file, 'utf8')

                // Fix relative specifiers to .js
                content = content.replace(/(from\s+["'])(\.\.?\/[^"']+?)(["'])/g, (m, p1, spec, p3) => {
                    const fixed = fixImportPath(spec, file, EXTENSIONS.esm, pkgDir)
                    return `${p1}${fixed}${p3}`
                })

                // Collect and replace inline import types
                const {occs, moduleToNames} = collectInlineImports(content)

                if (occs.length) {
                    // Replace occurrences from end to start to maintain indices
                    occs.sort((a, b) => b.start - a.start)
                    for (const o of occs) {
                        content = content.slice(0, o.start) + o.name + content.slice(o.end)
                    }

                    // Parse existing imports and generate missing ones
                    const existing = parseExistingImports(content)
                    const {importLines} = generateMissingImports(moduleToNames, existing, file, pkgDir)

                    if (importLines.length) {
                        const lines = content.split(/\r?\n/)
                        let lastImportIdx = -1

                        for (let i = 0; i < lines.length; i++) {
                            if (lines[i].trimStart().startsWith('import ')) {
                                lastImportIdx = i
                            }
                        }

                        const block = importLines.join('\n')
                        if (lastImportIdx >= 0) {
                            lines.splice(lastImportIdx + 1, 0, block)
                        } else {
                            lines.unshift(block, '')
                        }
                        content = lines.join('\n')
                    }
                }

                await fs.writeFile(file, content)
            } catch (error) {
                console.error(`❌ Error processing .d.ts file ${file}`)
                throw error
            }
        }),
    )
}

function createExportEntry(entry: string): {
    import: {default: string; types: string}
    require: {default: string; types: string}
} {
    return {
        import: {default: `./${entry}.js`, types: `./${entry}.d.ts`},
        require: {default: `./${entry}.cjs`, types: `./${entry}.d.cts`},
    }
}

function generateExportsKey(entry: string): string {
    return entry === 'index' ? '.' : `./${entry.replace(/\/index$/, '')}`
}

async function updateExports(pkgDir: string, pkg: PackageJson, srcFiles: string[]): Promise<void> {
    pkg.main = './index.cjs'
    pkg.module = './index.js'
    pkg.types = './index.d.ts'

    const exports: PackageExports = {}

    for (const rawEntry of srcFiles.sort((a, b) => a.localeCompare(b))) {
        const match = rawEntry.match(/src\/(.*)\.ts/)
        if (!match) continue

        const entry = match[1]
        const exportsKey = generateExportsKey(entry)
        exports[exportsKey] = createExportEntry(entry)
    }

    pkg.exports = exports
}

async function createDtsStubs(dtsFiles: string[]): Promise<void> {
    await Promise.all(
        dtsFiles.map(async (file) => {
            try {
                const src = await fs.readFile(file, 'utf8')
                const ctsPath = file.replace(/\.d\.ts$/, EXTENSIONS.dcts)
                const base = path.basename(file)
                const relativeJs = `./${base.replace(/\.d\.ts$/, EXTENSIONS.esm)}`
                const hasDefault = /\bexport\s+default\b|\bexport\s*\{\s*default\b/.test(src)

                const lines = [
                    `export * from '${relativeJs}';`,
                    hasDefault ? `export { default } from '${relativeJs}';` : '',
                ].filter(Boolean)

                await fs.writeFile(ctsPath, lines.join('\n'))
            } catch (error) {
                console.error(`❌ Error creating .d.cts stub for ${file}`)
                throw error
            }
        }),
    )
}

async function cleanupTypeExports(dctsFiles: string[]): Promise<void> {
    await Promise.all(
        dctsFiles.map(async (file) => {
            try {
                const content = await fs.readFile(file, 'utf8')
                const cleaned = content
                    .split(/\r?\n/)
                    .filter((line) => !/^\s*export\s+type\s+\*/.test(line))
                    .join('\n')

                if (cleaned !== content) {
                    await fs.writeFile(file, cleaned)
                }
            } catch (error) {
                console.error(`❌ Error cleaning up type exports in ${file}`)
                throw error
            }
        }),
    )
}

function createLibPrefixedPackage(pkg: PackageJson): PackageJson {
    const result: PackageJson = {
        ...pkg,
        main: './lib/index.cjs',
        module: './lib/index.js',
        types: './lib/index.d.ts',
    }

    if (pkg.exports && typeof pkg.exports === 'object') {
        const newExports: PackageExports = {}

        for (const [key, exp] of Object.entries(pkg.exports)) {
            if (exp && typeof exp === 'object') {
                const prefixPath = (p?: string): string | undefined => (p?.startsWith('./') ? `./lib/${p.slice(2)}` : p)

                newExports[key] = {
                    import: {
                        default: prefixPath(exp.import?.default) || '',
                        types: prefixPath(exp.import?.types) || '',
                    },
                    require: {
                        default: prefixPath(exp.require?.default) || '',
                        types: prefixPath(exp.require?.types) || '',
                    },
                }
            }
        }

        result.exports = newExports
    }

    return result
}

async function buildPackage(pkgDir: string): Promise<void> {
    try {
        // Validate package directory structure
        validatePackageDirectory(pkgDir)

        const pkgPath = path.join(pkgDir, 'package.json')
        const pkg: PackageJson = await fs.readJSON(pkgPath)

        // Clean and build
        await fs.remove(path.join(pkgDir, 'lib.new'))
        try {
            await $`rollup -c ${path.join(pkgDir, 'rollup.config.js')}`
        } catch (error) {
            console.error('❌ Rollup build failed')
            throw new Error('Build failed')
        }

        // Get all file globs in one batch
        const fileGlobs = await getAllFiles(pkgDir)

        // Fix imports for all generated files
        await fixImports(pkgDir, fileGlobs)

        // Update package exports based on source files
        await updateExports(pkgDir, pkg, fileGlobs.srcFiles)

        // Create .d.cts stubs and clean up type exports
        await Promise.all([createDtsStubs(fileGlobs.dtsFiles), cleanupTypeExports(fileGlobs.dctsFiles)])

        // Write package.json files
        await fs.writeJSON(path.join(pkgDir, 'lib.new/package.json'), pkg, {spaces: 4})

        const pkgForRoot = createLibPrefixedPackage(pkg)
        await fs.writeJSON(pkgPath, pkgForRoot, {spaces: 4})

        // Final file system operations
        await fs.remove(path.join(pkgDir, 'lib'))
        await fs.rename(path.join(pkgDir, 'lib.new'), path.join(pkgDir, 'lib'))
    } catch (error) {
        console.error(`❌ Build failed for package in ${pkgDir}`)
        throw error
    }
}

const pkgDir = process.cwd()

try {
    await buildPackage(pkgDir)
    console.log(`✅ Successfully built package in ${pkgDir}`)
} catch (error) {
    process.exit(1)
}
