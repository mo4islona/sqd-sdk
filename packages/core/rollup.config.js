import {globSync} from 'glob'
import typescript from '@rollup/plugin-typescript'
import {defineConfig} from 'rollup'

const inputs = globSync('src/**/*.ts')

export default defineConfig([
    {
        input: inputs,
        output: [
            {
                dir: 'lib.new',
                format: 'esm',
                sourcemap: true,
                preserveModules: true,
                preserveModulesRoot: 'src',
                entryFileNames: ({name}) => `${name}.js`,
                interop: 'auto',
            },
            {
                dir: 'lib.new',
                format: 'cjs',
                sourcemap: true,
                preserveModules: true,
                preserveModulesRoot: 'src',
                entryFileNames: ({name}) => `${name}.cjs`,
                interop: 'auto',
            },
        ],
        plugins: [
            typescript({
                tsconfig: 'tsconfig.build.json',
                compilerOptions: {
                    declaration: true,
                    noEmit: false,
                    outDir: 'lib.new',
                    sourceMap: true,
                    importHelpers: true,
                },
                noEmitOnError: true,
            }),
        ],
    },
])
