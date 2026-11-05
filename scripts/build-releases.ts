import { mkdir, copyFile, writeFile, readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { version } from '../package.json';

const targets = [
  'darwin-arm64',
  'darwin-x64',
  'linux-arm64',
  'linux-x64',
  'linux-arm64-musl',
  'linux-x64-musl',
  'windows-x64',
] as const;
await mkdir('dist/releases', { recursive: true });
for (const target of targets) {
  const filename = `pgstack-${target}${target.startsWith('windows') ? '.exe' : ''}`;
  const result = Bun.spawnSync(
    [
      'bun',
      'build',
      'src/index.ts',
      '--compile',
      '--define',
      'process.env.API_URL=undefined',
      `--target=bun-${target}`,
      '--outfile',
      `dist/releases/${filename}`,
    ],
    { stdout: 'inherit', stderr: 'inherit' },
  );
  if (result.exitCode !== 0) throw new Error(`Build failed for ${target}`);
  const checksum = createHash('sha256')
    .update(await readFile(`dist/releases/${filename}`))
    .digest('hex');
  await writeFile(`dist/releases/${filename}.sha256`, `${checksum}  ${filename}\n`);
}
await copyFile('LICENSE', 'dist/releases/LICENSE');
await writeFile('dist/releases/version', `${version}\n`);
