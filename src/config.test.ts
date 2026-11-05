import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtemp, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { configPath, readProfile, saveProfile } from './config';

let directory: string | undefined;
const originalDirectory = process.env.PGSTACK_CONFIG_DIR;
afterEach(async () => {
  if (directory) await rm(directory, { recursive: true, force: true });
  if (originalDirectory === undefined) delete process.env.PGSTACK_CONFIG_DIR;
  else process.env.PGSTACK_CONFIG_DIR = originalDirectory;
});

describe('credential storage', () => {
  test('isolates credentials by API origin and writes owner-only files', async () => {
    directory = await mkdtemp(join(tmpdir(), 'pgstack-cli-'));
    process.env.PGSTACK_CONFIG_DIR = directory;
    const profile = { token: 'example-token', expiresAt: '2030-01-01T00:00:00.000Z' };
    expect(await readProfile('https://api.example.com')).toBeUndefined();
    await saveProfile('https://api.example.com', profile);
    expect(await readProfile('https://api.example.com')).toEqual(profile);
    expect(await readProfile('https://other.example.com')).toBeUndefined();
    expect((await stat(configPath())).mode & 0o777).toBe(0o600);
    expect((await stat(directory)).mode & 0o777).toBe(0o700);
    await saveProfile('https://api.example.com');
    expect(await readProfile('https://api.example.com')).toBeUndefined();
  });
});
