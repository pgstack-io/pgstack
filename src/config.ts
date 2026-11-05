import { randomUUID } from 'node:crypto';
import { chmod, mkdir, readFile, rename, rm, writeFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { dirname, join } from 'node:path';
import { z } from 'zod';

const profileSchema = z.object({ token: z.string().min(1), expiresAt: z.string().datetime() });
const configSchema = z.object({ profiles: z.record(z.string(), profileSchema) });
export type Profile = z.infer<typeof profileSchema>;

export const configPath = () =>
  join(process.env.PGSTACK_CONFIG_DIR ?? join(homedir(), '.config', 'pgstack'), 'credentials.json');

export const readProfile = async (apiUrl: string) => (await readConfig()).profiles[apiUrl];

export const saveProfile = async (apiUrl: string, profile?: Profile) => {
  const config = await readConfig();
  if (profile) config.profiles[apiUrl] = profile;
  else delete config.profiles[apiUrl];
  const path = configPath();
  await mkdir(dirname(path), { recursive: true, mode: 0o700 });
  await chmod(dirname(path), 0o700);
  const temporary = `${path}.${randomUUID()}.tmp`;
  try {
    await writeFile(temporary, `${JSON.stringify(config, null, 2)}\n`, { mode: 0o600, flag: 'wx' });
    await rename(temporary, path);
  } finally {
    await rm(temporary, { force: true });
  }
};

const readConfig = async () => {
  try {
    return configSchema.parse(JSON.parse(await readFile(configPath(), 'utf8')));
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT')
      return { profiles: {} } as z.infer<typeof configSchema>;
    throw new Error(`Cannot read CLI credentials at ${configPath()}.`, { cause: error });
  }
};
