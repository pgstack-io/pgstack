import { createTRPCClient, httpLink } from '@trpc/client';
import type { CliRouter } from './api';
import superjson from 'superjson';
import { readProfile, saveProfile } from './config';

export const createClient = (apiUrl: string, token?: string) =>
  createTRPCClient<CliRouter>({
    links: [
      httpLink({
        url: `${apiUrl}/trpc`,
        transformer: superjson,
        headers: () => (token ? { authorization: `Bearer ${token}` } : {}),
        async fetch(url, options) {
          const response = await fetch(url, { ...options, redirect: 'error', signal: AbortSignal.timeout(15_000) });
          const contentType = response.headers.get('content-type')?.split(';')[0]?.trim().toLowerCase();
          if (contentType !== 'application/json') {
            throw new Error(
              `PgStack API returned HTTP ${response.status} (${contentType ?? 'unknown content type'}) instead of JSON. Check that the API and its proxy are running.`,
            );
          }
          return response;
        },
      }),
    ],
  });

export const authenticatedClient = async (apiUrl: string) => {
  const profile = await readProfile(apiUrl);
  if (!profile || new Date(profile.expiresAt) <= new Date()) throw new Error('Run pgstack auth login to sign in.');
  const credential = await createClient(apiUrl, profile.token).cliAuth.refresh.mutate();
  await saveProfile(apiUrl, { token: credential.token, expiresAt: credential.expiresAt.toISOString() });
  return createClient(apiUrl, credential.token);
};

export const resolveOrganization = async (client: ReturnType<typeof createClient>, identifier?: string) => {
  const { organizations } = await client.cliAuth.status.query();
  if (identifier) {
    const organization = organizations.find((item) => item.id === identifier || item.slug === identifier);
    if (!organization) throw new Error('Organization not found. Use pgstack auth status to see your organizations.');
    return organization.id;
  }
  if (organizations.length === 1) return organizations[0]!.id;
  if (!organizations.length) throw new Error('Join an organization on PgStack before starting Access sessions.');
  throw new Error(
    `Choose an organization with --organization <slug>: ${organizations.map((item) => `${item.slug} (${item.name})`).join(', ')}`,
  );
};
