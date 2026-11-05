import { describe, expect, test } from 'bun:test';
import superjson from 'superjson';
import { createClient } from './client';

describe('API responses', () => {
  test.each([200, 502])('reports HTTP %s HTML responses without a JSON parse error', async (status) => {
    const server = Bun.serve({
      hostname: '127.0.0.1',
      port: 0,
      fetch: () => new Response('<html>Proxy error</html>', { status, headers: { 'content-type': 'text/html' } }),
    });
    try {
      await expect(createClient(server.url.origin).cliAuth.status.query()).rejects.toThrow(
        `PgStack API returned HTTP ${status} (text/html) instead of JSON`,
      );
    } finally {
      server.stop(true);
    }
  });

  test('keeps SuperJSON dates in a successful response', async () => {
    const status = {
      user: { id: 'example-user', email: 'user@example.com', githubUsername: 'example-user' },
      expiresAt: new Date('2030-01-01T00:00:00Z'),
      organizations: [],
    };
    const server = Bun.serve({
      hostname: '127.0.0.1',
      port: 0,
      fetch: () => Response.json({ result: { data: superjson.serialize(status) } }),
    });
    try {
      expect(await createClient(server.url.origin).cliAuth.status.query()).toEqual(status);
    } finally {
      server.stop(true);
    }
  });
});
