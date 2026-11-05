#!/usr/bin/env bun
import { createInterface } from 'node:readline/promises';
import { Command } from 'commander';
import { parseDuration, parseSessionSlug } from './arguments';
import { login } from './auth';
import { authenticatedClient, resolveOrganization } from './client';
import { saveProfile } from './config';
import { chooseProject, parseEnvironment, projectSelector, type ProjectSelector } from './projects';
import { version } from '../package.json';

const API_URL = process.env.API_URL ?? 'https://api.pgstack.io';

const program = new Command()
  .name('pgstack')
  .description('Temporary PostgreSQL access through PgStack')
  .version(version, '-v, --version')
  .option('--organization <slug>', 'Organization slug or ID', process.env.PGSTACK_ORGANIZATION)
  .option('--json', 'Print machine-readable JSON')
  .action(() => program.outputHelp());

const auth = program.command('auth').description('Sign in and manage CLI credentials');
auth.action(() => auth.outputHelp());

auth
  .command('login')
  .description('Sign in through your browser')
  .option('--no-browser', 'Print the URL without opening a browser')
  .action(async (options: { browser: boolean }) => login(API_URL, options.browser));

auth
  .command('status')
  .description('Show your account and organizations')
  .action(async () => {
    const status = await (await authenticatedClient(API_URL)).cliAuth.status.query();
    if (program.opts().json) return printJson(status);
    console.log(
      `Signed in as ${status.user.email ?? status.user.githubUsername}\nLogin expires: ${status.expiresAt.toISOString()}`,
    );
    console.table(
      status.organizations.map((organization) => ({ organization: organization.slug, name: organization.name })),
    );
  });

auth
  .command('logout')
  .description('Remove the saved CLI token from this computer')
  .action(async () => {
    await saveProfile(API_URL);
    if (program.opts().json) printJson({ success: true });
    else console.log('Signed out. Existing Access sessions retain their own expiration.');
  });

const access = program.command('access').description('Start and manage your PostgreSQL Access sessions');

access
  .command('start [slug]')
  .description('Start a session using a project slug or --env <environment>')
  .addHelpText('after', '\nUse the returned PostgreSQL URL to connect.')
  .option('--env <environment>', 'Environment to access', parseEnvironment)
  .option('--duration <duration>', 'Access duration, up to 1h', '1h')
  .option('--reason <reason>', 'Reason for access')
  .option('--role <role>', 'Configured Access role name', 'Default')
  .action(
    async (
      identifier: string | undefined,
      options: {
        duration: string;
        reason?: string;
        role: string;
        env?: ProjectSelector['environment'];
      },
    ) => {
      const selector = projectSelector(identifier, options);
      const durationSeconds = parseDuration(options.duration);
      const { client, organizationId } = await sessionContext();
      const selected = await selectAccessProject(client, organizationId, selector);
      const project = await client.accessSession.findProject.query({ organizationId, identifier: selected.slug });
      let reason = options.reason?.trim();
      if (project.requireReason && !reason) {
        if (program.opts().json || !process.stdin.isTTY) throw new Error('This project requires --reason <reason>.');
        const prompt = createInterface({ input: process.stdin, output: process.stdout });
        try {
          reason = (await prompt.question('Reason for access: ')).trim();
        } finally {
          prompt.close();
        }
        if (!reason) throw new Error('A reason for access is required.');
      }
      const session = await client.accessSession.start.mutate({
        organizationId,
        identifier: project.slug,
        durationSeconds,
        reason,
        role: options.role,
      });
      if (program.opts().json) return printJson(session);
      console.log(session.connectionUrl);
    },
  );

access
  .command('list [slug]')
  .description('List your active sessions, optionally filtered by project')
  .action(async (identifier?: string) => {
    const { client, organizationId } = await sessionContext();
    const sessions = await client.accessSession.list.query({ organizationId, identifier });
    if (program.opts().json) return printJson(sessions);
    if (!sessions.length) return console.log('No active sessions.');
    console.table(
      sessions.map((session) => ({
        session: session.slug,
        project: session.projectSlug,
        environment: session.environment.toLowerCase(),
        expires: session.expiresAt.toISOString(),
        reason: session.reason,
      })),
    );
  });

access
  .command('extend <session>')
  .description('Add time to an active session; at most 1h may remain')
  .option('--duration <duration>', 'Additional time to add', '15m')
  .action(async (value: string, options: { duration: string }) => {
    const slug = parseSessionSlug(value);
    const durationSeconds = parseDuration(options.duration);
    const { client, organizationId } = await sessionContext();
    const session = await client.accessSession.extend.mutate({ organizationId, slug, durationSeconds });
    if (program.opts().json) return printJson(session);
    console.log(`Session ${session.slug} now expires ${session.expiresAt.toISOString()}.`);
  });

access
  .command('revoke <session>')
  .description('End access and retain the session history')
  .addHelpText('after', '\nOpen connections close within one minute.')
  .option('--reason <reason>', 'Reason for revoking access')
  .action(async (value: string, options: { reason?: string }) => {
    const slug = parseSessionSlug(value);
    const { client, organizationId } = await sessionContext();
    const session = await client.accessSession.revoke.mutate({ organizationId, slug, reason: options.reason });
    if (program.opts().json) return printJson(session);
    console.log(`Session ${session.slug} revoked.`);
  });

const printJson = (value: unknown) => console.log(JSON.stringify(value, null, 2));
const sessionContext = async () => {
  const client = await authenticatedClient(API_URL);
  const organizationId = await resolveOrganization(client, program.opts().organization as string | undefined);
  return { client, organizationId };
};

const selectAccessProject = async (
  client: Awaited<ReturnType<typeof authenticatedClient>>,
  organizationId: string,
  selector: ProjectSelector,
) => {
  const projects = await client.accessSession.findProjects.query({ organizationId, environment: selector.environment });
  let prompt: ReturnType<typeof createInterface> | undefined;
  const ask =
    !program.opts().json && process.stdin.isTTY
      ? async (question: string) => {
          prompt ??= createInterface({ input: process.stdin, output: process.stdout });
          return prompt.question(question);
        }
      : undefined;
  try {
    return await chooseProject(projects, selector, ask);
  } finally {
    prompt?.close();
  }
};

await program.parseAsync().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : 'Command failed.';
  if (program.opts().json) console.error(JSON.stringify({ error: message }));
  else console.error(`Error: ${message}`);
  process.exitCode = 1;
});
