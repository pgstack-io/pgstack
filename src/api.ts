import type { TRPCBuiltRouter, TRPCDefaultErrorShape, TRPCMutationProcedure, TRPCQueryProcedure } from '@trpc/server';

export const Environment = {
  TESTING: 'TESTING',
  DEVELOPMENT: 'DEVELOPMENT',
  STAGING: 'STAGING',
  PRODUCTION: 'PRODUCTION',
} as const;
export type Environment = (typeof Environment)[keyof typeof Environment];

type Query<Input, Output> = TRPCQueryProcedure<{ input: Input; output: Output; meta: object }>;
type Mutation<Input, Output> = TRPCMutationProcedure<{ input: Input; output: Output; meta: object }>;
type Credential = { token: string; expiresAt: Date };
type Organization = { id: string; slug: string; name: string };
type OrganizationInput = { organizationId: string };
type ProjectInput = OrganizationInput & { identifier: string };
type SessionInput = OrganizationInput & { slug: string };
type Project = { slug: string; name: string; environment: Environment; state: string };
export type AccessSession = {
  slug: string;
  projectSlug: string;
  projectName: string;
  environment: Environment;
  role: string;
  readOnly?: boolean;
  expiresAt: Date;
  reason: string | null | undefined;
  revokedAt: Date | null | undefined;
};

// API procedures consumed by the CLI.
export type CliRouter = TRPCBuiltRouter<
  { ctx: object; meta: object; errorShape: TRPCDefaultErrorShape; transformer: true },
  {
    cliAuth: {
      startLogin: Mutation<
        void,
        { deviceCode: string; userCode: string; verificationUrl: string; expiresAt: Date; intervalSeconds: number }
      >;
      pollLogin: Mutation<{ deviceCode: string }, { status: 'pending' } | ({ status: 'approved' } & Credential)>;
      status: Query<
        void,
        {
          user: { id: string; email: string | null | undefined; githubUsername: string };
          expiresAt: Date;
          organizations: Organization[];
        }
      >;
      refresh: Mutation<void, Credential>;
    };
    accessSession: {
      findProjects: Query<OrganizationInput & { environment?: Environment }, Project[]>;
      findProject: Query<ProjectInput, { slug: string; name: string; requireReason: boolean }>;
      start: Mutation<
        ProjectInput & { durationSeconds: number; reason?: string; role?: string; readOnly?: boolean },
        AccessSession & { connectionUrl: string }
      >;
      list: Query<OrganizationInput & { identifier?: string }, AccessSession[]>;
      extend: Mutation<SessionInput & { durationSeconds: number }, AccessSession>;
      revoke: Mutation<SessionInput & { reason?: string }, AccessSession>;
    };
  }
>;
