import { describe, expect, mock, test } from 'bun:test';
import { Environment } from './api';
import { chooseProject, parseEnvironment, projectSelector } from './projects';

const projects = [
  { slug: 'orders-production', name: 'Orders', environment: 'PRODUCTION', state: 'RUNNING' },
  { slug: 'accounts-production', name: 'Accounts', environment: 'PRODUCTION', state: 'RUNNING' },
  { slug: 'orders-staging', name: 'Orders', environment: 'STAGING', state: 'RUNNING' },
];

describe('Access project selection', () => {
  test('accepts case-insensitive environment flags and rejects unknown environments', () => {
    expect(parseEnvironment('production')).toBe(Environment.PRODUCTION);
    expect(parseEnvironment('STAGING')).toBe(Environment.STAGING);
    expect(() => parseEnvironment('prod')).toThrow('Choose an environment');
    expect(() => projectSelector(undefined, {})).toThrow('Specify a project');
  });

  test('uses the only matching project without prompting', async () => {
    const ask = mock(async () => 'unexpected');
    expect(await chooseProject(projects, { environment: parseEnvironment('staging') }, ask)).toEqual(projects[2]!);
    expect(ask).not.toHaveBeenCalled();
  });

  test('shows slugs and project names when an environment is ambiguous', async () => {
    const ask = mock(async (question: string) => {
      expect(question).toContain('accounts-production - Accounts');
      expect(question).toContain('orders-production - Orders');
      expect(question).not.toContain('orders-staging');
      return 'orders-production';
    });
    expect(await chooseProject(projects, { environment: parseEnvironment('production') }, ask)).toEqual(projects[0]!);
    expect(ask).toHaveBeenCalledTimes(1);
  });

  test('accepts numbers or slugs and retries invalid selections', async () => {
    const answers = ['99', 'missing', '2'];
    const ask = mock(async () => answers.shift()!);
    expect(await chooseProject(projects, { environment: parseEnvironment('production') }, ask)).toEqual(projects[0]!);
    expect(ask).toHaveBeenCalledTimes(3);
    expect(await chooseProject(projects, {}, async () => 'orders-staging')).toEqual(projects[2]!);
    await expect(chooseProject(projects, {}, async () => '')).rejects.toThrow('cancelled');
  });

  test('requires an explicit project in noninteractive mode when multiple match', async () => {
    await expect(chooseProject(projects, { environment: parseEnvironment('production') })).rejects.toThrow(
      'Specify a project slug',
    );
    expect(
      await chooseProject(projects, { environment: parseEnvironment('production'), identifier: 'orders-production' }),
    ).toEqual(projects[0]!);
    expect(await chooseProject(projects, { identifier: 'orders-staging' })).toEqual(projects[2]!);
    await expect(chooseProject(projects, { identifier: 'Orders' })).rejects.toThrow('No Access projects');
  });

  test('keeps explicit environment filtering separate from slug matching', async () => {
    const collision = { slug: 'production', name: 'Production', environment: 'STAGING' };
    expect(await chooseProject([collision, projects[0]!], { environment: parseEnvironment('production') })).toEqual(
      projects[0]!,
    );
    expect(await chooseProject([collision, projects[0]!], { identifier: 'production' })).toEqual(collision);
    await expect(
      chooseProject(projects, { identifier: 'orders-staging', environment: parseEnvironment('production') }),
    ).rejects.toThrow('No Access projects');
  });

  test('treats positional arguments only as slugs', async () => {
    expect(projectSelector('orders-production', {})).toEqual({
      identifier: 'orders-production',
      environment: undefined,
    });
    await expect(chooseProject(projects, projectSelector('production', {}))).rejects.toThrow('No Access projects');
    await expect(chooseProject(projects, projectSelector('Orders', {}))).rejects.toThrow('No Access projects');
  });
});
