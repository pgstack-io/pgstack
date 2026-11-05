import type { Environment } from './api';

type Project = { slug: string; name: string; environment: string; state?: string };
export type ProjectSelector = { identifier?: string; environment?: Environment };
const ENVIRONMENTS = ['TESTING', 'DEVELOPMENT', 'STAGING', 'PRODUCTION'];

export const parseEnvironment = (value: string) => {
  const environment = value.trim().toUpperCase();
  if (!ENVIRONMENTS.includes(environment)) {
    throw new Error('Choose an environment: testing, development, staging, or production.');
  }
  return environment as Environment;
};

export const projectSelector = (slug: string | undefined, options: { env?: Environment }) => {
  const identifier = slug?.trim();
  if (!identifier && !options.env) throw new Error('Specify a project slug or --env <environment>.');
  return { identifier, environment: options.env };
};

export const chooseProject = async <T extends Project>(
  projects: T[],
  selector: ProjectSelector,
  ask?: (question: string) => Promise<string>,
) => {
  const inEnvironment = projects.filter(
    (project) => !selector.environment || project.environment === selector.environment,
  );
  const matches = (
    selector.identifier ? inEnvironment.filter((project) => project.slug === selector.identifier) : inEnvironment
  ).toSorted(
    (left, right) =>
      left.name.localeCompare(right.name) ||
      left.environment.localeCompare(right.environment) ||
      left.slug.localeCompare(right.slug),
  );
  if (!matches.length) {
    throw new Error(
      `No Access projects match${selector.identifier ? ` "${selector.identifier}"` : ''}${selector.environment ? ` in ${selector.environment.toLowerCase()}` : ''}.`,
    );
  }
  if (matches.length === 1) return matches[0]!;
  const choices = matches.map((project, index) => `${index + 1}. ${project.slug} - ${project.name}`).join('\n');
  if (!ask) {
    throw new Error(`Multiple projects match. Specify a project slug:\n${choices}`);
  }
  const question = `Multiple projects match. Which project would you like to use?\n${choices}\nEnter a number or slug: `;
  while (true) {
    const answer = (await ask(question)).trim();
    if (!answer) throw new Error('Project selection cancelled.');
    if (/^\d+$/.test(answer)) {
      const project = matches[Number(answer) - 1];
      if (project) return project;
    }
    const selected = matches.filter((project) => project.slug === answer);
    if (selected.length === 1) return selected[0]!;
  }
};
