export const parseDuration = (value: string) => {
  const match = value.match(/^(\d+)(s|m|h)$/);
  if (!match) throw new Error('Use a duration such as 30s, 15m, or 1h.');
  const duration = Number(match[1]) * ({ s: 1, m: 60, h: 3600 }[match[2]!] ?? 0);
  if (!Number.isSafeInteger(duration) || duration < 1 || duration > 3600)
    throw new Error('Duration must be between 1 second and 1 hour.');
  return duration;
};

export const parseSessionSlug = (value: string) => {
  const slug = value.replace(/^session_/, '');
  if (!/^[a-f0-9]{12}$/.test(slug)) throw new Error('Use the session ID shown by pgstack access list.');
  return slug;
};
