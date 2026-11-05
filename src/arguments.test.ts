import { describe, expect, test } from 'bun:test';
import { parseDuration, parseSessionSlug } from './arguments';

describe('command arguments', () => {
  test('accepts bounded durations and rejects ambiguous or overflowing values', () => {
    expect(parseDuration('1h')).toBe(3600);
    expect(parseDuration('15m')).toBe(900);
    for (const invalid of ['0s', '2h', '-1m', '1.5h', '9007199254740993s', '30', '1h30m']) {
      expect(() => parseDuration(invalid)).toThrow();
    }
  });
  test('accepts session IDs and database usernames', () => {
    expect(parseSessionSlug('abcdef123456')).toBe('abcdef123456');
    expect(parseSessionSlug('session_abcdef123456')).toBe('abcdef123456');
    expect(() => parseSessionSlug('project-name')).toThrow();
  });
});
