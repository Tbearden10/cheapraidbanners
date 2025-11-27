// Resolve Bungie lastOnlineStatusChange to canonical epoch-ms timestamp.
// Strategy:
// 1) Try epoch seconds (value * 1000).
// 2) If that yields an implausible year, try epoch milliseconds (value as-is).
// 3) If still implausible, treat as "seconds ago": Date.now() - value*1000.
// 4) If none produce a plausible year, return null.
//
// Plausible year range: [2000, currentYear + 1]
export function resolveLastOnlineStatusChangeToMs(value: any): number | null {
  if (value === null || value === undefined) return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;

  const now = Date.now();
  const currentYear = new Date(now).getUTCFullYear();
  const minYear = 2000;
  const maxYear = currentYear + 1;

  const isPlausible = (ts: number) => {
    if (!Number.isFinite(ts) || ts <= 0) return false;
    const yr = new Date(ts).getUTCFullYear();
    return !Number.isNaN(yr) && yr >= minYear && yr <= maxYear;
  };

  // 1) Try as epoch seconds
  const asSeconds = Math.trunc(n) * 1000;
  if (isPlausible(asSeconds)) return asSeconds;

  // 2) Try as epoch milliseconds
  const asMillis = Math.trunc(n);
  if (isPlausible(asMillis)) return asMillis;

  // 3) Try as "seconds ago"
  const asSecondsAgo = now - Math.trunc(n) * 1000;
  if (isPlausible(asSecondsAgo)) return asSecondsAgo;

  // 4) No plausible interpretation
  return null;
}