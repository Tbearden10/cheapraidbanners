export function isActivityCompleted(a: any): boolean {
  // Mirror statsProcessor / SearchContext heuristic:
  //  - if explicit completed provided, honor it
  //  - else if timePlayedSeconds is present (numeric > 0) treat as completed
  //  - else if period / activityDetails.period exists, treat as completed
  //  - else default to true (conservative)
  if (!a) return false;

  // Bungie-style completed flag in many payloads
  if (typeof a.values?.completed?.basic?.value !== 'undefined') {
    return a.values.completed.basic.value === 1;
  }

  // Some shapes use activityDuration / timePlayedSeconds
  const maybeSeconds =
    typeof a.values?.timePlayedSeconds?.basic?.value === 'number'
      ? a.values.timePlayedSeconds.basic.value
      : typeof a.seconds === 'number'
      ? a.seconds
      : undefined;

  if (typeof maybeSeconds === 'number' && maybeSeconds > 0) return true;

  // Period implies the run exists and is likely trimmed/completed
  if (a.period || a.activityDetails?.period) return true;

  return true;
}