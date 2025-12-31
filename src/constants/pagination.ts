// ============================================================================
// FILE: src/constants/pagination.ts
// Pagination constants for Bungie API fetching
// ============================================================================

/**
 * Maximum page number to fetch per character/mode combination (0-indexed)
 * Prevents infinite loops in case of API issues
 * e.g., MAX_PAGES_PER_CHARACTER = 100 means we'll fetch pages 0-99
 */
export const MAX_PAGES_PER_CHARACTER = 99;

/**
 * Default page size for activity fetching
 */
export const DEFAULT_PAGE_SIZE = 250;
