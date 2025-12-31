// ============================================================================
// FILE: src/constants/pagination.ts
// Pagination constants for Bungie API fetching
// ============================================================================

/**
 * Maximum number of pages to fetch per character/mode combination
 * Prevents infinite loops in case of API issues
 */
export const MAX_PAGES_PER_CHARACTER = 100;

/**
 * Default page size for activity fetching
 */
export const DEFAULT_PAGE_SIZE = 250;
