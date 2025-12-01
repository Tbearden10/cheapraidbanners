// ============================================================================
// FILE: src/utils/rateLimiter.ts
// Simple sliding window rate limiter with parallel batching support
// Bungie API: 25 req/sec averaged over 10 seconds (250 req / 10 sec window)
// ============================================================================

export class SlidingWindowRateLimiter {
  private requests: number[] = [];
  private readonly maxRequests: number;
  private readonly windowMs: number;

  constructor(maxRequests: number, windowMs: number) {
    this.maxRequests = maxRequests;
    this.windowMs = windowMs;
  }

  /**
   * Acquire permission to make N requests
   * Returns immediately if capacity available, otherwise waits
   */
  async acquireBatch(count: number): Promise<void> {
    const now = Date.now();
    
    // Clean up old requests outside the window
    this.requests = this.requests.filter(t => t > now - this.windowMs);
    
    // Check if we have capacity for all requests
    const availableSlots = this.maxRequests - this.requests.length;
    
    if (availableSlots >= count) {
      // Reserve slots immediately
      for (let i = 0; i < count; i++) {
        this.requests.push(now);
      }
      return;
    }
    
    // Not enough capacity - calculate wait time
    const oldestRequest = this.requests[0];
    const waitTime = oldestRequest ? (oldestRequest + this.windowMs - now) : 0;
    
    if (waitTime > 0) {
      await new Promise(resolve => setTimeout(resolve, waitTime + 10)); // +10ms buffer
    }
    
    // Retry acquisition
    return this.acquireBatch(count);
  }

  /**
   * Acquire permission for a single request
   */
  async acquire(): Promise<void> {
    return this.acquireBatch(1);
  }

  /**
   * Get current capacity info
   */
  getStatus() {
    const now = Date.now();
    this.requests = this.requests.filter(t => t > now - this.windowMs);
    
    return {
      used: this.requests.length,
      available: this.maxRequests - this.requests.length,
      capacity: this.maxRequests,
      windowMs: this.windowMs,
    };
  }
}

// ============================================================================
// Global instance (shared across worker invocations in same isolate)
// ============================================================================

// Bungie API: 25 req/sec averaged over 10 seconds
export const bungieRateLimiter = new SlidingWindowRateLimiter(
  250,  // 250 requests
  10000 // per 10 seconds (25/sec average)
);

// ============================================================================
// Helper: Process items in parallel with rate limiting
// ============================================================================

/**
 * Process an array of items in parallel batches, respecting rate limits
 * 
 * @param items - Array of items to process
 * @param fn - Async function to process each item
 * @param options - Configuration
 * @returns Array of results (same order as input)
 */
export async function processWithRateLimit<T, R>(
  items: T[],
  fn: (item: T, index: number) => Promise<R>,
  options: {
    rateLimiter: SlidingWindowRateLimiter;
    batchSize?: number;  // Max items to process in parallel
    onProgress?: (completed: number, total: number) => void;
  }
): Promise<Array<R | null>> {
  const { rateLimiter, batchSize = 10, onProgress } = options;
  
  const results: Array<R | null> = new Array(items.length).fill(null);
  
  // Process in batches
  for (let i = 0; i < items.length; i += batchSize) {
    const batch = items.slice(i, i + batchSize);
    const batchSize_actual = batch.length;
    
    // Acquire rate limit tokens for entire batch upfront
    await rateLimiter.acquireBatch(batchSize_actual);
    
    // Process batch in parallel (we already reserved capacity)
    const batchResults = await Promise.allSettled(
      batch.map((item, idx) => fn(item, i + idx))
    );
    
    // Store results
    for (let j = 0; j < batchResults.length; j++) {
      const result = batchResults[j];
      if (result.status === 'fulfilled') {
        results[i + j] = result.value;
      }
    }
    
    // Progress callback
    if (onProgress) {
      onProgress(Math.min(i + batchSize, items.length), items.length);
    }
  }
  
  return results;
}