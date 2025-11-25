/**
 * Rate limiting utilities for API calls
 */

const sleep = (ms: number) => new Promise((res) => setTimeout(res, ms));

export async function withRateLimit<T>(
  fn: () => Promise<T>,
  delayMs: number = 100,
  maxRetries: number = 3
): Promise<T> {
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      await sleep(delayMs * attempt); // Progressive delay
      return await fn();
    } catch (error: any) {
      // Check if it's a throttle error
      if (
        error?.message?.includes("throttle") ||
        error?.message?.includes("rate limit") ||
        error?.message?.includes("429") ||
        error?.status === 429
      ) {
        if (attempt === maxRetries) {
          throw error; // Final attempt failed
        }

        // Exponential backoff: 100ms, 500ms, 2000ms
        const backoffDelay = Math.min(100 * Math.pow(5, attempt + 1), 5000);
        console.log(
          `Rate limit hit, retrying in ${backoffDelay}ms (attempt ${attempt + 1}/${
            maxRetries + 1
          })`
        );
        await sleep(backoffDelay);
        continue;
      }

      // Non-throttle error, throw immediately
      throw error;
    }
  }
  throw new Error("Rate limit retry exhausted");
}
