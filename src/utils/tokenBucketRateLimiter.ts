// Simple token-bucket rate limiter for Workers / Node
// - capacity: max burst tokens
// - refillPerSecond: how many tokens are added per second
// Usage:
//   const limiter = new TokenBucketRateLimiter(250, 25);
//   await limiter.removeToken(); // wait until a token is available
//   // then perform the request
// If you get a 429 with Retry-After, call limiter.pause(ms)

export class TokenBucketRateLimiter {
  private capacity: number;
  private tokens: number;
  private refillPerMs: number; // tokens per millisecond
  private lastRefill: number;
  private pausedUntil: number | null;

  constructor(capacity: number, refillPerSecond: number) {
    this.capacity = Math.max(1, Math.floor(capacity));
    this.tokens = this.capacity;
    this.refillPerMs = refillPerSecond / 1000;
    this.lastRefill = Date.now();
    this.pausedUntil = null;
  }

  private refill() {
    const now = Date.now();
    if (now <= this.lastRefill) return;
    const elapsed = now - this.lastRefill;
    const add = elapsed * this.refillPerMs;
    if (add > 0) {
      this.tokens = Math.min(this.capacity, this.tokens + add);
      this.lastRefill = now;
    }
  }

  // Remove a single token (default). Will wait until available.
  // maxWaitMs avoids waiting forever; you can tune or remove if you prefer.
  async removeToken(maxWaitMs = 60_000): Promise<void> {
    await this.removeTokens(1, maxWaitMs);
  }

  // Remove count tokens, waiting if needed.
  async removeTokens(count: number, maxWaitMs = 60_000): Promise<void> {
    const start = Date.now();
    count = Math.max(1, Math.floor(count));

    while (true) {
      // if paused (due to 429), wait until pause expires
      if (this.pausedUntil && Date.now() < this.pausedUntil) {
        const waitMs = Math.min(this.pausedUntil - Date.now(), 1000);
        await this.sleep(waitMs);
        continue;
      }

      this.refill();

      if (this.tokens >= count) {
        this.tokens -= count;
        return;
      }

      // compute how long until enough tokens accumulate
      const deficit = count - this.tokens;
      const waitMsRaw = Math.ceil(deficit / this.refillPerMs);
      // don't wait too long in single chunk; re-check paused flag periodically
      const sleepMs = Math.min(waitMsRaw, 1000);

      if (Date.now() - start + sleepMs > maxWaitMs) {
        throw new Error('Rate limit wait exceeded');
      }

      await this.sleep(sleepMs);
    }
  }

  // Pause issuing tokens for durationMs (useful when receiving Retry-After)
  pause(durationMs: number) {
    const until = Date.now() + Math.max(0, Math.floor(durationMs));
    if (!this.pausedUntil || until > this.pausedUntil) {
      this.pausedUntil = until;
    }
  }

  // Convenience: parse Retry-After values (seconds or HTTP-date) and pause
  pauseFromRetryAfter(retryAfterValue: string | null) {
    if (!retryAfterValue) return;
    const seconds = Number(retryAfterValue);
    if (!Number.isNaN(seconds) && seconds > 0) {
      this.pause(seconds * 1000);
      return;
    }
    // attempt to parse HTTP-date
    const date = Date.parse(retryAfterValue);
    if (!Number.isNaN(date)) {
      const ms = Math.max(0, date - Date.now());
      this.pause(ms);
    }
  }

  getStatus() {
    this.refill();
    return {
      tokens: Math.floor(this.tokens),
      capacity: this.capacity,
      pausedUntil: this.pausedUntil,
    };
  }

  // Internal helper
  private sleep(ms: number) {
    return new Promise((resolve) => setTimeout(resolve, Math.max(0, ms)));
  }
}

// Export a default instance you can reuse app-wide (tune values as needed).
// For 250 requests / 10s -> refill=25/s, capacity=250
export const defaultBungieLimiter = new TokenBucketRateLimiter(250, 25);