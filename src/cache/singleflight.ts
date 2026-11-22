/** Concurrent calls for one key share a single execution within this process. */
export class SingleFlight<T> {
  private readonly inFlight = new Map<string, Promise<T>>();

  get size(): number {
    return this.inFlight.size;
  }

  do(key: string, fn: () => Promise<T>): { promise: Promise<T>; shared: boolean } {
    const existing = this.inFlight.get(key);
    if (existing) return { promise: existing, shared: true };
    const promise = fn().finally(() => {
      if (this.inFlight.get(key) === promise) this.inFlight.delete(key);
    });
    this.inFlight.set(key, promise);
    return { promise, shared: false };
  }

  /** Later callers start a new execution instead of joining one that may predate a write. */
  forget(key: string): void {
    this.inFlight.delete(key);
  }

  forgetAll(): void {
    this.inFlight.clear();
  }
}
