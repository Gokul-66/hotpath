import type { AdmissionPolicy } from "./lru.js";

const ROWS = 4;
const MAX_COUNT = 15;

/** FNV-1a, with a final mix so nearby keys spread across the table. */
function hash(key: string): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < key.length; i++) {
    h ^= key.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  h ^= h >>> 16;
  h = Math.imul(h, 0x85ebca6b);
  h ^= h >>> 13;
  return h >>> 0;
}

/** Double hashing: rows step by a second value derived from the first hash. Odd, so it never shares a factor with the width. */
function stride(h: number): number {
  let s = Math.imul(h ^ (h >>> 15), 0x9e3779b1);
  s ^= s >>> 14;
  return s | 1;
}

/**
 * Approximate counts in fixed memory. Estimates can be too high because of collisions, but are
 * never too low between agings. Each operation is O(ROWS), independent of the number of keys.
 */
export class CountMinSketch {
  private readonly table: Uint8Array;
  private readonly mask: number;

  constructor(minWidth: number) {
    const width = 2 ** Math.ceil(Math.log2(Math.max(16, minWidth)));
    this.mask = width - 1;
    this.table = new Uint8Array(width * ROWS);
  }

  // Runs on every read, so both loops are written out and allocate nothing.
  increment(key: string): void {
    const { table, mask } = this;
    const h1 = hash(key);
    const h2 = stride(h1);
    for (let row = 0, base = 0; row < ROWS; row++, base += mask + 1) {
      const slot = base + (((h1 + Math.imul(row, h2)) >>> 0) & mask);
      if (table[slot] < MAX_COUNT) table[slot]++;
    }
  }

  estimate(key: string): number {
    const { table, mask } = this;
    const h1 = hash(key);
    const h2 = stride(h1);
    let min = MAX_COUNT;
    for (let row = 0, base = 0; row < ROWS; row++, base += mask + 1) {
      const count = table[base + (((h1 + Math.imul(row, h2)) >>> 0) & mask)];
      if (count < min) min = count;
    }
    return min;
  }

  /** Halves every counter, so old popularity fades. */
  age(): void {
    for (let i = 0; i < this.table.length; i++) this.table[i] >>>= 1;
  }
}

/**
 * Admits a key only if it has been requested more often than the entry it would evict, so a
 * one-off scan cannot flush popular entries. The cost: a newly popular key must earn its place.
 */
export class TinyLfu implements AdmissionPolicy {
  private readonly sketch: CountMinSketch;
  private readonly sampleSize: number;
  private accesses = 0;

  constructor(capacity: number) {
    this.sketch = new CountMinSketch(capacity * 8);
    this.sampleSize = capacity * 10;
  }

  recordAccess(key: string): void {
    this.sketch.increment(key);
    if (++this.accesses >= this.sampleSize) {
      this.sketch.age();
      this.accesses = Math.floor(this.accesses / 2);
    }
  }

  admit(candidate: string, victim: string): boolean {
    return this.sketch.estimate(candidate) > this.sketch.estimate(victim);
  }
}
