/** Decides whether a new key may displace the current eviction victim. */
export interface AdmissionPolicy {
  recordAccess(key: string): void;
  admit(candidate: string, victim: string): boolean;
}

interface Node<V> {
  key: string;
  value: V;
  prev: Node<V>;
  next: Node<V>;
}

export interface LruStats {
  hits: number;
  misses: number;
  evictions: number;
  rejected: number;
}

/**
 * Fixed-capacity LRU: a hash map for O(1) lookup and a doubly linked list for O(1)
 * reordering and eviction. The sentinel's next is the most recent node and its prev is the victim.
 */
export class LruCache<V> {
  readonly stats: LruStats = { hits: 0, misses: 0, evictions: 0, rejected: 0 };
  private readonly nodes = new Map<string, Node<V>>();
  private readonly sentinel: Node<V>;

  constructor(
    readonly capacity: number,
    private readonly admission?: AdmissionPolicy
  ) {
    if (!Number.isInteger(capacity) || capacity < 1) throw new RangeError("capacity must be a positive integer");
    const sentinel = { key: "", value: undefined } as unknown as Node<V>;
    sentinel.prev = sentinel;
    sentinel.next = sentinel;
    this.sentinel = sentinel;
  }

  get size(): number {
    return this.nodes.size;
  }

  get(key: string): V | undefined {
    this.admission?.recordAccess(key);
    const node = this.nodes.get(key);
    if (!node) {
      this.stats.misses++;
      return undefined;
    }
    this.stats.hits++;
    this.unlink(node);
    this.pushFront(node);
    return node.value;
  }

  /** Returns false if the admission policy kept the key out. */
  set(key: string, value: V): boolean {
    const existing = this.nodes.get(key);
    if (existing) {
      existing.value = value;
      this.unlink(existing);
      this.pushFront(existing);
      return true;
    }
    if (this.nodes.size >= this.capacity) {
      const victim = this.sentinel.prev;
      if (this.admission && !this.admission.admit(key, victim.key)) {
        this.stats.rejected++;
        return false;
      }
      this.unlink(victim);
      this.nodes.delete(victim.key);
      this.stats.evictions++;
    }
    const node = { key, value } as Node<V>;
    this.nodes.set(key, node);
    this.pushFront(node);
    return true;
  }

  delete(key: string): boolean {
    const node = this.nodes.get(key);
    if (!node) return false;
    this.unlink(node);
    this.nodes.delete(key);
    return true;
  }

  clear(): void {
    this.nodes.clear();
    this.sentinel.prev = this.sentinel;
    this.sentinel.next = this.sentinel;
  }

  /** Most recent first. For tests. */
  keys(): string[] {
    const keys: string[] = [];
    for (let n = this.sentinel.next; n !== this.sentinel; n = n.next) keys.push(n.key);
    return keys;
  }

  private unlink(node: Node<V>): void {
    node.prev.next = node.next;
    node.next.prev = node.prev;
  }

  private pushFront(node: Node<V>): void {
    node.next = this.sentinel.next;
    node.prev = this.sentinel;
    this.sentinel.next.prev = node;
    this.sentinel.next = node;
  }
}
