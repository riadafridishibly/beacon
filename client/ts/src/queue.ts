import { byteLength } from "./event.ts";

/** Store holds the queue and device id. Each set must replace the value atomically. */
export interface Store {
  keys(prefix: string): Promise<string[]>;
  get(key: string): Promise<string | null>;
  set(key: string, value: string): Promise<void>;
  remove(key: string): Promise<void>;
}

const segmentPrefix = "queue.";

/** Segment is a group of events that goes to the server as one request. */
export interface Segment {
  key: string;
  size: number;
  events: number;
  // In memory until written. The open segment always keeps them.
  lines?: string[];
  saved?: boolean;
}

/** QueueOptions configures Queue.open. */
export interface QueueOptions {
  store: Store;
  maxBytes: number;
  maxEvents: number;
  batchBytes: number;
  onDrop: (events: number) => void;
  onError: (message: string) => void;
}

function parseLines(value: string): string[] {
  return value.split("\n").filter((line) => {
    if (line === "") return false;
    try {
      JSON.parse(line);
      return true;
    } catch {
      return false; // cut short by a crash
    }
  });
}

/** Queue stores events until the server accepts them. Store calls run one at a time. */
export class Queue {
  private readonly opts: QueueOptions;
  private readonly batchBytes: number;
  private segs: Segment[] = [];
  private active: Segment | null = null;
  private total = 0;
  private next = 0;
  private readonly dirty = new Set<Segment>();
  private writeScheduled = false;
  private ops: Promise<unknown> = Promise.resolve();

  private constructor(opts: QueueOptions) {
    this.opts = opts;
    this.batchBytes = Math.min(opts.batchBytes, opts.maxBytes);
  }

  static async open(opts: QueueOptions): Promise<Queue> {
    const q = new Queue(opts);
    for (const key of (await opts.store.keys(segmentPrefix)).sort()) {
      const value = await opts.store.get(key);
      if (value === null) continue;
      const size = byteLength(value);
      q.segs.push({ key, size, events: value.split("\n").filter(Boolean).length });
      q.total += size;
      q.next = Number(key.slice(segmentPrefix.length)) + 1;
    }
    return q;
  }

  get totalBytes(): number {
    return this.total;
  }

  get segments(): readonly Segment[] {
    return this.segs;
  }

  /** add appends one encoded event and reports whether a segment is ready to send. */
  add(line: string): boolean {
    const n = byteLength(line) + 1;
    let ready = false;
    if (this.active && this.active.size + n > this.batchBytes) {
      this.seal();
      ready = true;
    }
    if (!this.active) {
      const key = segmentPrefix + String(this.next++).padStart(20, "0");
      this.active = { key, size: 0, events: 0, lines: [] };
      this.segs.push(this.active);
    }
    const seg = this.active;
    seg.lines?.push(line);
    seg.size += n;
    seg.events++;
    this.total += n;
    this.markDirty(seg);
    if (seg.events >= this.opts.maxEvents) {
      this.seal();
      ready = true;
    }
    while (this.total > this.opts.maxBytes && this.segs.length > 1) {
      const oldest = this.segs[0] as Segment;
      this.remove(oldest);
      this.opts.onDrop(oldest.events);
    }
    return ready;
  }

  seal(): void {
    if (this.active?.saved) this.active.lines = undefined;
    this.active = null;
  }

  /**
   * sealIdle seals the open segment unless older segments are still waiting.
   * Sealing on every tick while offline would leave one small entry per interval.
   */
  sealIdle(): void {
    if (this.segs.length === 1) this.seal();
  }

  /** oldest returns the first sealed segment, null when there is none. */
  oldest(): Segment | null {
    const first = this.segs[0];
    if (!first || first === this.active) return null;
    return first;
  }

  async read(seg: Segment): Promise<string[]> {
    if (seg.lines) return [...seg.lines];
    const value = await this.run(() => this.opts.store.get(seg.key));
    return value === null ? [] : parseLines(value);
  }

  /** remove deletes seg. The size cap may have already removed it. */
  remove(seg: Segment): void {
    const i = this.segs.indexOf(seg);
    if (i < 0) return;
    this.segs.splice(i, 1);
    this.total -= seg.size;
    this.dirty.delete(seg);
    this.run(() => this.opts.store.remove(seg.key)).catch((err) => {
      this.opts.onError(`remove from queue: ${err}`);
    });
  }

  rewrite(seg: Segment, lines: string[]): void {
    if (!this.segs.includes(seg)) return;
    const size = lines.reduce((sum, line) => sum + byteLength(line) + 1, 0);
    this.total += size - seg.size;
    seg.size = size;
    seg.events = lines.length;
    seg.lines = lines;
    this.markDirty(seg);
  }

  /** sync resolves once every queued write has been attempted. */
  async sync(): Promise<void> {
    do {
      await this.ops;
    } while (this.writeScheduled || this.dirty.size > 0);
  }

  private markDirty(seg: Segment): void {
    seg.saved = false;
    this.dirty.add(seg);
    if (this.writeScheduled) return;
    this.writeScheduled = true;
    this.ops = this.ops.then(() => this.writeDirty());
  }

  private async writeDirty(): Promise<void> {
    this.writeScheduled = false;
    const segs = [...this.dirty];
    this.dirty.clear();
    for (const seg of segs) {
      if (!this.segs.includes(seg) || !seg.lines) continue;
      try {
        await this.opts.store.set(seg.key, `${seg.lines.join("\n")}\n`);
        if (!this.dirty.has(seg)) {
          seg.saved = true;
          if (seg !== this.active) seg.lines = undefined;
        }
      } catch (err) {
        // A sealed segment keeps its lines, so it can still be sent.
        this.opts.onError(`write queue: ${err}`);
      }
    }
  }

  private run<T>(op: () => Promise<T>): Promise<T> {
    const result = this.ops.then(op);
    this.ops = result.catch(() => undefined);
    return result;
  }
}
