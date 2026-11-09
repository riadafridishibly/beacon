import {
  BeaconError,
  cleanMessage,
  filterProps,
  type Props,
  validate,
  type WireEvent,
  withMessage,
} from "./event.ts";
import { Queue, type Segment, type Store } from "./queue.ts";
import { backoffDelay, classify } from "./send.ts";

export { BeaconError, type Props } from "./event.ts";
export type { Store } from "./queue.ts";

const requestTimeoutMs = 30_000;
const maxBatchEvents = 1000;

// Keeps a batch under the server's 1 MiB limit even when it isn't compressed.
const maxBatchBytes = 900 << 10;

const loadedAt = Date.now();

/** ClientOptions configures createClient. endpoint, key and store are required. */
export interface ClientOptions {
  /** The server's base URL. */
  endpoint: string;
  /** The app's ingest key. */
  key: string;
  /** Holds the queue and the device id. Use one client per store. */
  store: Store;
  appVersion?: string;
  platform?: string;
  osVersion?: string;
  /** The props keys that may leave the app. Others are dropped. */
  allowProps?: readonly string[];
  /** Compresses request bodies. Without it they are sent uncompressed. */
  gzip?: (data: string) => Promise<Uint8Array>;
  /** Required where crypto.getRandomValues is missing, as on React Native. */
  randomId?: () => string;
  /** When the process started, in ms since the epoch, for appOpened. */
  processStartMs?: number;
  /** Caps the queue, 10 MiB by default. Past it the oldest events are dropped. */
  maxQueueBytes?: number;
  /** Receives messages about delivery problems and dropped data. */
  log?: (message: string) => void;
  /** At most 1000, the server's limit. */
  batchEvents?: number;
  flushIntervalMs?: number;
  heartbeatIntervalMs?: number;
  closeWaitMs?: number;
  minBackoffMs?: number;
  maxBackoffMs?: number;
  now?: () => number;
}

function defaultRandomId(): string {
  const c = globalThis.crypto;
  if (!c?.getRandomValues) throw new BeaconError("beacon: randomId is required on this platform");
  const bytes = c.getRandomValues(new Uint8Array(16));
  return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

// A logger that throws must not stop delivery.
function safeLog(log: ClientOptions["log"]): (message: string) => void {
  return (message) => {
    try {
      log?.(`beacon: ${message}`);
    } catch {
      // Nothing useful can be done with the failure.
    }
  };
}

function formatTime(ms: number): string {
  return new Date(ms).toISOString().replace(".000Z", "Z");
}

function unref(timer: unknown): void {
  (timer as { unref?: () => void }).unref?.();
}

/** Client queues and sends events. Create one with createClient. */
export class Client {
  /** The id generated for this install. */
  readonly deviceId: string;

  private readonly opts: ClientOptions;
  private readonly logf: (message: string) => void;
  private readonly url: string;
  private readonly queue: Queue;
  private readonly allowed: ReadonlySet<string>;
  private readonly now: () => number;
  private readonly randomId: () => string;
  private readonly startedMs: number;
  private readonly minBackoffMs: number;
  private readonly maxBackoffMs: number;
  private readonly flushTimer: ReturnType<typeof setInterval>;
  private readonly beatTimer: ReturnType<typeof setInterval>;

  private userId = "";
  private sessionId = "";
  private sessionStartMs = 0;
  private opened = false;
  private closed = false;
  private closing: Promise<void> | null = null;

  private failures = 0;
  private draining: Promise<void> | null = null;
  private again = false;
  private waiting = false;
  private stopped = false;
  private retryTimer: ReturnType<typeof setTimeout> | null = null;
  private inFlight: AbortController | null = null;

  /** @internal Use createClient. */
  constructor(opts: ClientOptions, deviceId: string, queue: Queue, randomId: () => string) {
    this.opts = opts;
    this.logf = safeLog(opts.log);
    this.deviceId = deviceId;
    this.queue = queue;
    this.randomId = randomId;
    this.url = `${opts.endpoint.replace(/\/+$/, "")}/v1/events`;
    this.allowed = new Set(opts.allowProps ?? []);
    this.now = opts.now ?? Date.now;
    this.startedMs = opts.processStartMs ?? (globalThis.performance?.timeOrigin || loadedAt);
    this.minBackoffMs = opts.minBackoffMs ?? 1_000;
    this.maxBackoffMs = opts.maxBackoffMs ?? 300_000;

    this.flushTimer = setInterval(() => {
      this.queue.sealIdle();
      this.kick();
    }, opts.flushIntervalMs ?? 30_000);
    this.beatTimer = setInterval(() => this.heartbeat(), opts.heartbeatIntervalMs ?? 60_000);
    unref(this.flushTimer);
    unref(this.beatTimer);
    this.kick(); // events left by the previous run
  }

  /** setUserId sets the user_id of later events, "" for none. Only tester builds should call it. */
  setUserId(id: string): void {
    this.userId = id;
  }

  /**
   * track queues an event. It throws if the server would reject it. Report
   * errors with trackError: the dashboard counts only names starting "error.".
   */
  track(name: string, props?: Props): void {
    this.trackEvent(name, undefined, props);
  }

  /**
   * trackError queues error.<area>, counted as an error by the dashboard. A
   * non-empty message, cut to 2 KiB, replaces props.message if allowed.
   */
  trackError(area: string, message: string, props?: Props): void {
    // A message that isn't allowed stays out of props, or every error would log a drop.
    const clean = this.allowed.has("message") ? cleanMessage(message) : "";
    this.trackEvent(`error.${area}`, undefined, withMessage(props, clean));
  }

  /** trackValue queues an event with its one numeric measure, such as a duration in ms. */
  trackValue(name: string, value: number, props?: Props): void {
    this.trackEvent(name, value, props);
  }

  /** appOpened records app.open once per process. Call it when the first screen is usable. */
  appOpened(): void {
    if (this.opened) return;
    this.opened = true;
    this.enqueue("app.open", Math.floor(Math.max(this.now() - this.startedMs, 0)), { cold: true });
  }

  /** startSession begins a session and heartbeats each minute. It does nothing while one is open. */
  startSession(): void {
    if (this.sessionId !== "") return;
    this.sessionId = this.randomId();
    this.sessionStartMs = this.now();
    this.enqueue("session.start");
  }

  /** endSession sends session.end and closes the session. */
  endSession(): void {
    if (this.sessionId === "") return;
    try {
      this.enqueue("session.end", this.sessionSeconds());
    } finally {
      this.sessionId = "";
    }
  }

  /** flush starts sending queued events now, without waiting for them. */
  flush(): void {
    this.queue.seal();
    this.kick();
  }

  /**
   * close ends any open session and makes one last attempt to send, for a few
   * seconds at most. Unsent events stay in the store for the next run.
   */
  close(): Promise<void> {
    this.closing ??= this.shutdown();
    return this.closing;
  }

  private async shutdown(): Promise<void> {
    this.endSession();
    this.closed = true;
    clearInterval(this.flushTimer);
    clearInterval(this.beatTimer);
    this.queue.seal();

    const giveUp = setTimeout(() => {
      this.stopped = true;
      this.inFlight?.abort();
    }, this.opts.closeWaitMs ?? 5_000);
    await this.draining;
    if (!this.waiting && !this.stopped) await this.runDrain();
    clearTimeout(giveUp);
    this.stopped = true;
    if (this.retryTimer !== null) clearTimeout(this.retryTimer);
    await this.queue.sync();
  }

  private trackEvent(name: string, value: number | undefined, rawProps: unknown): void {
    const { props, dropped } = filterProps(rawProps, this.allowed);
    if (dropped.length > 0) this.logf(`${name}: dropped props not allowed: ${dropped.join(", ")}`);
    this.enqueue(name, value, props);
  }

  private heartbeat(): void {
    if (this.sessionId === "" || this.closed) return;
    try {
      this.enqueue("session.heartbeat", this.sessionSeconds());
    } catch (err) {
      this.logf(`heartbeat: ${err}`);
    }
  }

  private sessionSeconds(): number {
    return Math.round((this.now() - this.sessionStartMs) / 1000);
  }

  private enqueue(name: string, value?: number, props?: Props): void {
    if (this.closed) throw new BeaconError("beacon: client closed");
    const e: WireEvent = {
      name,
      ts: formatTime(this.now()),
      device_id: this.deviceId,
      user_id: this.userId || undefined,
      session_id: this.sessionId || undefined,
      app_version: this.opts.appVersion || undefined,
      platform: this.opts.platform || undefined,
      os_version: this.opts.osVersion || undefined,
      value,
      props,
    };
    validate(e);
    if (this.queue.add(JSON.stringify(e))) this.kick();
  }

  private kick(): void {
    if (this.stopped || this.closed) return;
    if (this.draining) {
      this.again = true;
      return;
    }
    if (this.waiting) return;
    void this.runDrain();
  }

  private runDrain(): Promise<void> {
    this.draining = this.drain().then((waitMs) => {
      this.draining = null;
      if (waitMs > 0) {
        this.waiting = true;
        this.again = false;
        this.retryTimer = setTimeout(() => {
          this.waiting = false;
          this.kick();
        }, waitMs);
        unref(this.retryTimer);
      } else if (this.again) {
        this.again = false;
        this.kick();
      }
    });
    return this.draining;
  }

  private nextBackoff(): number {
    return backoffDelay(this.failures++, this.minBackoffMs, this.maxBackoffMs);
  }

  // Sends sealed segments oldest first and returns how long to wait before
  // trying again, 0 when nothing is left.
  private async drain(): Promise<number> {
    while (!this.stopped) {
      const seg = this.queue.oldest();
      if (!seg) return 0;
      let lines: string[];
      let outcome: ReturnType<typeof classify>;
      try {
        lines = await this.queue.read(seg);
        if (lines.length === 0) {
          this.queue.remove(seg);
          continue;
        }
        outcome = await this.send(lines);
      } catch (err) {
        this.logf(`send: ${err}`);
        return this.nextBackoff();
      }
      switch (outcome.action) {
        case "accept":
          this.failures = 0;
          this.queue.remove(seg);
          break;
        case "drop_batch":
          this.failures = 0;
          this.logf(`dropped a batch of ${lines.length} events the server rejected`);
          this.queue.remove(seg);
          break;
        case "drop_event":
          this.failures = 0;
          this.dropEvent(seg, lines, outcome.index ?? -1);
          break;
        case "retry_after":
          return (outcome.waitSeconds ?? 0) * 1000;
        default:
          return this.nextBackoff();
      }
    }
    return 0;
  }

  // The server stores nothing from a rejected batch, so the rest can be sent again.
  private dropEvent(seg: Segment, lines: string[], index: number): void {
    if (index < 0 || index >= lines.length || lines.length === 1) {
      this.logf(`dropped a batch of ${lines.length} events the server rejected`);
      this.queue.remove(seg);
      return;
    }
    this.logf(`dropped event ${index} of a batch the server rejected`);
    this.queue.rewrite(
      seg,
      lines.filter((_, i) => i !== index),
    );
  }

  private async send(lines: string[]): Promise<ReturnType<typeof classify>> {
    const json = `{"events":[${lines.join(",")}]}`;
    const headers: Record<string, string> = {
      Authorization: `Bearer ${this.opts.key}`,
      "Content-Type": "application/json",
    };
    let body: BodyInit = json;
    if (this.opts.gzip) {
      body = (await this.opts.gzip(json)) as BodyInit;
      headers["Content-Encoding"] = "gzip";
    }
    if (this.stopped) throw new BeaconError("beacon: client closing");
    const abort = new AbortController();
    this.inFlight = abort;
    const timeout = setTimeout(() => abort.abort(), requestTimeoutMs);
    try {
      const resp = await fetch(this.url, { method: "POST", headers, body, signal: abort.signal });
      const text = await resp.text();
      if (resp.status !== 202) this.logf(`server returned ${resp.status}: ${text.slice(0, 500)}`);
      return classify(resp.status, resp.headers.get("Retry-After"), text);
    } finally {
      clearTimeout(timeout);
      this.inFlight = null;
    }
  }
}

async function loadDeviceId(store: Store, randomId: () => string): Promise<string> {
  const existing = (await store.get("device_id"))?.trim();
  if (existing) return existing;
  const id = randomId();
  await store.set("device_id", id);
  return id;
}

/** createClient opens the queue and starts sending in the background. */
export async function createClient(opts: ClientOptions): Promise<Client> {
  if (!opts.endpoint || !opts.key || !opts.store) {
    throw new BeaconError("beacon: endpoint, key and store are required");
  }
  // A pattern, because URL is incomplete on React Native.
  if (!/^https?:\/\/[^/\s]+/.test(opts.endpoint)) {
    throw new BeaconError(`beacon: invalid endpoint ${JSON.stringify(opts.endpoint)}`);
  }
  const batchEvents = opts.batchEvents ?? 100;
  if (!Number.isInteger(batchEvents) || batchEvents < 1 || batchEvents > maxBatchEvents) {
    throw new BeaconError(`beacon: batchEvents must be from 1 to ${maxBatchEvents}`);
  }
  const randomId = opts.randomId ?? defaultRandomId;
  const deviceId = await loadDeviceId(opts.store, randomId);
  validate({
    name: "probe",
    device_id: deviceId,
    app_version: opts.appVersion,
    platform: opts.platform,
    os_version: opts.osVersion,
  });
  const log = safeLog(opts.log);
  const queue = await Queue.open({
    store: opts.store,
    maxBytes: opts.maxQueueBytes || 10 << 20,
    maxEvents: batchEvents,
    batchBytes: maxBatchBytes,
    onDrop: (n) => log(`queue full: dropped ${n} oldest events`),
    onError: log,
  });
  return new Client(opts, deviceId, queue, randomId);
}

/** KeyValueStorage is the part of React Native's AsyncStorage the client needs. */
export interface KeyValueStorage {
  getAllKeys(): Promise<readonly string[]>;
  getItem(key: string): Promise<string | null>;
  setItem(key: string, value: string): Promise<void>;
  removeItem(key: string): Promise<void>;
}

/**
 * keyValueStore adapts AsyncStorage. Android limits it to 6 MB by default, so
 * set maxQueueBytes below that.
 */
export function keyValueStore(storage: KeyValueStorage, namespace = "beacon"): Store {
  const prefix = `${namespace}:`;
  return {
    keys: async (p) =>
      (await storage.getAllKeys())
        .filter((k) => k.startsWith(prefix + p))
        .map((k) => k.slice(prefix.length)),
    get: (k) => storage.getItem(prefix + k),
    set: (k, v) => storage.setItem(prefix + k, v),
    remove: (k) => storage.removeItem(prefix + k),
  };
}
