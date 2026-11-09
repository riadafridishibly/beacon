import assert from "node:assert/strict";
import { test } from "node:test";
import { byteLength } from "./event.ts";
import {
  BeaconError,
  createClient,
  type KeyValueStorage,
  keyValueStore,
  type Store,
} from "./index.ts";
import { platformName } from "./node.ts";
import { Queue } from "./queue.ts";
import { backoffDelay } from "./send.ts";
import { decode, names, startServer } from "./testserver.ts";

const until = async (cond: () => boolean) => {
  const deadline = Date.now() + 10_000;
  while (!cond()) {
    assert.ok(Date.now() < deadline, "timed out");
    await new Promise((resolve) => setTimeout(resolve, 2));
  }
};

function memoryStore(): Store & { data: Map<string, string> } {
  const data = new Map<string, string>();
  return {
    data,
    keys: async (p) => [...data.keys()].filter((k) => k.startsWith(p)),
    get: async (k) => data.get(k) ?? null,
    set: async (k, v) => void data.set(k, v),
    remove: async (k) => void data.delete(k),
  };
}

const openQueue = (store: Store, maxBytes: number, maxEvents: number, onDrop = (_: number) => {}) =>
  Queue.open({ store, maxBytes, maxEvents, batchBytes: 900 << 10, onDrop, onError: assert.fail });

test("sends when the batch is full", async (t) => {
  const srv = await startServer(t);
  const c = await createClient({ ...(await srv.options(t)), batchEvents: 3 });
  t.after(() => c.close());
  for (const name of ["a", "b", "c"]) c.track(name);
  assert.equal(names(decode(await srv.wait(1))), "a b c");
});

test("sends on the interval", async (t) => {
  const srv = await startServer(t);
  const c = await createClient({ ...(await srv.options(t)), flushIntervalMs: 5 });
  t.after(() => c.close());
  c.track("a");
  assert.equal(names(decode(await srv.wait(1))), "a");
});

test("retries after a server error", async (t) => {
  const srv = await startServer(t);
  srv.respond = (call) => (call < 3 ? [500, `{"error":"internal error"}`] : null);
  const c = await createClient(await srv.options(t));
  t.after(() => c.close());
  c.track("a");
  c.flush();
  assert.equal(names(decode(await srv.wait(1))), "a");
});

test("drops a bad event and resends the rest", async (t) => {
  const srv = await startServer(t);
  srv.respond = (call) => (call === 0 ? [400, `{"error":"bad","index":1}`] : null);
  const c = await createClient({ ...(await srv.options(t)), batchEvents: 3 });
  t.after(() => c.close());
  for (const name of ["a", "b", "c"]) c.track(name);
  assert.equal(names(decode(await srv.wait(1))), "a c");
});

test("drops a bad batch", async (t) => {
  const srv = await startServer(t);
  srv.respond = (call) => (call === 0 ? [400, `{"error":"invalid JSON"}`] : null);
  const store = memoryStore();
  const c = await createClient({ ...(await srv.options(t)), store });
  t.after(() => c.close());
  c.track("a");
  c.flush();
  await until(() => ![...store.data.keys()].some((k) => k.startsWith("queue.")));
  c.track("b");
  c.flush();
  assert.equal(names(decode(await srv.wait(1))), "b");
});

test("the queue survives a restart", async (t) => {
  const srv = await startServer(t);
  srv.respond = () => [503, ""];
  const opts = await srv.options(t);
  const c = await createClient(opts);
  c.track("a");
  await c.close();
  assert.throws(() => c.track("b"), BeaconError);

  srv.respond = null;
  const again = await createClient(opts);
  t.after(() => again.close());
  assert.equal(names(decode(await srv.wait(1))), "a");
});

test("the device id persists", async (t) => {
  const srv = await startServer(t);
  const opts = await srv.options(t);
  const a = await createClient(opts);
  await a.close();
  const b = await createClient(opts);
  await b.close();
  assert.ok(a.deviceId);
  assert.equal(a.deviceId, b.deviceId);
});

test("session events", async (t) => {
  const srv = await startServer(t);
  let current = Date.UTC(2026, 10, 8, 12);
  const c = await createClient({
    ...(await srv.options(t)),
    heartbeatIntervalMs: 5,
    now: () => (current += 1000),
  });
  c.track("before");
  c.startSession();
  c.startSession();
  await new Promise((resolve) => setTimeout(resolve, 30));
  c.endSession();
  c.track("after");
  await c.close();

  const events = decode(await srv.wait(1));
  assert.match(names(events), /^before session\.start session\.heartbeat.* session\.end after$/);
  const start = events[1];
  const end = events.at(-2);
  assert.equal(events[0]?.session_id, undefined);
  assert.equal(events.at(-1)?.session_id, undefined);
  assert.ok(start?.session_id);
  assert.equal(start.session_id, end?.session_id);
  assert.ok((end?.value ?? 0) > 0);
});

test("appOpened records once", async (t) => {
  const srv = await startServer(t);
  const c = await createClient({
    ...(await srv.options(t)),
    processStartMs: 1000,
    now: () => 1800,
  });
  c.appOpened();
  c.appOpened();
  await c.close();
  const events = decode(await srv.wait(1));
  assert.equal(events.length, 1);
  assert.equal(events[0]?.name, "app.open");
  assert.equal(events[0]?.value, 800);
  assert.deepEqual(events[0]?.props, { cold: true });
});

test("props must be allowed", async (t) => {
  const srv = await startServer(t);
  const logs: string[] = [];
  const c = await createClient({
    ...(await srv.options(t)),
    allowProps: ["code"],
    log: (m) => logs.push(m),
  });
  c.track("error.sync", { code: 42, message: "secret" });
  c.track("x", { message: "secret" });
  assert.throws(() => c.track("x", [1] as never), BeaconError);
  assert.throws(() => c.track("Bad Name"), BeaconError);
  assert.throws(() => c.trackValue("x", Number.NaN), BeaconError);
  await c.close();
  const events = decode(await srv.wait(1));
  assert.deepEqual(events[0]?.props, { code: 42 });
  assert.equal(events[1]?.props, undefined);
  assert.ok(logs.some((m) => m.includes("message")));
});

test("trackError names the event and sets message", async (t) => {
  const srv = await startServer(t);
  const c = await createClient({
    ...(await srv.options(t)),
    allowProps: ["code", "message"],
  });
  c.trackError("engine", "boom", { code: 42, message: "old", other: 1 });
  c.trackError("sync", "");
  c.trackError("auth", "denied", null as never);
  assert.throws(() => c.trackError("", "m"), BeaconError);
  assert.throws(() => c.trackError("Bad Area", "m"), BeaconError);
  assert.throws(() => c.trackError("x", "m", [1] as never), BeaconError);
  assert.throws(() => c.trackError("x", "", [1] as never), BeaconError);
  await c.close();
  const events = decode(await srv.wait(1));
  assert.equal(events.length, 3);
  assert.equal(events[0]?.name, "error.engine");
  assert.deepEqual(events[0]?.props, { code: 42, message: "boom" });
  assert.equal(events[1]?.name, "error.sync");
  assert.equal(events[1]?.props, undefined);
  assert.equal(events[2]?.name, "error.auth");
  assert.deepEqual(events[2]?.props, { message: "denied" });
});

test("trackError cleans the message", async (t) => {
  const srv = await startServer(t);
  const c = await createClient({ ...(await srv.options(t)), allowProps: ["message"] });
  const tests: [string, string][] = [
    ["a\0b", "ab"],
    ["a\ud800b", "a�b"],
    ["€".repeat(5000), "€".repeat(682)],
    ["😀".repeat(1000), "😀".repeat(512)],
    ["\x01".repeat(5000), "\x01".repeat(2048)],
  ];
  for (const [input] of tests) c.trackError("x", input);
  c.trackError("only_nul", "\0", { message: "kept" });
  await c.close();
  const events = decode(await srv.wait(1));
  assert.equal(events.length, tests.length + 1);
  for (const [i, [, want]] of tests.entries()) {
    assert.deepEqual(events[i]?.props, { message: want });
  }
  assert.deepEqual(events[tests.length]?.props, { message: "kept" });
});

test("trackError drops a message that is not allowed", async (t) => {
  const srv = await startServer(t);
  const c = await createClient(await srv.options(t));
  c.trackError("engine", "secret");
  await c.close();
  const events = decode(await srv.wait(1));
  assert.equal(events[0]?.name, "error.engine");
  assert.equal(events[0]?.props, undefined);
});

test("user id is opt in", async (t) => {
  const srv = await startServer(t);
  const c = await createClient(await srv.options(t));
  c.track("a");
  c.setUserId("tester");
  c.track("b");
  await c.close();
  const events = decode(await srv.wait(1));
  assert.equal(events[0]?.user_id, undefined);
  assert.equal(events[1]?.user_id, "tester");
});

test("sends uncompressed without gzip, through a key-value store", async (t) => {
  const srv = await startServer(t);
  const data = new Map<string, string>([["other", "kept"]]);
  const storage: KeyValueStorage = {
    getAllKeys: async () => [...data.keys()],
    getItem: async (k) => data.get(k) ?? null,
    setItem: async (k, v) => void data.set(k, v),
    removeItem: async (k) => void data.delete(k),
  };
  const c = await createClient({
    ...(await srv.options(t)),
    store: keyValueStore(storage),
    gzip: undefined,
    randomId: () => "id",
  });
  c.track("a");
  await c.close();
  assert.equal(names(decode(await srv.wait(1))), "a");
  assert.deepEqual([...data.keys()].sort(), ["beacon:device_id", "other"]);
});

test("createClient rejects bad options", async (t) => {
  const srv = await startServer(t);
  const opts = await srv.options(t);
  for (const bad of [
    { endpoint: "" },
    { endpoint: "ftp://x" },
    { key: "" },
    { appVersion: "a".repeat(65) },
    { batchEvents: 0 },
    { batchEvents: 1001 },
    { batchEvents: 1.5 },
  ]) {
    await assert.rejects(createClient({ ...opts, ...bad }), BeaconError);
  }
});

test("a logger that throws doesn't stop delivery", async (t) => {
  const srv = await startServer(t);
  srv.respond = (call) => (call === 0 ? [500, `{"error":"internal error"}`] : null);
  const c = await createClient({
    ...(await srv.options(t)),
    log: () => {
      throw new Error("broken logger");
    },
  });
  c.track("a");
  c.flush();
  assert.equal(names(decode(await srv.wait(1))), "a");
  await c.close();
});

test("close doesn't send once its time is up", async (t) => {
  const srv = await startServer(t);
  let requests = 0;
  srv.respond = () => {
    requests++;
    return null;
  };
  const store = memoryStore();
  const c = await createClient({
    ...(await srv.options(t)),
    store,
    closeWaitMs: 1,
    gzip: async (data) => {
      await new Promise((resolve) => setTimeout(resolve, 30));
      return Buffer.from(data);
    },
  });
  c.track("a");
  await c.close();
  assert.equal(requests, 0);
  assert.ok([...store.data.keys()].some((k) => k.startsWith("queue.")));
});

test("platform names match the Go client", () => {
  assert.equal(platformName("darwin"), "macos");
  assert.equal(platformName("win32"), "windows");
  assert.equal(platformName("linux"), "linux");
});

test("a saved segment leaves memory when sealed", async () => {
  const store = memoryStore();
  const q = await openQueue(store, 1 << 20, 10);
  q.add(`{"name":"a"}`);
  await q.sync();
  q.seal();
  const seg = q.oldest();
  assert.ok(seg);
  assert.equal(seg.lines, undefined);
  assert.deepEqual(await q.read(seg), [`{"name":"a"}`]);
});

test("an unsaved segment stays in memory when sealed", async () => {
  const store = memoryStore();
  store.set = async () => {
    throw new Error("disk full");
  };
  const q = await Queue.open({
    store,
    maxBytes: 1 << 20,
    maxEvents: 10,
    batchBytes: 900 << 10,
    onDrop: () => {},
    onError: () => {},
  });
  q.add(`{"name":"a"}`);
  await q.sync();
  q.seal();
  const seg = q.oldest();
  assert.ok(seg);
  assert.deepEqual(await q.read(seg), [`{"name":"a"}`]);
});

test("queue drops the oldest past the cap", async () => {
  let dropped = 0;
  const q = await openQueue(memoryStore(), 100, 2, (n) => (dropped += n));
  for (let i = 0; i < 20; i++) q.add(JSON.stringify({ name: `e${String(i).padStart(2, "0")}` }));
  await q.sync();
  assert.ok(dropped > 0);
  assert.ok(q.totalBytes <= 100, `${q.totalBytes} bytes`);
  const oldest = q.oldest();
  assert.ok(oldest);
  assert.ok(!(await q.read(oldest))[0]?.includes("e00"));
});

test("queue skips a torn line", async () => {
  const store = memoryStore();
  store.data.set("queue.00000000000000000000", `{"name":"a"}\n{"name":"b`);
  const q = await openQueue(store, 1 << 20, 10);
  const seg = q.oldest();
  assert.ok(seg);
  assert.deepEqual(await q.read(seg), [`{"name":"a"}`]);
});

test("queue reopens oldest first and continues numbering", async () => {
  const store = memoryStore();
  const q = await openQueue(store, 1 << 20, 1);
  q.add(`{"name":"a"}`);
  q.add(`{"name":"b"}`);
  await q.sync();
  const reopened = await openQueue(store, 1 << 20, 1);
  assert.equal(reopened.segments.length, 2);
  const seg = reopened.oldest();
  assert.ok(seg);
  assert.deepEqual(await reopened.read(seg), [`{"name":"a"}`]);
  reopened.add(`{"name":"c"}`);
  await reopened.sync();
  assert.equal(store.data.size, 3);
});

test("a batch fits the body limit", async () => {
  const q = await openQueue(memoryStore(), 100 << 20, 100);
  const big = JSON.stringify({ name: "x", props: { p: "a".repeat(16 << 10) } });
  for (let i = 0; i < 300; i++) q.add(big);
  await q.sync();
  assert.ok(q.segments.length >= 2);
  for (const seg of q.segments) assert.ok(seg.size <= 900 << 10, `${seg.size} bytes`);
});

test("backoff grows and caps", () => {
  for (const [failures, limit] of [
    [0, 1],
    [1, 2],
    [2, 4],
    [3, 8],
    [4, 8],
    [2000, 8],
  ] as const) {
    const d = backoffDelay(failures, 1000, 8000);
    assert.ok(d >= (limit * 1000) / 2 && d <= limit * 1000, `failures ${failures}: ${d}`);
  }
});

test("byteLength counts UTF-8", () => {
  for (const s of ["a", "é", "€", "😀", "a€😀é"]) {
    assert.equal(byteLength(s), Buffer.byteLength(s), s);
  }
});
