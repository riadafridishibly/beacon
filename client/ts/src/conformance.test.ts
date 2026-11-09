import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { filterProps, validate, type WireEvent } from "./event.ts";
import { createClient } from "./index.ts";
import { type Action, classify } from "./send.ts";
import { decode, startServer } from "./testserver.ts";

function fixture<T>(name: string): T {
  return JSON.parse(
    readFileSync(new URL(`../../conformance/${name}`, import.meta.url), "utf8"),
  ) as T;
}

test("conformance: events", () => {
  const cases =
    fixture<{ name: string; event: WireEvent; props_size?: number; valid: boolean }[]>(
      "events.json",
    );
  const allowed = new Set(["p", "p\0", "cold"]);
  for (const c of cases) {
    const raw = c.props_size ? { p: "a".repeat(c.props_size - `{"p":""}`.length) } : c.event.props;
    let valid = true;
    let message = "";
    try {
      validate({ ...c.event, props: filterProps(raw, allowed).props });
    } catch (err) {
      valid = false;
      message = String(err);
    }
    assert.equal(valid, c.valid, `${c.name}: ${message}`);
  }
});

test("conformance: responses", () => {
  const cases =
    fixture<
      {
        name: string;
        status: number;
        retry_after?: string;
        body: string;
        action: Action;
        index?: number;
        wait_seconds?: number;
      }[]
    >("responses.json");
  for (const c of cases) {
    const got = classify(c.status, c.retry_after ?? null, c.body);
    assert.equal(got.action, c.action, c.name);
    if (c.action === "drop_event") assert.equal(got.index, c.index ?? 0, c.name);
    if (c.action === "retry_after") assert.equal(got.waitSeconds, c.wait_seconds, c.name);
  }
});

test("conformance: batch body", async (t) => {
  const srv = await startServer(t);
  const c = await createClient({
    ...(await srv.options(t)),
    appVersion: "1.4.0",
    platform: "macos",
    osVersion: "15.1",
    randomId: () => "fixed-id",
    now: () => Date.UTC(2026, 10, 8, 11, 59, 0),
    allowProps: ["cold"],
  });
  c.setUserId("tester@example.com");
  c.startSession();
  c.trackValue("sync.done", 812, { cold: true });
  await c.close();
  assert.deepEqual(decode(await srv.wait(1)), fixture<{ events: unknown[] }>("batch.json").events);
});
