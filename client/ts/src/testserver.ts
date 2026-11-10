import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { TestContext } from "node:test";
import { gunzipSync } from "node:zlib";
import type { ClientOptions } from "./index.ts";
import { nodeOptions } from "./node.ts";

/** Received is an event as the test server decodes it. */
export interface Received {
  name: string;
  event_id?: string;
  ts?: string;
  device_id?: string;
  user_id?: string;
  session_id?: string;
  value?: number;
  props?: unknown;
}

type Respond = (call: number, body: string) => [number, string] | null;

/** TestServer records the bodies it accepts. */
export class TestServer {
  readonly bodies: string[] = [];
  respond: Respond | null = null;
  url = "";
  private calls = 0;
  private readonly server = createServer((req, res) => void this.handle(req, res));

  async start(t: TestContext): Promise<void> {
    await new Promise<void>((resolve) => this.server.listen(0, "127.0.0.1", resolve));
    this.url = `http://127.0.0.1:${(this.server.address() as AddressInfo).port}/`;
    t.after(() => {
      this.server.closeAllConnections();
      this.server.close();
    });
  }

  private async handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(chunk as Buffer);
    const raw = Buffer.concat(chunks);
    const gzipped = req.headers["content-encoding"] === "gzip";
    if (
      req.url !== "/v1/events" ||
      req.method !== "POST" ||
      req.headers.authorization !== "Bearer key" ||
      req.headers["content-type"] !== "application/json"
    ) {
      res.writeHead(418).end(`{"error":"bad request shape"}`);
      return;
    }
    const body = (gzipped ? gunzipSync(raw) : raw).toString();
    const [status, reply] = this.respond?.(this.calls++, body) ?? [202, `{"accepted":1}`];
    if (status === 202) this.bodies.push(body);
    res.writeHead(status).end(reply);
  }

  async wait(n: number): Promise<string[]> {
    const deadline = Date.now() + 10_000;
    while (this.bodies.length < n) {
      assert.ok(Date.now() < deadline, `got ${this.bodies.length} bodies, want ${n}`);
      await new Promise((resolve) => setTimeout(resolve, 2));
    }
    return this.bodies;
  }

  async options(t: TestContext): Promise<ClientOptions> {
    const dir = await mkdtemp(join(tmpdir(), "beacon-"));
    t.after(() => rm(dir, { recursive: true, force: true }));
    return {
      ...nodeOptions(dir),
      endpoint: this.url,
      key: "key",
      flushIntervalMs: 3_600_000,
      heartbeatIntervalMs: 3_600_000,
      minBackoffMs: 1,
      maxBackoffMs: 5,
    };
  }
}

/** startServer starts a TestServer that stops with the test. */
export async function startServer(t: TestContext): Promise<TestServer> {
  const s = new TestServer();
  await s.start(t);
  return s;
}

/** decode returns the events of the accepted bodies. */
export function decode(bodies: string[]): Received[] {
  return bodies.flatMap((b) => (JSON.parse(b) as { events: Received[] }).events);
}

/** names joins event names for comparison. */
export function names(events: Received[]): string {
  return events.map((e) => e.name).join(" ");
}
