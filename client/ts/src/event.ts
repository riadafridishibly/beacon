// These limits match internal/ingest. client/conformance/events.json is run
// against both.
const maxNameLen = 64;
const maxIdLen = 128;
const maxUserLen = 256;
const maxAttrLen = 64;
const maxProps = 16 << 10;

// Keeps an error message within maxProps even if JSON escaping grows every
// byte to six.
const maxMessage = 2 << 10;

const namePattern = /^[a-z0-9_]+(\.[a-z0-9_]+)*$/;

/** Props is the JSON object an event carries beside its one numeric value. A top-level Error is sent as its message. */
export type Props = Record<string, unknown>;

/** WireEvent is one event as the server receives it. */
export interface WireEvent {
  name: string;
  ts?: string;
  device_id?: string;
  user_id?: string;
  session_id?: string;
  app_version?: string;
  platform?: string;
  os_version?: string;
  value?: number;
  props?: Props;
}

/** BeaconError is thrown for an event or configuration the client refuses. */
export class BeaconError extends Error {
  override name = "BeaconError";
}

/** byteLength returns the UTF-8 size of s. TextEncoder is missing on older React Native. */
export function byteLength(s: string): number {
  let n = 0;
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (c < 0x80) n += 1;
    else if (c < 0x800) n += 2;
    else if (c >= 0xd800 && c < 0xdc00 && i + 1 < s.length) {
      n += 4;
      i++;
    } else n += 3;
  }
  return n;
}

/** validate applies the server's rules, so that an event it would reject never reaches the queue. */
export function validate(e: WireEvent): void {
  if (!namePattern.test(e.name) || e.name.length > maxNameLen) {
    throw new BeaconError(
      `beacon: name ${JSON.stringify(e.name)} must be at most ${maxNameLen} lowercase letters, digits and underscores, in dot-separated parts`,
    );
  }
  if (!e.device_id) throw new BeaconError("beacon: device_id is required");
  if (over(e.device_id, maxIdLen) || over(e.session_id, maxIdLen)) {
    throw new BeaconError(`beacon: device_id and session_id must be at most ${maxIdLen} bytes`);
  }
  if (over(e.user_id, maxUserLen)) {
    throw new BeaconError(`beacon: user_id must be at most ${maxUserLen} bytes`);
  }
  if (
    over(e.app_version, maxAttrLen) ||
    over(e.platform, maxAttrLen) ||
    over(e.os_version, maxAttrLen)
  ) {
    throw new BeaconError(
      `beacon: app_version, platform and os_version must be at most ${maxAttrLen} bytes`,
    );
  }
  if (
    [e.device_id, e.user_id, e.session_id, e.app_version, e.platform, e.os_version].some(hasNul)
  ) {
    throw new BeaconError(
      "beacon: device_id, user_id, session_id, app_version, platform and os_version must not contain NUL",
    );
  }
  if (e.value !== undefined && !Number.isFinite(e.value)) {
    throw new BeaconError("beacon: value must be a finite number");
  }
  if (e.props !== undefined) {
    const json = JSON.stringify(e.props);
    if (byteLength(json) > maxProps) {
      throw new BeaconError(`beacon: props must be at most ${maxProps} bytes`);
    }
    if (!storableJson(json)) {
      throw new BeaconError("beacon: props must not contain NUL or unpaired surrogates");
    }
  }
}

function hasNul(s: string | undefined): boolean {
  return s?.includes("\0") ?? false;
}

// jsonb refuses \u0000 and unpaired surrogates. JSON.stringify writes both as
// escapes.
function storableJson(json: string): boolean {
  for (let i = 0; i + 1 < json.length; i++) {
    if (json[i] !== "\\") continue;
    i++;
    if (json[i] !== "u") continue;
    const r = Number.parseInt(json.slice(i + 1, i + 5), 16);
    i += 4;
    if (r === 0 || (r >= 0xdc00 && r <= 0xdfff)) return false;
    if (r >= 0xd800 && r <= 0xdbff) {
      if (json[i + 1] !== "\\" || json[i + 2] !== "u") return false;
      const low = Number.parseInt(json.slice(i + 3, i + 7), 16);
      if (!(low >= 0xdc00 && low <= 0xdfff)) return false;
      i += 6;
    }
  }
  return true;
}

function over(s: string | undefined, limit: number): boolean {
  return s !== undefined && byteLength(s) > limit;
}

/** cleanMessage makes error text safe to store, so that a long or malformed message can't cost the event. */
export function cleanMessage(message: unknown): string {
  const text = message instanceof Error ? message.message : String(message ?? "");
  let out = "";
  let size = 0;
  for (const ch of text) {
    if (ch === "\0") continue;
    const c = ch.length === 1 && ch >= "\ud800" && ch <= "\udfff" ? "�" : ch;
    size += byteLength(c);
    if (size > maxMessage) break;
    out += c;
  }
  return out;
}

/**
 * withMessage returns props with its message key set, unless message is empty.
 * Props that filterProps refuses are returned for it to report.
 */
export function withMessage(props: unknown, message: string): unknown {
  const isObject = typeof props === "object" && !Array.isArray(props);
  if (message === "" || !(isObject || props === undefined)) return props;
  return { ...(props as Props | null | undefined), message };
}

/** filterProps keeps the allowed keys of an object and returns the names of the others. */
export function filterProps(
  props: unknown,
  allowed: ReadonlySet<string>,
): { props?: Props; dropped: string[] } {
  if (props === undefined || props === null) return { dropped: [] };
  if (typeof props !== "object" || Array.isArray(props)) {
    throw new BeaconError("beacon: props must be an object");
  }
  const kept: Props = {};
  const dropped: string[] = [];
  for (const [k, v] of Object.entries(props)) {
    if (allowed.has(k)) kept[k] = v instanceof Error ? cleanMessage(v) : v;
    else dropped.push(k);
  }
  return Object.keys(kept).length > 0 ? { props: kept, dropped } : { dropped };
}
