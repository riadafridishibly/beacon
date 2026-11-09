// These limits match internal/ingest. client/conformance/events.json is run
// against both.
const maxNameLen = 64;
const maxIdLen = 128;
const maxUserLen = 256;
const maxAttrLen = 64;
const maxProps = 16 << 10;

const namePattern = /^[a-z0-9_]+(\.[a-z0-9_]+)*$/;

export type Props = Record<string, unknown>;

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
  if (e.value !== undefined && !Number.isFinite(e.value)) {
    throw new BeaconError("beacon: value must be a finite number");
  }
  if (e.props !== undefined && byteLength(JSON.stringify(e.props)) > maxProps) {
    throw new BeaconError(`beacon: props must be at most ${maxProps} bytes`);
  }
}

function over(s: string | undefined, limit: number): boolean {
  return s !== undefined && byteLength(s) > limit;
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
    if (allowed.has(k)) kept[k] = v;
    else dropped.push(k);
  }
  return Object.keys(kept).length > 0 ? { props: kept, dropped } : { dropped };
}
