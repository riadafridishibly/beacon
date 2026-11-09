const maxRetryAfterSeconds = 3600;

export type Action = "accept" | "drop_event" | "drop_batch" | "retry_after" | "backoff";

export interface Outcome {
  action: Action;
  index?: number;
  waitSeconds?: number;
}

/**
 * classify maps a response to an outcome. Events are dropped only on a 400 in
 * the server's own error format, so that an error page from a proxy can't
 * destroy the queue. client/conformance/responses.json is the table every
 * client must follow.
 */
export function classify(status: number, retryAfter: string | null, body: string): Outcome {
  if (status === 202) return { action: "accept" };
  if (status === 400) return classifyBadRequest(body);
  if (status === 429 && retryAfter !== null && /^\d+$/.test(retryAfter) && Number(retryAfter) > 0) {
    return {
      action: "retry_after",
      waitSeconds: Math.min(Number(retryAfter), maxRetryAfterSeconds),
    };
  }
  return { action: "backoff" };
}

function classifyBadRequest(body: string): Outcome {
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch {
    return { action: "backoff" };
  }
  if (typeof parsed !== "object" || parsed === null) return { action: "backoff" };
  const { error, index } = parsed as { error?: unknown; index?: unknown };
  if (typeof error !== "string" || error === "") return { action: "backoff" };
  if (index === undefined || index === null) return { action: "drop_batch" };
  if (!Number.isInteger(index)) return { action: "backoff" };
  return { action: "drop_event", index: index as number };
}

/** backoffDelay doubles up to maxMs, with jitter so a fleet that lost the server doesn't return together. */
export function backoffDelay(failures: number, minMs: number, maxMs: number): number {
  const d = Math.min(maxMs, minMs * 2 ** failures);
  return d / 2 + Math.random() * (d / 2);
}
