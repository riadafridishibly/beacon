import { mkdir, readdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { promisify } from "node:util";
import { gzip } from "node:zlib";
import type { ClientOptions, Store } from "./index.ts";

const gzipAsync = promisify(gzip);

/** fileStore keeps each value in a file in dir. */
export function fileStore(dir: string): Store {
  const ready = mkdir(dir, { recursive: true, mode: 0o700 });
  return {
    async keys(prefix) {
      await ready;
      const names = await readdir(dir);
      await Promise.all(
        names.filter((n) => n.endsWith(".tmp")).map((n) => rm(join(dir, n), { force: true })),
      );
      return names.filter((n) => n.startsWith(prefix) && !n.endsWith(".tmp"));
    },
    async get(key) {
      await ready;
      try {
        return await readFile(join(dir, key), "utf8");
      } catch (err) {
        if ((err as NodeJS.ErrnoException).code === "ENOENT") return null;
        throw err;
      }
    },
    async set(key, value) {
      await ready;
      const path = join(dir, key);
      await writeFile(`${path}.tmp`, value, { mode: 0o600 });
      await rename(`${path}.tmp`, path);
    },
    async remove(key) {
      await ready;
      await rm(join(dir, key), { force: true });
    },
  };
}

/** platformName returns the name the Go client sends for the same system. */
export function platformName(platform: string): string {
  if (platform === "darwin") return "macos";
  if (platform === "win32") return "windows";
  return platform;
}

/** nodeOptions gives the platform options for Node and Electron's main process. */
export function nodeOptions(
  dir: string,
): Pick<ClientOptions, "store" | "gzip" | "platform" | "processStartMs"> {
  return {
    store: fileStore(dir),
    gzip: (data) => gzipAsync(data),
    platform: platformName(process.platform),
    processStartMs: performance.timeOrigin,
  };
}
