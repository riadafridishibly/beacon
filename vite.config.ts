import { fileURLToPath } from "node:url";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig, type Plugin } from "vite";

// The app in `make dev`: templ's proxy in front of the Go server.
const appURL = "http://localhost:7331";

// This dev server only serves CSS/JS to pages rendered by Go. Print the app
// URL instead of Vite's, and send / to the app.
function pointToApp(): Plugin {
  return {
    name: "point-to-app",
    apply: "serve",
    configureServer(server) {
      server.printUrls = () => {
        server.config.logger.info(
          `  ➜  App:    ${appURL}  ← open this\n` +
            `  ➜  Assets: http://localhost:${server.config.server.port} (Vite, used by the app)\n`,
        );
      };
      server.middlewares.use((req, res, next) => {
        if (req.url !== "/") return next();
        res.writeHead(302, { Location: appURL });
        res.end();
      });
    },
  };
}

// The build goes to internal/web/dist, which the Go binary embeds and serves
// under /static/. In dev, the layout loads /@vite/client and the entries
// straight from this server (internal/web/assets.go).
export default defineConfig(({ command }) => ({
  root: "frontend",
  base: command === "build" ? "/static/" : "/",
  plugins: [tailwindcss(), pointToApp()],
  server: {
    port: 8341,
    strictPort: true,
    origin: "http://localhost:8341",
  },
  build: {
    outDir: "../internal/web/dist",
    emptyOutDir: true,
    manifest: true,
    // The CSP allows fonts only from 'self', not data: URLs.
    assetsInlineLimit: (file) => (file.endsWith(".woff2") ? false : undefined),
    rolldownOptions: {
      // The stylesheet is its own entry so the layout can <link> it in dev
      // too.
      input: ["./frontend/src/main.ts", "./frontend/src/app.css"].map((p) =>
        fileURLToPath(new URL(p, import.meta.url)),
      ),
      onLog(level, log, handler) {
        // htmx only evals hx-on:* / js: expressions, which main.ts disables.
        if (log.code === "EVAL" && log.id?.includes("htmx.org")) return;
        handler(level, log);
      },
    },
  },
}));
