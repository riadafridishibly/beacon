import htmx from "htmx.org";

// Don't import app.css here. In dev, Vite would inject it with JS after the
// first paint, flashing unstyled HTML. The layout links it instead
// (internal/web/assets.go).

// The CSP forbids inline styles and eval: .htmx-indicator styles live in
// app.css, and hx-on:* / js: expressions are disabled.
htmx.config.includeIndicatorStyles = false;
htmx.config.allowEval = false;
// Handlers return fragments for HX-Request; a history restore must get the full page.
htmx.config.historyRestoreAsHxRequest = false;
// Swap 422 so handlers can re-render a form with its validation errors.
htmx.config.responseHandling = [
  { code: "204", swap: false },
  { code: "[23]..", swap: true },
  { code: "422", swap: true },
  { code: "[45]..", swap: false, error: true },
];

const requestError = () => document.getElementById("request-error");
document.addEventListener("htmx:beforeRequest", () => {
  requestError()?.setAttribute("hidden", "");
});
for (const event of ["htmx:responseError", "htmx:sendError"]) {
  document.addEventListener(event, () => requestError()?.removeAttribute("hidden"));
}
