// Adds the CSRF header the admin server requires on state-changing requests.
// Imported first by main.js so every fetch() on every page goes through it.
(() => {
  const HEADER = "X-Copycord-CSRF";
  const SAFE = new Set(["GET", "HEAD", "OPTIONS", "TRACE"]);
  const nativeFetch = window.fetch.bind(window);

  window.fetch = (input, init) => {
    const isRequest = typeof Request !== "undefined" && input instanceof Request;
    const method = String(
      (init && init.method) || (isRequest ? input.method : "GET")
    ).toUpperCase();
    const url = new URL(isRequest ? input.url : String(input), location.href);
    if (SAFE.has(method) || url.origin !== location.origin) {
      return nativeFetch(input, init);
    }
    const headers = new Headers(
      (init && init.headers) || (isRequest ? input.headers : undefined)
    );
    headers.set(HEADER, "1");
    return nativeFetch(input, { ...(init || {}), headers });
  };
})();
