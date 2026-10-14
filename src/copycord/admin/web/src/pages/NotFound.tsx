// Unknown route: the old 404 page message, inside the normal shell.

import { Link } from "react-router";

export function NotFound() {
  return (
    <section className="mx-auto my-10 max-w-xl rounded-card border border-border bg-card p-6 text-center shadow-card">
      <h2 className="mb-4 text-2xl font-bold tracking-wide">404 – Page not found</h2>
      <img
        src="/static/logo.png"
        alt="Copycord logo"
        draggable={false}
        className="mx-auto mt-4 w-24 opacity-90"
      />
      <div className="mt-6 flex justify-center">
        <Link
          to="/"
          className="rounded-control border border-border px-4 py-2 text-sm text-muted no-underline hover:text-text"
        >
          Back to dashboard
        </Link>
      </div>
    </section>
  );
}
