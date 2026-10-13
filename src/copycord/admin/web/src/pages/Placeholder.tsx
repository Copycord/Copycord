// Stand-in for a page that has not moved to the SPA yet.

export function Placeholder({ title, legacyHref }: { title: string; legacyHref: string }) {
  return (
    <section className="rounded-card border border-border bg-card p-6 shadow-card">
      <h1 className="mb-2 text-2xl font-bold tracking-wide">{title}</h1>
      <p className="text-muted">
        This page is not in the new dashboard yet. Use the <a href={legacyHref}>current page</a>.
      </p>
    </section>
  );
}
