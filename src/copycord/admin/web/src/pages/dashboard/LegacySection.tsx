// Stand in for a dashboard card that is not ported yet: its title and a link
// to the same card on the old page. Each section file swaps this out when
// its lane lands.

export interface LegacySectionProps {
  title: string;
  /** Id of the card on the old index.html. */
  anchor: string;
}

export function LegacySection({ title, anchor }: LegacySectionProps) {
  return (
    <section
      data-legacy-section={anchor}
      className="flex items-center justify-between gap-3 rounded-card border border-border bg-linear-to-b from-card to-[rgba(20,10,40,0.45)] px-[18px] py-3 text-text"
    >
      <h2 className="text-base font-semibold">{title}</h2>
      <a href={`/#${anchor}`} className="text-sm text-accent hover:underline">
        Open on the old page
      </a>
    </section>
  );
}
