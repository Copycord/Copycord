// What a page skeleton has to share with its page: the headings, every control outside the list rows
// (inputs, selects, buttons, links, by their accessible label), the column count of each table and the
// pager. Rows are left out here and compared cell by cell in each page's skeleton test.

export const DATA = "<data>";

/** The visible text of el: a shimmer counts as DATA, aria-hidden glyphs (carets, icons) are skipped. */
function text(el: Element): string {
  let out = "";
  for (const n of Array.from(el.childNodes)) {
    if (n.nodeType === Node.TEXT_NODE) out += n.textContent ?? "";
    else if (n instanceof Element) {
      if (n.hasAttribute("data-skeleton")) out += DATA;
      else if (n.getAttribute("aria-hidden") !== "true") out += text(n);
    }
  }
  return out.replace(/\s+/g, " ").trim();
}

/** A control's name. A dropdown toggle (ToolbarSelect) reads as "select label: chosen label"; its native select is aria-hidden. */
function label(el: Element): string {
  const native = el.getAttribute("aria-haspopup") === "listbox" ? el.previousElementSibling : null;
  if (native?.tagName === "SELECT") return `${native.getAttribute("aria-label") ?? ""}: ${text(el)}`;
  return el.getAttribute("aria-label") ?? el.getAttribute("placeholder") ?? text(el);
}

export interface Structure {
  headings: string[];
  controls: string[];
  columns: number[];
  pagers: number;
}

/**
 * rows: a selector matching the list rows of both states (loaded rows and skeleton rows).
 * data: text that comes from the API on the loaded page (a chosen mapping, a count); it compares as DATA.
 */
export function structure(root: Element, rows?: string, data: string[] = []): Structure {
  // Hidden parts (a collapsed panel, an empty state waiting for its turn) are not on screen in either state.
  const keep = (el: Element) => !el.closest("[hidden]") && (!rows || !el.closest(rows));
  const all = (sel: string) => Array.from(root.querySelectorAll(sel)).filter(keep);
  return {
    headings: all("h1, h2, h3").map((h) => `${h.tagName.toLowerCase()} ${text(h)}`),
    controls: all("input, select:not([aria-hidden=true]), textarea, button, a").map((c) => {
      const type = c.getAttribute("type");
      const name = data.reduce((s, d) => s.split(d).join(DATA), label(c));
      return `${c.tagName.toLowerCase()}${type && c.tagName !== "BUTTON" ? `:${type}` : ""} ${name}`;
    }),
    columns: all("table").map((t) => t.querySelectorAll("thead th").length),
    pagers: all("nav[aria-label=Pagination]").length,
  };
}

/** The classes of el without the old hook classes (ch-card, sc-token-item, ...) that only real rows carry. */
export function shared(el: Element, hooks: string[] = []): string {
  return (el.getAttribute("class") ?? "")
    .split(/\s+/)
    .filter((c) => c && !hooks.includes(c))
    .join(" ");
}

/** Each direct child as "tag classes": a skeleton row has the cells of a real row when these match. */
export const cells = (row: Element, hooks: string[] = []) =>
  Array.from(row.children).map((c) => `${c.tagName.toLowerCase()} ${shared(c, hooks)}`);
