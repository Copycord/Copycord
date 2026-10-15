# Copycord admin web app

## Loading skeletons

Every page shows a skeleton while its code chunk and its first API answer load. The rules:

- The skeleton is the page's own structure. Its real static text, headings, labels, inputs, buttons, tabs, toolbar controls, footer text and promo cards render in the skeleton exactly as on the loaded page, as inert chrome (disabled or not focusable, no handlers).
- Shimmer bars appear only where data from the API lands: names, counts, values, lists and statuses that come from `/api/*`. Buttons and other elements that do not come from the API never get a shimmer.
- The skeleton and the loaded page share their classes (exported constants or shared components), so the two cannot drift apart.
- When the data lands there is zero layout shift: each bar has the box of the value that replaces it, and every static element sits where it will stay.
- The page's own loading state (after its chunk loads, before the API answers) uses the same markup as its skeleton.
- Refetches keep the rows on screen. After the first load a page never goes back to shimmer; it can mark itself busy (for example `aria-busy`) while it refreshes.
- A loading row is built from the real row's classes and has its cells: the same columns, paddings and text boxes, with `Skeleton.Text` (a bar in the text's own line box) where each value goes. Controls that are always on the page (search boxes, filters, pagers, page buttons) render from the first paint, never behind a data or status condition. A list draws as many loading rows as it had on its last load (`components/ui/lastCount.ts`), so the rows land in place. Each page has a `*Skeleton.test.tsx` that renders the skeleton, the page's loading state and the loaded page and checks they share headings, controls, columns, pager and row cells (`test/structure.ts`).

Refetching works stale-while-revalidate. The skeleton shows on the first mount only. When a filter, search, mapping or page changes, `useApi` keeps the previous answer on screen while the new one loads and aborts any request it supersedes; the list marks itself `aria-busy` and an empty state shows only once a fetch has settled with no rows. Search boxes that query the server wait for a pause in typing (Logs, 300 ms) before they fetch; searches that filter rows already on the page (Channels, Guilds, Forwarding, mappings) send no request at all.
