# Copycord admin web app

## Loading skeletons

Every page shows a skeleton while its code chunk and its first API answer load. The rules:

- The skeleton is the page's own structure. Its real static text, headings, labels, inputs, buttons, tabs, toolbar controls, footer text and promo cards render in the skeleton exactly as on the loaded page, as inert chrome (disabled or not focusable, no handlers).
- Shimmer bars appear only where data from the API lands: names, counts, values, lists and statuses that come from `/api/*`. Buttons and other elements that do not come from the API never get a shimmer.
- The skeleton and the loaded page share their classes (exported constants or shared components), so the two cannot drift apart.
- When the data lands there is zero layout shift: each bar has the box of the value that replaces it, and every static element sits where it will stay.
- The page's own loading state (after its chunk loads, before the API answers) uses the same markup as its skeleton.
- Refetches keep the rows on screen. After the first load a page never goes back to shimmer; it can mark itself busy (for example `aria-busy`) while it refreshes.
