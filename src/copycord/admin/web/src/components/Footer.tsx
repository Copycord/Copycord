// Page footer from the old pages (#fixed-footer): a centered pill at the end of the page flow with the
// Copycord name, the version and GitHub, Discord and donate links.

import { PROMO_LINKS } from "../pages/dashboard/config/model";
import { DISCORD_PATH } from "./icons";

// #fixed-footer .footer-link
const LINK_BASE =
  "inline-flex items-center justify-center text-accent opacity-90 transition duration-200 hover:-translate-y-px hover:opacity-100";
const LINK = `${LINK_BASE} size-7`;
// The old version link reused the 28 px icon box, so a long version ran into "Copycord"; it sizes to its text here.
const VERSION_LINK = `${LINK_BASE} h-7`;

const GITHUB_PATH =
  "M12,2A10,10 0 0,0 2,12c0,4.42 2.87,8.16 6.84,9.49.5.09.68-.22.68-.48 0-.24-.01-.87-.01-1.71-2.78.6-3.37-1.34-3.37-1.34-.45-1.16-1.11-1.47-1.11-1.47-.91-.62.07-.61.07-.61 1.01.07 1.54 1.04 1.54 1.04.9 1.53 2.36 1.09 2.94.83.09-.65.35-1.09.63-1.34-2.22-.25-4.56-1.11-4.56-4.95 0-1.09.39-1.98 1.03-2.68-.1-.25-.45-1.27.1-2.65 0 0 .84-.27 2.75 1.02A9.56 9.56 0 0 1 12 6.8c.85 0 1.71.11 2.51.33 1.9-1.29 2.74-1.02 2.74-1.02.55 1.38.2 2.4.1 2.65.64.7 1.03 1.59 1.03 2.68 0 3.85-2.34 4.7-4.57 4.95.36.31.68.92.68 1.85 0 1.34-.01 2.42-.01 2.75 0 .26.18.58.69.48A10 10 0 0 0 22 12 10 10 0 0 0 12 2Z";

export function Footer({ version }: { version: string | null }) {
  const shown = version ?? "dev";
  return (
    <footer id="fixed-footer" role="contentinfo" aria-label="Site footer" className="relative mt-auto w-full text-center">
      <div className="inline-flex items-center gap-4 rounded-full px-3.5 py-2 text-text">
        <div className="inline-flex items-center gap-3.5 text-sm font-semibold" aria-label={`Copycord version ${shown}`}>
          <span className="font-semibold">Copycord</span>
          <bdi className="tabular-nums opacity-85">
            {version ? (
              <a
                id="footer-version-link"
                className={VERSION_LINK}
                href={`${PROMO_LINKS.github}/releases/tag/${version}`}
                target="_blank"
                rel="noopener noreferrer"
              >
                {version}
              </a>
            ) : (
              <span id="footer-version-text">dev</span>
            )}
          </bdi>
        </div>
        <div className="inline-flex items-center gap-2.5" role="group" aria-label="Links">
          <a className={LINK} href={PROMO_LINKS.github} target="_blank" rel="noopener noreferrer" aria-label="GitHub">
            <svg viewBox="0 0 24 24" className="size-[18px]" fill="currentColor" aria-hidden="true">
              <path d={GITHUB_PATH} />
            </svg>
          </a>
          <a className={LINK} href={PROMO_LINKS.discord} target="_blank" rel="noopener noreferrer" aria-label="Join our Discord">
            <svg viewBox="0 0 127.14 96.36" className="size-[18px]" fill="currentColor" aria-hidden="true">
              <path d={DISCORD_PATH} />
            </svg>
          </a>
          <a className={LINK} href={PROMO_LINKS.kofi} target="_blank" rel="noopener noreferrer" aria-label="Buy me a coffee">
            <img
              src="https://storage.ko-fi.com/cdn/brandasset/v2/kofi_symbol.png"
              alt=""
              width={18}
              height={18}
              loading="lazy"
              className="size-[18px]"
            />
          </a>
        </div>
      </div>
    </footer>
  );
}
