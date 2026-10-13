// Proxies card: the old hint linking to the Configuration page, plus a
// collapsible editor for the saved list (GET and PUT /api/scraper/proxies).

import { useEffect, useState } from "react";
import { Button, Card, ChipInput, cx } from "../../components/ui";
import type { Notify } from "../system/providers";
import { getProxies, reason, saveProxies } from "./scraperApi";
import { TITLE } from "./TokensCard";

export function ProxyCard({ notify }: { notify: Notify }) {
  const [open, setOpen] = useState(false);
  const [proxies, setProxies] = useState<string[]>([]);
  const [saved, setSaved] = useState<string[]>([]);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    let live = true;
    void getProxies().then((list) => {
      if (!live) return;
      setProxies(list);
      setSaved(list);
    });
    return () => {
      live = false;
    };
  }, []);

  const dirty = proxies.join("\n") !== saved.join("\n");

  async function onSave() {
    setSaving(true);
    try {
      const j = await saveProxies(proxies);
      if (!j?.ok) throw new Error(j?.error || "Failed to save proxies");
      const next = j.proxies ?? proxies;
      setProxies(next);
      setSaved(next);
      notify("Proxies saved", "success");
    } catch (e) {
      notify(reason(e, "Failed to save proxies"), "error");
    } finally {
      setSaving(false);
    }
  }

  return (
    <Card id="sc-proxy-card" aria-label="Proxies">
      <div className="mb-2 flex items-center justify-between gap-3">
        <h3 className={TITLE}>Proxies</h3>
        <button
          id="sc-proxy-toggle"
          type="button"
          aria-controls="sc-proxy-panel"
          aria-expanded={open}
          aria-label={open ? "Collapse proxies" : "Expand proxies"}
          className={cx(
            "inline-flex size-8 shrink-0 cursor-pointer items-center justify-center rounded-control border border-border",
            "bg-white/4 text-accent outline-none transition duration-200 focus-visible:ring-2 focus-visible:ring-accent/60",
            "hover:-translate-y-0.5 hover:border-accent/30 hover:bg-white/8",
          )}
          onClick={() => setOpen((v) => !v)}
        >
          <span aria-hidden="true" className="text-sm leading-none">
            {open ? "▴" : "▾"}
          </span>
        </button>
      </div>
      <p className="m-0 text-[13px]">
        The scraper uses the shared proxy list from the{" "}
        <a href="/" className="text-accent underline">
          Configuration
        </a>{" "}
        page. Add or manage proxies in the <strong>Proxy</strong> section.
      </p>
      <div id="sc-proxy-panel" hidden={!open} className="mt-3">
        <ChipInput
          id="sc-proxy-input"
          aria-label="Proxies"
          value={proxies}
          onChange={setProxies}
          placeholder="host:port or scheme://user:pass@host:port, Enter to add"
        />
        <div className="mt-2.5 flex justify-end">
          <Button id="sc-proxy-save" variant="primary" size="sm" loading={saving} disabled={!dirty} onClick={() => void onSave()}>
            Save proxies
          </Button>
        </div>
      </div>
    </Card>
  );
}
