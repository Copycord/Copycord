// /app/_kit: every shared component on one page, for page lanes and review.
// Not linked from the sidebar.

import { useState } from "react";
import {
  Badge,
  Button,
  Card,
  ChipInput,
  EmptyState,
  Field,
  Input,
  Select,
  Spinner,
  StatusPill,
  Table,
  Tabs,
  Textarea,
  Toggle,
  Tooltip,
  useConfirm,
} from "../components/ui";
import type { Column } from "../components/ui";
import { StatusGate } from "../components/ui/StatusGate";

interface DemoRow {
  id: string;
  name: string;
  status: "online" | "offline" | "warn";
}

const ROWS: DemoRow[] = [
  { id: "1", name: "Server", status: "online" },
  { id: "2", name: "Client", status: "warn" },
  { id: "3", name: "Forwarder", status: "offline" },
];

const COLUMNS: Column<DemoRow>[] = [
  { key: "name", header: "Name" },
  { key: "status", header: "Status", cell: (r) => <StatusPill status={r.status} /> },
];

export function Kit() {
  const confirm = useConfirm();
  const [toggle, setToggle] = useState(true);
  const [words, setWords] = useState<string[]>(["spoiler", "nsfw"]);
  const [ids, setIds] = useState<string[]>(["123456789012345678"]);
  const [answer, setAnswer] = useState("none yet");
  const [tableMode, setTableMode] = useState<"rows" | "loading" | "empty">("rows");
  const [gated, setGated] = useState(false);

  return (
    <div className="flex flex-col gap-5 p-page">
      <h1 className="text-2xl font-semibold">Component kit</h1>

      <Card title="Buttons">
        <div className="flex flex-wrap items-center gap-3">
          <Button variant="primary">Primary</Button>
          <Button>Secondary</Button>
          <Button variant="danger">Danger</Button>
          <Button variant="ghost">Ghost</Button>
          <Button variant="outline">Outline</Button>
          <Button variant="primary" loading>
            Saving
          </Button>
          <Button disabled>Disabled</Button>
          <Button size="sm">Small</Button>
          <Button size="lg" variant="primary">
            Large
          </Button>
        </div>
      </Card>

      <Card title="Fields">
        <div className="grid gap-4 md:grid-cols-2">
          <Field label="Server token" help="From the Discord developer portal">
            <Input placeholder="Token" />
          </Field>
          <Field label="Guild id" error="Must be a number">
            <Input defaultValue="abc" />
          </Field>
          <Field label="Mode">
            <Select
              options={[
                { value: "copy", label: "Copy" },
                { value: "mirror", label: "Mirror" },
              ]}
            />
          </Field>
          <Field label="Notes">
            <Textarea placeholder="Anything" />
          </Field>
          <Field label="Blocked words" help="Enter or comma to add, Backspace removes the last">
            <ChipInput value={words} onChange={setWords} placeholder="Type a word" />
          </Field>
          <Field label="Channel ids" help="Digits only, paste a list">
            <ChipInput
              mode="ids"
              value={ids}
              onChange={setIds}
              validate={(v) => (v.length < 17 ? "too short for a Discord id" : undefined)}
            />
          </Field>
        </div>
      </Card>

      <Card title="Toggle, badges, tooltip, spinner">
        <div className="flex flex-col gap-4">
          <Toggle checked={toggle} onChange={setToggle} label="Enable cloning" description="Copies new messages as they arrive" />
          <div className="flex flex-wrap items-center gap-2">
            <StatusPill status="online" />
            <StatusPill status="offline" />
            <StatusPill status="warn" />
            <StatusPill status="unknown" />
            <Badge>neutral</Badge>
            <Badge tone="good">good</Badge>
            <Badge tone="warn">warn</Badge>
            <Badge tone="bad">bad</Badge>
            <Badge tone="accent">accent</Badge>
          </div>
          <div className="flex items-center gap-4">
            <Tooltip content="Explains the button">
              <Button variant="ghost">Hover me</Button>
            </Tooltip>
            <Spinner size="sm" />
            <Spinner />
            <Spinner size="lg" />
          </div>
        </div>
      </Card>

      <Card title="Confirm dialog">
        <div className="flex items-center gap-3">
          <Button
            variant="danger"
            onClick={async () => {
              const ok = await confirm({
                title: "Delete mapping?",
                body: "This removes the mapping and its settings.",
                confirmLabel: "Delete",
                danger: true,
              });
              setAnswer(ok ? "confirmed" : "cancelled");
            }}
          >
            Ask
          </Button>
          <span className="text-sm text-text-2">Answer: {answer}</span>
        </div>
      </Card>

      <Card
        title="Table"
        actions={
          <Select
            aria-label="Table state"
            value={tableMode}
            onChange={(e) => setTableMode(e.target.value as typeof tableMode)}
            options={[
              { value: "rows", label: "Rows" },
              { value: "loading", label: "Loading" },
              { value: "empty", label: "Empty" },
            ]}
          />
        }
      >
        <Table
          columns={COLUMNS}
          rows={tableMode === "empty" ? [] : ROWS}
          loading={tableMode === "loading"}
          rowKey={(r) => r.id}
          empty="No processes"
        />
      </Card>

      <Card title="Tabs">
        <Tabs
          aria-label="Demo tabs"
          tabs={[
            { id: "general", label: "General", content: <p>General settings.</p> },
            { id: "filters", label: "Filters", content: <p>Filter settings.</p> },
            { id: "locked", label: "Locked", content: null, disabled: true },
          ]}
        />
      </Card>

      <Card title="Empty state">
        <EmptyState title="No mappings yet" description="Add a mapping to start cloning." action={<Button variant="primary">Add mapping</Button>} />
      </Card>

      <Card title="Status gate" actions={<Toggle hideLabel label="Gate on" checked={gated} onChange={setGated} />}>
        <StatusGate
          gate={{
            gated,
            ready: !gated,
            serverReady: !gated,
            clientReady: !gated,
            message: "Waiting for Server and Client connection...",
          }}
        >
          <p className="py-10 text-center text-sm">Page content behind the gate.</p>
        </StatusGate>
      </Card>
    </div>
  );
}
