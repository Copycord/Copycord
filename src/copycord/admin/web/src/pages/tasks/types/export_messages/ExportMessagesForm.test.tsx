import { fireEvent, render, screen, within } from "@testing-library/react";
import { MemoryRouter, Route, Routes, useParams } from "react-router";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../../../components/Toast";
import type { TaskRun, TaskType } from "../../../../types/tasks";
import { describeRunInputs, entryFor } from "..";
import { EXPORT_MESSAGES_EXPLAINER, ExportMessagesForm } from "./ExportMessagesForm";
import { ExportMessagesDownloadCard } from "./entry";

const TYPE: TaskType = { key: "export_messages", title: "Export messages", description: "", inputs_schema: {}, active: 0, total: 0 };
const GUILDS = [
  { id: "111", name: "Alpha", icon_url: null },
  { id: "222", name: "Beta", icon_url: null },
];

function serve(reply?: { status: number; body: unknown }) {
  const posts: Record<string, unknown>[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init?: RequestInit) => {
      if (url === "/api/tasks/types/export_messages/options") return Response.json({ guilds: GUILDS });
      if (url === "/api/tasks/runs" && init?.method === "POST") {
        posts.push(JSON.parse(String(init.body)) as Record<string, unknown>);
        if (reply) return Response.json(reply.body, { status: reply.status });
        return Response.json({ run: { id: "r7", number: 7 } }, { status: 201 });
      }
      return Response.json({ detail: "not found" }, { status: 404 });
    }),
  );
  return posts;
}

function RunPage() {
  const { id } = useParams();
  return <p>Run page {id}</p>;
}

function renderForm(initial?: Record<string, string>) {
  render(
    <ToastProvider>
      <MemoryRouter initialEntries={["/tasks"]}>
        <Routes>
          <Route path="/tasks" element={<ExportMessagesForm type={TYPE} open initial={initial} onClose={() => {}} />} />
          <Route path="/tasks/runs/:id" element={<RunPage />} />
        </Routes>
      </MemoryRouter>
    </ToastProvider>,
  );
}

const submit = () => fireEvent.click(screen.getByRole("button", { name: "Export messages" }));
const type = (label: string, value: string) => fireEvent.change(screen.getByLabelText(label), { target: { value } });

async function pick(name: string) {
  fireEvent.click(await screen.findByRole("combobox", { name: "Server" }));
  fireEvent.click(await screen.findByRole("option", { name }));
}

beforeEach(() => localStorage.clear());
afterEach(() => vi.unstubAllGlobals());

describe("ExportMessagesForm", () => {
  it("is the export_messages entry's form and asks for a server before any start", async () => {
    expect(entryFor("export_messages").Form).toBe(ExportMessagesForm);
    const posts = serve();
    renderForm();
    expect(screen.getByText(EXPORT_MESSAGES_EXPLAINER)).toBeInTheDocument();
    submit();
    expect(await screen.findByText("Pick a server.")).toBeInTheDocument();
    expect(posts).toEqual([]);
  });

  it("posts the server, the ID filters, the webhook, the dates and the nested filters, then opens the run", async () => {
    const posts = serve();
    renderForm();
    await pick("Beta");
    type("Channel ID", " 555 ");
    type("User ID", "666");
    type("Webhook URL", "https://discord.com/api/webhooks/1/abc");
    type("After", "2026-10-01T12:00:00");
    fireEvent.click(screen.getByLabelText("System messages"));
    fireEvent.click(screen.getByLabelText("Threads"));
    fireEvent.click(screen.getByLabelText("Only messages with a word"));
    type("Word", " hello ");
    type("Minimum reactions", "3");
    const att = screen.getByRole("group", { name: "Attachment types" });
    fireEvent.click(within(att).getByLabelText("Videos"));
    const dl = screen.getByRole("group", { name: "Download media" });
    fireEvent.click(within(dl).getByLabelText("Images"));
    fireEvent.click(within(dl).getByLabelText("Other files"));
    submit();
    expect(await screen.findByText("Run page r7")).toBeInTheDocument();
    expect(posts).toEqual([
      {
        type: "export_messages",
        inputs: {
          guild_id: "222",
          channel_id: "555",
          user_id: "666",
          webhook_url: "https://discord.com/api/webhooks/1/abc",
          has_attachments: false,
          after_iso: new Date("2026-10-01T12:00:00").toISOString(),
          before_iso: null,
          filters: {
            has_content: true,
            embeds: true,
            attachments: true,
            att_types: { images: true, videos: false, audio: true, other: true },
            links: true,
            emojis: true,
            word_on: true,
            word: "hello",
            replies: true,
            bots: true,
            system: true,
            min_length: 0,
            min_reactions: 3,
            pinned: true,
            stickers: true,
            mentions: true,
            threads: false,
            forum_threads: false,
            private_threads: false,
            download_media: { images: true, videos: false, audio: false, other: true },
          },
        },
      },
    ]);
  });

  it("turning attachments off disables the types and posts none", async () => {
    const posts = serve();
    renderForm();
    await pick("Alpha");
    fireEvent.click(screen.getByLabelText("Attachments"));
    const att = screen.getByRole("group", { name: "Attachment types" });
    expect(within(att).getByLabelText("Images")).toBeDisabled();
    expect(screen.getByText("Attachments are not included.")).toBeInTheDocument();
    submit();
    await screen.findByText("Run page r7");
    const inputs = posts[0]!.inputs as { filters: { attachments: boolean; att_types: unknown } };
    expect(inputs.filters.attachments).toBe(false);
    expect(inputs.filters.att_types).toEqual({ images: false, videos: false, audio: false, other: false });
  });

  it("shows the backend's validation messages on their fields, nested ones too, and the rest under the form", async () => {
    serve({
      status: 422,
      body: {
        errors: {
          channel_id: "Value error, Enter a numeric Discord channel ID",
          webhook_url: "Value error, Enter a webhook URL starting with https://",
          after_iso: "Value error, Enter an ISO date and time, like 2026-10-17T12:00:00Z",
          "filters.min_length": "Input should be greater than or equal to 0",
          inputs: "An export for this server is already running",
        },
      },
    });
    renderForm();
    await pick("Alpha");
    submit();
    expect(await screen.findByText("Enter a numeric Discord channel ID")).toBeInTheDocument();
    expect(screen.getByText("Enter a webhook URL starting with https://")).toBeInTheDocument();
    expect(screen.getByText("Enter an ISO date and time, like 2026-10-17T12:00:00Z")).toBeInTheDocument();
    expect(screen.getByText("Input should be greater than or equal to 0")).toBeInTheDocument();
    expect(screen.getByText("An export for this server is already running")).toBeInTheDocument();
    expect(screen.getByLabelText("Channel ID")).toHaveAttribute("aria-invalid", "true");
    expect(screen.getByLabelText("Minimum length")).toHaveAttribute("aria-invalid", "true");
    expect(screen.queryByText("Run page r7")).toBeNull();
  });

  it("starts with the server the page preselected", async () => {
    const posts = serve();
    renderForm({ guild: "111" });
    expect(await screen.findByRole("combobox", { name: "Server" })).toHaveTextContent("Alpha");
    submit();
    await screen.findByText("Run page r7");
    expect((posts[0]!.inputs as { guild_id: string }).guild_id).toBe("111");
  });
});

describe("Export messages run page", () => {
  it("names the server and the filters in the Inputs rows and has the download card", () => {
    const run = {
      type: "export_messages",
      context: { label: "Alpha" },
      inputs: { guild_id: "111", channel_id: null, webhook_url: "https://discord.com/api/webhooks/1/secret", filters: { attachments: false } },
    } as unknown as TaskRun;
    const rows = describeRunInputs(run);
    expect(rows).toContainEqual({ label: "Server", value: "Alpha" });
    expect(rows).toContainEqual({ label: "Channel ID", value: "All channels" });
    expect(rows).toContainEqual({ label: "Webhook", value: "https://discord.com/api/webhooks/1/..." });
    expect(rows).toContainEqual({ label: "Attachments", value: "Not included" });
    expect(entryFor("export_messages").RunCard).toBeDefined();
  });

  it("shows the saved file from this type's route with a Download JSON button", async () => {
    const urls: string[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string) => {
        urls.push(url);
        return Response.json({ file: { filename: "messages-111-20261017-120000.json", size: 2048, messages: 42 } });
      }),
    );
    const run = { id: "r7", type: "export_messages", report: { output: { json_path: "/data/exports/111/x/messages.json" } } } as unknown as TaskRun;
    render(<ExportMessagesDownloadCard run={run} />);
    expect(await screen.findByText("messages-111-20261017-120000.json")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Download JSON" })).toBeInTheDocument();
    expect(urls).toEqual(["/api/tasks/export-messages/r7"]);
  });
});
