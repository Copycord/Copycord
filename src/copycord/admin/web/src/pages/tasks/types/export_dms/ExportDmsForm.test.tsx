import { fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes, useParams } from "react-router";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { ToastProvider } from "../../../../components/Toast";
import type { TaskType } from "../../../../types/tasks";
import { typeLabel } from "../../typeLabels";
import { describeRunInputs, entryFor, heroStat, summaryStats } from "..";
import { maskWebhook } from "./entry";
import { EXPORT_DMS_EXPLAINER, ExportDmsForm, destinationError, userIdError, webhookError } from "./ExportDmsForm";

const TYPE: TaskType = { key: "export_dms", title: "Export DMs", description: "", inputs_schema: {}, active: 0, total: 0 };
const UID = "123456789012345678";
const HOOK = "https://discord.com/api/webhooks/111/secret-token";

function serve(reply?: { status: number; body: unknown }) {
  const posts: Record<string, unknown>[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init?: RequestInit) => {
      if (url === "/api/tasks/runs" && init?.method === "POST") {
        posts.push(JSON.parse(String(init.body)) as Record<string, unknown>);
        if (reply) return Response.json(reply.body, { status: reply.status });
        return Response.json({ run: { id: "r9", number: 9 } }, { status: 201 });
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

function renderForm() {
  render(
    <ToastProvider>
      <MemoryRouter initialEntries={["/tasks"]}>
        <Routes>
          <Route path="/tasks" element={<ExportDmsForm type={TYPE} open onClose={() => {}} />} />
          <Route path="/tasks/runs/:id" element={<RunPage />} />
        </Routes>
      </MemoryRouter>
    </ToastProvider>,
  );
}

const userId = () => screen.getByRole("textbox", { name: "User ID" });
const webhook = () => screen.getByRole("textbox", { name: "Webhook URL" });
const submit = () => fireEvent.click(screen.getByRole("button", { name: "Export DMs" }));

beforeEach(() => localStorage.clear());

describe("ExportDmsForm", () => {
  it("explains the export and starts with a JSON file and no webhook", () => {
    serve();
    renderForm();
    expect(screen.getByText(EXPORT_DMS_EXPLAINER)).toBeInTheDocument();
    expect(userId()).toHaveValue("");
    expect(screen.getByRole("checkbox", { name: "Save a JSON file" })).toBeChecked();
    expect(webhook()).toHaveValue("");
    expect(screen.getByRole("button", { name: "Export DMs" })).toBeInTheDocument();
  });

  it("asks for a numeric user ID without a request", async () => {
    const posts = serve();
    renderForm();
    for (const bad of ["", "abc", "12 34"]) {
      fireEvent.change(userId(), { target: { value: bad } });
      submit();
      expect(await screen.findByRole("alert")).toHaveTextContent("Enter a numeric Discord user ID.");
    }
    expect(posts).toEqual([]);
  });

  it("needs a destination and a webhook that is a URL", async () => {
    const posts = serve();
    renderForm();
    fireEvent.change(userId(), { target: { value: UID } });
    fireEvent.click(screen.getByRole("checkbox", { name: "Save a JSON file" }));
    submit();
    expect(await screen.findByRole("alert")).toHaveTextContent("Pick a destination");
    fireEvent.change(webhook(), { target: { value: "discord.com/api/webhooks/1/x" } });
    submit();
    expect(await screen.findByRole("alert")).toHaveTextContent("Enter a webhook URL starting with https://");
    expect(posts).toEqual([]);
  });

  it("starts the run with the inputs the slash command took and opens it", async () => {
    const posts = serve();
    renderForm();
    fireEvent.change(userId(), { target: { value: ` ${UID} ` } });
    fireEvent.change(webhook(), { target: { value: HOOK } });
    submit();
    expect(await screen.findByText("Run page r9")).toBeInTheDocument();
    expect(posts[0]).toEqual({ type: "export_dms", inputs: { user_id: UID, json_file: true, webhook_url: HOOK } });
  });

  it("sends no webhook when the field is empty", async () => {
    const posts = serve();
    renderForm();
    fireEvent.change(userId(), { target: { value: UID } });
    submit();
    expect(await screen.findByText("Run page r9")).toBeInTheDocument();
    expect(posts[0]!.inputs).toEqual({ user_id: UID, json_file: true, webhook_url: null });
  });

  it("shows a 422 from the server on its field", async () => {
    serve({ status: 422, body: { errors: { user_id: "Value error, Enter a numeric Discord user ID" } } });
    renderForm();
    fireEvent.change(userId(), { target: { value: UID } });
    submit();
    expect(await screen.findByRole("alert")).toHaveTextContent(/^Enter a numeric Discord user ID$/);
  });

  it("validates in words", () => {
    expect(userIdError(UID)).toBeUndefined();
    expect(userIdError("12")).toBeTruthy();
    expect(webhookError("")).toBeUndefined();
    expect(webhookError(HOOK)).toBeUndefined();
    expect(webhookError("ftp://x")).toBeTruthy();
    expect(destinationError(false, "")).toBeTruthy();
    expect(destinationError(false, HOOK)).toBeUndefined();
  });
});

describe("export_dms registration", () => {
  it("has its own form, inputs rows without the webhook token, summary words and hero", () => {
    expect(entryFor("export_dms").Form).toBe(ExportDmsForm);
    const run = {
      type: "export_dms",
      inputs: { user_id: UID, json_file: true, webhook_url: HOOK },
      context: { label: UID },
      summary: { exported: 300, failed: 0, forwarded: 300 },
      report: null,
    } as unknown as Parameters<typeof describeRunInputs>[0] & Parameters<typeof heroStat>[0];
    expect(describeRunInputs(run)).toEqual([
      { label: "User ID", value: UID },
      { label: "JSON file", value: "Yes" },
      { label: "Webhook", value: "https://discord.com/api/webhooks/111/..." },
    ]);
    expect(heroStat(run)).toEqual({ key: "exported", label: "messages exported", value: 300 });
    expect(summaryStats(run).map((s) => s.label)).toEqual(["Messages exported", "Messages failed", "Messages forwarded"]);
    expect(maskWebhook(null)).toBe("None");
    expect(maskWebhook("https://x.io/a")).toBe("https://x.io/a");
  });

  it("is titled Export DMs before the type list loads", () => {
    expect(typeLabel("export_dms").title).toBe("Export DMs");
  });
});
