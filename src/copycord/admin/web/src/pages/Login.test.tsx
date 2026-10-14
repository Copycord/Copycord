import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it } from "vitest";
import { LOGIN_ERROR_META, Login, VERSION_META, readLoginError } from "./Login";

function setMeta(name: string, text: string | null) {
  document.head.querySelectorAll(`meta[name="${name}"]`).forEach((m) => m.remove());
  if (text === null) return;
  const meta = document.createElement("meta");
  meta.name = name;
  meta.content = text;
  document.head.append(meta);
}

const setError = (text: string | null) => setMeta(LOGIN_ERROR_META, text);

afterEach(() => {
  setError(null);
  setMeta(VERSION_META, null);
});

describe("Login", () => {
  it("reads the server error from the meta tag", () => {
    expect(readLoginError()).toBe("");
    setError("Incorrect password. Please try again.");
    expect(readLoginError()).toBe("Incorrect password. Please try again.");
  });

  it("posts a plain form to /login with the old ids", () => {
    const { container } = render(<Login />);
    const form = container.querySelector("#login-form");
    expect(form).toHaveAttribute("method", "post");
    expect(form).toHaveAttribute("action", "/login");
    expect(container.querySelector("#password")).toHaveAttribute("name", "password");
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("shows the error as text and enables Sign in once a password is typed", async () => {
    setError("<b>Too many failed attempts.</b>");
    const user = userEvent.setup();
    render(<Login />);
    expect(screen.getByRole("alert")).toHaveTextContent("<b>Too many failed attempts.</b>");
    const submit = screen.getByRole("button", { name: "Sign in" });
    expect(submit).toBeDisabled();
    await user.type(screen.getByLabelText("Password"), "pw");
    expect(submit).toBeEnabled();
  });

  it("shows the server's version in the footer, dev without one", () => {
    const { container, unmount } = render(<Login />);
    expect(container.querySelector("#footer-version-text")).toHaveTextContent("dev");
    unmount();
    setMeta(VERSION_META, "v3.28.0");
    render(<Login />);
    expect(screen.getByLabelText("Copycord version v3.28.0")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "v3.28.0" })).toHaveAttribute(
      "href",
      expect.stringContaining("/releases/tag/v3.28.0"),
    );
  });

  it("ignores the space key and drops typed spaces", async () => {
    const user = userEvent.setup();
    render(<Login />);
    const input = screen.getByLabelText("Password") as HTMLInputElement;
    await user.type(input, "ab cd");
    expect(input.value).toBe("abcd");
    await user.keyboard(" ");
    expect(input.value).toBe("abcd");
    expect(screen.getByRole("button", { name: "Sign in" })).toBeEnabled();
  });

  it("strips whitespace from a pasted password and submits it without spaces", async () => {
    const user = userEvent.setup();
    const { container } = render(<Login />);
    const input = screen.getByLabelText("Password") as HTMLInputElement;
    await user.click(input);
    await user.paste("abc 123 ");
    expect(input.value).toBe("abc123");
    let sent: FormDataEntryValue | null = null;
    const form = container.querySelector("#login-form") as HTMLFormElement;
    form.addEventListener("submit", (e) => {
      e.preventDefault();
      sent = new FormData(form).get("password");
    });
    await user.click(screen.getByRole("button", { name: "Sign in" }));
    expect(sent).toBe("abc123");
  });

  it("does not enable Sign in for a pasted run of spaces", async () => {
    const user = userEvent.setup();
    render(<Login />);
    const input = screen.getByLabelText("Password") as HTMLInputElement;
    await user.click(input);
    await user.paste("  \t ");
    expect(input.value).toBe("");
    expect(screen.getByRole("button", { name: "Sign in" })).toBeDisabled();
  });

  it("toggles the password between hidden and shown", async () => {
    const user = userEvent.setup();
    render(<Login />);
    const input = screen.getByLabelText("Password");
    expect(input).toHaveAttribute("type", "password");
    await user.click(screen.getByRole("button", { name: "Show password" }));
    expect(input).toHaveAttribute("type", "text");
    await user.click(screen.getByRole("button", { name: "Hide password" }));
    expect(input).toHaveAttribute("type", "password");
  });
});
