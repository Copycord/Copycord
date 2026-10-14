import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it } from "vitest";
import { LOGIN_ERROR_META, Login, readLoginError } from "./Login";

function setError(text: string | null) {
  document.head.querySelectorAll(`meta[name="${LOGIN_ERROR_META}"]`).forEach((m) => m.remove());
  if (text === null) return;
  const meta = document.createElement("meta");
  meta.name = LOGIN_ERROR_META;
  meta.content = text;
  document.head.append(meta);
}

afterEach(() => setError(null));

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
