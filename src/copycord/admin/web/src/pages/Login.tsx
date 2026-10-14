// Login page: a plain form post to POST /login (auth.py), same ids as the old
// templates/login.html. A failed post returns this page again with the error
// in a meta tag, shown here as text.

import { useState } from "react";
import { Button } from "../components/ui";
import { CONTROL_CLASS } from "../components/ui/Field";

export const LOGIN_ERROR_META = "copycord-login-error";

/** The error the server put in the page, or "" when there is none. */
export function readLoginError(doc: Document = document): string {
  const meta = doc.querySelector<HTMLMetaElement>(`meta[name="${LOGIN_ERROR_META}"]`);
  return meta?.content.trim() ?? "";
}

export function Login() {
  const [error] = useState(readLoginError);
  const [password, setPassword] = useState("");
  const [shown, setShown] = useState(false);
  const empty = password.trim().length === 0;

  return (
    <div className="flex min-h-screen flex-col items-center justify-center bg-page px-4 font-sans text-text">
      <img src="/static/logo.png" alt="Copycord Logo" draggable={false} className="mb-6 w-24" />
      <section className="w-full max-w-sm rounded-card border border-border bg-card p-6 shadow-card">
        <h1 className="text-center text-2xl font-bold tracking-wide">Copycord</h1>
        <p className="mt-1 mb-4 text-center text-sm text-muted">Enter the password to continue.</p>

        {error ? (
          <div role="alert" className="login-error mb-4 rounded-control border border-danger/40 bg-danger/10 px-3 py-2 text-sm text-danger">
            {error}
          </div>
        ) : null}

        <form
          method="post"
          action="/login"
          id="login-form"
          className="flex flex-col gap-3"
          onSubmit={(e) => {
            if (empty) e.preventDefault();
          }}
        >
          <label htmlFor="password" className="text-sm font-semibold">
            Password
          </label>
          <div className="relative">
            <input
              id="password"
              name="password"
              type={shown ? "text" : "password"}
              autoComplete="current-password"
              autoFocus
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className={`${CONTROL_CLASS} pr-10`}
            />
            <button
              type="button"
              className="absolute inset-y-0 right-0 flex w-10 items-center justify-center text-muted hover:text-text"
              aria-pressed={shown}
              aria-label={shown ? "Hide password" : "Show password"}
              onClick={() => setShown((s) => !s)}
            >
              <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                {shown ? (
                  <path d="M17.94 17.94A10.07 10.07 0 0 1 12 19c-7 0-11-7-11-7a18.4 18.4 0 0 1 5.06-5.94M9.9 9.9A3 3 0 0 0 9 12a3 3 0 0 0 3 3 3 3 0 0 0 2.1-.9M1 1l22 22" />
                ) : (
                  <>
                    <path d="M1 12s4-7 11-7 11 7 11 7-4 7-11 7S1 12 1 12Z" />
                    <circle cx="12" cy="12" r="3" />
                  </>
                )}
              </svg>
            </button>
          </div>
          <Button type="submit" variant="primary" disabled={empty} className="w-full">
            Sign in
          </Button>
        </form>
      </section>
    </div>
  );
}
