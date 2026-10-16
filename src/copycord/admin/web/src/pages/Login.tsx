// Login page: a plain form post to POST /login (auth.py), same ids as the old
// templates/login.html. A failed post returns this page again with the error
// in a meta tag, shown here as text.

import { useState } from "react";
import { Footer } from "../components/Footer";

export const LOGIN_ERROR_META = "copycord-login-error";
// /version sits behind the password, so the server puts the footer version in the login page too.
export const VERSION_META = "copycord-version";

function readMeta(name: string, doc: Document): string {
  const meta = doc.querySelector<HTMLMetaElement>(`meta[name="${name}"]`);
  return meta?.content.trim() ?? "";
}

/** The error the server put in the page, or "" when there is none. */
export function readLoginError(doc: Document = document): string {
  return readMeta(LOGIN_ERROR_META, doc);
}

/** The version the server put in the page, or null when there is none. */
export function readVersion(doc: Document = document): string | null {
  return readMeta(VERSION_META, doc) || null;
}

const stripSpace = (value: string) => value.replace(/\s+/g, "");

export function Login() {
  const [error] = useState(readLoginError);
  const [version] = useState(readVersion);
  const [password, setPassword] = useState("");
  const [shown, setShown] = useState(false);
  const empty = password.trim().length === 0;

  // body.login-page: main.page-main grows to center the card, the footer pill sits under it.
  return (
    <>
      <div className="flex flex-1 items-center justify-center p-5">
        <div className="flex w-full max-w-[420px] flex-col items-center">
          <img
            src="/static/logo.png"
            alt="Copycord Logo"
            draggable={false}
            className="pointer-events-none mb-[18px] size-[86px] select-none"
          />
          <section className="w-full max-w-[420px] rounded-[14px] border border-border bg-card px-6 pt-[26px] pb-[22px] text-text shadow-popover">
            <h1 className="m-0 mb-1 text-[1.3rem] font-semibold text-text">Copycord</h1>
            <p className="m-0 mb-5 text-[0.9rem] text-muted">Enter the password to continue.</p>

            {error ? (
              <div
                role="alert"
                className="login-error mb-3.5 rounded-btn border border-danger-a35 bg-danger-a10 px-2.5 py-2 text-[0.85rem] text-danger-text"
              >
                {error}
              </div>
            ) : null}

            <form
              method="post"
              action="/login"
              id="login-form"
              className="w-full"
              onSubmit={(e) => {
                if (empty) e.preventDefault();
              }}
            >
              <label htmlFor="password" className="mb-1.5 block text-[0.83rem] font-medium text-text">
                Password
              </label>
              <div className="relative inline-block w-full">
                <input
                  id="password"
                  name="password"
                  type={shown ? "text" : "password"}
                  autoComplete="current-password"
                  autoFocus
                  value={password}
                  // Passwords never hold whitespace: the space key does nothing and pasted or typed spaces are dropped.
                  onKeyDown={(e) => {
                    if (e.key === " ") e.preventDefault();
                  }}
                  onPaste={(e) => {
                    e.preventDefault();
                    const el = e.currentTarget;
                    const start = el.selectionStart ?? el.value.length;
                    const end = el.selectionEnd ?? el.value.length;
                    const text = stripSpace(e.clipboardData.getData("text"));
                    setPassword(stripSpace(el.value.slice(0, start) + text + el.value.slice(end)));
                  }}
                  onChange={(e) => setPassword(stripSpace(e.target.value))}
                  className="w-full rounded-btn border border-border-input bg-bg-3 py-2.5 pr-10 pl-3 text-[0.95rem] text-text outline-none transition-[border-color,background-color] duration-150 focus:border-primary focus:bg-bg-2"
                />
                <button
                  type="button"
                  className="absolute top-1/2 right-3 flex -translate-y-1/2 cursor-pointer items-center justify-center border-0 bg-transparent p-0 leading-0 text-text opacity-60 outline-none hover:opacity-100 focus:opacity-100"
                  aria-pressed={shown}
                  aria-label={shown ? "Hide password" : "Show password"}
                  onClick={() => setShown((s) => !s)}
                >
                  <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" className="pointer-events-none block">
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
              <button
                type="submit"
                disabled={empty}
                className="mt-4 inline-block w-full cursor-pointer rounded-full border-0 bg-primary py-2.5 text-[0.95rem] font-semibold text-white shadow-[0_10px_18px_var(--color-shadow-45)] transition duration-150 enabled:hover:bg-primary-600 enabled:hover:shadow-[0_12px_22px_var(--color-shadow-50)] enabled:active:translate-y-px enabled:active:bg-primary-700 disabled:cursor-default disabled:opacity-50 disabled:shadow-none"
              >
                Sign in
              </button>
            </form>
          </section>
        </div>
      </div>
      <Footer version={version} />
    </>
  );
}
