// Side menu "Log out": POST /logout through the CSRF fetch wrapper, then go
// to the login page whatever the response.
document.addEventListener("click", async (e) => {
  const link = e.target.closest && e.target.closest("[data-logout]");
  if (!link) return;
  e.preventDefault();
  if (link.dataset.busy === "1") return;
  link.dataset.busy = "1";
  try {
    await fetch("/logout", {
      method: "POST",
      credentials: "same-origin",
    });
  } catch {}
  location.assign("/login");
});
