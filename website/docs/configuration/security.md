---
sidebar_position: 1
title: Security defaults
---

# Security defaults

Learn how Copycord protects your dashboard, tokens and backups by default, and what to change to open the dashboard to other devices.

## Password

- The dashboard always asks for a password; the login cannot be turned off.
- The installer creates a random password on the first install and shows it once. To print it again, run `copycord config get PASSWORD --show-secrets`.
- In Docker, Copycord does not start until you set `PASSWORD`.
- To change the password, run `copycord config set PASSWORD 'your-new-password'`, then `copycord restart`.
- To end your session, click **Log out** in the dashboard sidebar.

## Dashboard address

By default the dashboard listens on `127.0.0.1`, so only the computer Copycord runs on can open it, at `http://localhost:8080`. The Docker image listens on every network, because the container needs it; publish the port only where you want it reachable.

To open the dashboard to your network:

1. Set your own `PASSWORD`.
2. Set `DASHBOARD_HOST` to `0.0.0.0` (every network) or to one IP address of this computer: `copycord config set DASHBOARD_HOST 0.0.0.0`.
3. Run `copycord restart`.

On any address other than `127.0.0.1`, Copycord does not start while the password is `copycord`, and `copycord doctor` reports a failure. If something else already protects the dashboard, such as a reverse proxy with its own login, `COPYCORD_ALLOW_INSECURE_DASHBOARD=1` skips this check.

:::warning
Do not put the dashboard on the open internet as it is. Put it behind HTTPS first, with a reverse proxy, a VPN or a tunnel.
:::

## Reverse proxies

A reverse proxy hides the visitor's address behind its own. To log the real address of each sign-in, set `TRUSTED_PROXIES` to the IP address of your proxy; separate several addresses with commas. Leave it empty, the default, when no proxy sits in front of Copycord.

## Private files

On Linux and macOS, only your user can read these files:

- `secret.key` in the [data directory](/docs/cli#data-dir), which signs the dashboard login.
- `.env`, which holds your password.
- Backup archives, which contain `secret.key` and your database. Keep copies of them private too. See [Backups](/docs/dashboard/backups).

The Docker image runs Copycord as user 1000, not root. See [Docker Install](/docs/getting-started/docker-install#file-ownership).
