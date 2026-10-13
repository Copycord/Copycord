---
sidebar_position: 1
title: Security defaults
---

# Security defaults

What Copycord does out of the box to keep your dashboard, tokens and backups private, and what to change when you want more.

## Password

- The dashboard always asks for a password. There is no way to turn the login off.
- The installer makes up a random password on a first install and shows it once. `copycord config get PASSWORD --show-secrets` prints it again.
- In Docker, Copycord does not start until you set `PASSWORD`.
- Change it with `copycord config set PASSWORD 'your-new-password'`, then `copycord restart`.
- **Log out** in the dashboard sidebar ends your session.

## Only this computer by default

The dashboard listens on `127.0.0.1`, so only the computer Copycord runs on can open it, at `http://localhost:8080`. The Docker image listens on every network, because the container needs it; publish the port only where you want it reachable.

## Opening it to your network

1. Set your own `PASSWORD`.
2. Set `DASHBOARD_HOST=0.0.0.0` (every network) or one IP address of this computer, in `.env`: `copycord config set DASHBOARD_HOST 0.0.0.0`.
3. Run `copycord restart`.

Copycord refuses to start on any address other than `127.0.0.1` while the password is still `copycord`. Do not put the dashboard on the open internet as it is. Put it behind HTTPS (a reverse proxy, a VPN or a tunnel) first. See [Dashboard address](/docs/cli#dashboard-address).

## Behind a reverse proxy: TRUSTED_PROXIES

A proxy hides the real visitor address behind its own. Set `TRUSTED_PROXIES` to the IP address of your proxy (several addresses are separated by commas) and Copycord uses the visitor address the proxy reports, so the log shows who tried to sign in. Leave it empty, the default, unless a proxy sits in front of Copycord: Copycord then trusts no one's claim about where a request came from.

## Files that stay private

On Linux and macOS these are readable only by your user:

- `secret.key` in the [data directory](/docs/cli#data-dir), which signs the dashboard login cookie.
- The `.env` file, which holds your password.
- Backup archives, which contain `secret.key` and your database. Keep copies private too. See [Backups](/docs/dashboard/backups).

The Docker image runs as user 1000, not root, so the `./data` folder must belong to that user.
