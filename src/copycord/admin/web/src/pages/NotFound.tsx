// Unknown route: the old 404.html card (.card, 640px wide, 40px from the top) inside the normal shell.

import { Link } from "react-router";
import { Card } from "../components/ui";

export function NotFound() {
  return (
    <Card className="mx-auto my-10 max-w-[640px] text-center">
      <h2 className="my-5 text-2xl font-bold">404 – Page not found</h2>
      <img
        src="/static/logo.png"
        alt="Copycord logo"
        draggable={false}
        className="mt-4 inline h-auto w-24 max-w-none opacity-90"
      />
      {/* .btns with a .btn.btn-ghost */}
      <div className="mt-6 flex flex-wrap justify-center gap-2.5">
        <Link
          to="/"
          className="inline-flex min-h-[42px] items-center justify-center rounded-control border border-border bg-wash-4 px-4 py-2.5 text-sm leading-none font-semibold text-accent no-underline transition duration-200 hover:-translate-y-0.5 hover:border-accent-a30 hover:bg-line-1 hover:shadow-soft"
        >
          Back to dashboard
        </Link>
      </div>
    </Card>
  );
}
