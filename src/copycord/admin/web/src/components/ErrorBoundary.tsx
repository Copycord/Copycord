// Catches a render error in the page under it and shows a card with a reload
// button instead of unmounting the whole app. Layout keys it on the path, so
// navigating to another page starts it fresh.

import { Component, type ErrorInfo, type ReactNode } from "react";
import { Button, Card } from "./ui";

interface Props {
  children: ReactNode;
}

interface State {
  failed: boolean;
}

export class ErrorBoundary extends Component<Props, State> {
  state: State = { failed: false };

  static getDerivedStateFromError(): State {
    return { failed: true };
  }

  componentDidCatch(error: unknown, info: ErrorInfo): void {
    console.error("page render failed", error, info.componentStack);
  }

  render(): ReactNode {
    if (!this.state.failed) return this.props.children;
    return (
      <Card role="alert" className="mx-auto my-8 w-full max-w-md text-center">
        <p className="m-0 mb-4">Something went wrong.</p>
        <Button type="button" onClick={() => window.location.reload()}>
          Reload
        </Button>
      </Card>
    );
  }
}
