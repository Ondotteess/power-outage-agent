import { Link } from "react-router-dom";

export function NotFound() {
  return (
    <div className="flex h-full items-center justify-center">
      <div className="text-center">
        <div className="font-mono text-5xl text-accent-teal">404</div>
        <div className="mt-2 text-sm text-ink-muted">This route doesn't exist.</div>
        <Link to="/" className="btn mt-4 inline-flex">
          Back to dashboard
        </Link>
      </div>
    </div>
  );
}
