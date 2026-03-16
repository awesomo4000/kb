# Step 13: Comparison Operators

> **STATUS: DONE** — Merged in PR #32 (commit f4d8ae2). Review caught
> `checkComparison` returning `true` for unbound variables — fixed to `false`.

Adds infix comparison operators (`=`, `!=`, `<`, `>`, `<=`, `>=`) to rule
bodies. Comparisons filter bindings — they never generate new facts, only
discard bindings that don't satisfy the condition.

Checklist: all items complete. See full spec for details.
