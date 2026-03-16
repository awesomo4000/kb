# Step 14: Bare Number Literals

> **STATUS: DONE** — Merged in PR #35 (commit 9b754cb).

Adds numeric literals to the lexer so `Pts >= 20` works without quotes.
Pure lexer/parser convenience — numbers become `Term{ .constant = "20" }`
internally, identical to quoted strings. No evaluator or storage changes.

Checklist: all items complete. See full spec for details.
