# Step 12: Wildcard Terms

Adds `_` as a wildcard term in rule bodies and queries. Each `_` matches any
value independently — two wildcards in the same rule never unify with each
other.

---

## 0. Motivation

Without wildcards, expressing "entity X participates in *some* relation
regardless of the other argument" requires inventing throwaway variable names:

```prolog
is_member(U) :- member_of(U, _Unused1).
has_sessions(C) :- session(_Unused2, C).
```

This is ugly and fragile. If a user accidentally reuses `_Unused1` in another
atom in the same rule, they get an unintended join constraint. Wildcards make
the intent explicit and guarantee independence by construction.

```prolog
is_member(U) :- member_of(U, _).
has_sessions(C) :- session(_, C).
has_cross_domain(U) :- member_of(U, _), member_of(_, U).
```

---

## 1. Scope

**In scope:**

- Parser desugars `_` to fresh internal variable names
- Works in rule bodies, facts, and queries
- Each `_` is independent (no unification between wildcards)
- Internal names use `#` character to prevent collision with user variables

**Out of scope:**

- No lexer changes (`_` already lexes as an identifier)
- No AST changes (wildcards become regular variables)
- No evaluator changes (anonymous variables bind normally, just never appear
  in heads so their bindings are discarded)
- No bitmap evaluator changes (same reasoning)
- No stratification changes (wildcards are variables)

---

## 2. Design

### The collision problem

Generating names like `_anon_0` risks collision — a user could write a variable
called `_anon_0` in their `.dl` file. We'd need to document "don't use this
name" and hope people read the docs.

### The fix: structurally unparseable names

Generated names use a character that the lexer cannot produce inside an
identifier. The lexer's `isIdentChar` accepts `[a-zA-Z0-9_]` only. The `#`
character is outside this set, so a variable named `_#0` is structurally
impossible to write in a `.dl` file — the lexer would produce an error token
at `#`.

But internally, variable names are just `[]const u8` slices in the AST. The
evaluator compares them with `std.mem.eql`, which doesn't care about
lexer-valid characters. So `_#0` works perfectly as an internal variable name.

**Generated name format:** `_#0`, `_#1`, `_#2`, ...

Counter resets per rule (and per query), so names don't grow unboundedly across
a program.

### Implementation

In `parseTerm`, when the current token is the identifier `_`:

```zig
fn parseTerm(self: *Parser) ParseError!Term {
    if (self.current.type == .string) {
        // ... existing string handling ...
    }

    if (self.current.type == .identifier) {
        const text = self.current.text;

        // Wildcard: generate fresh anonymous variable
        if (text.len == 1 and text[0] == '_') {
            const name = std.fmt.allocPrint(
                self.allocator(), "_#{}", .{self.anon_counter}
            ) catch return error.OutOfMemory;
            self.anon_counter += 1;
            self.advance();
            return .{ .variable = name };
        }

        // ... existing identifier handling ...
    }

    self.setError("expected variable, constant, or string");
    return error.UnexpectedToken;
}
```

Note: only bare `_` is a wildcard. `_foo`, `_x`, `_Unused` remain normal
identifiers with their existing semantics (lowercase-start → constant,
uppercase-start → variable).

### Counter field on Parser

Add `anon_counter: u32 = 0` to the `Parser` struct. Reset it at the start of
each rule and each query:

```zig
// In parseRule, before parsing:
self.anon_counter = 0;

// In parseQueryBody (or wherever queries are parsed), before parsing:
self.anon_counter = 0;
```

This keeps generated names short and predictable per rule. Rule N's wildcards
don't affect rule N+1's naming.

---

## 3. How It Flows Through the System

1. **Lexer:** `_` lexes as `identifier` with text `"_"`. No change.

2. **Parser:** `parseTerm` checks for `"_"`, generates `_#0`, `_#1`, etc.
   Returned as `Term{ .variable = "_#0" }`.

3. **AST:** Rule body contains normal `Atom`s with variable terms. Nothing
   special about `_#N` at the AST level.

4. **Stratification:** Wildcards are variables. They appear in body atoms, not
   in heads, so they don't create predicate dependencies. No effect on strata.

5. **Bitmap evaluator:** Anonymous variables bind during iteration just like
   any other variable. Since they don't appear in the head's terms,
   `computeHeadVals` / `head_from_body` mapping simply doesn't reference them.
   Their bindings are produced and ignored. No code change needed.

6. **Query resolution:** If a user writes `?- member_of(_, "admins").`, the
   wildcard becomes `_#0` which is a variable starting with `_` (lowercase).
   **Problem:** `parseTerm` currently treats lowercase-start identifiers as
   constants. But `_#0` is generated as `.variable` directly, bypassing the
   uppercase check. So this works — the generated `Term{ .variable = "_#0" }`
   is returned before the uppercase/lowercase classification runs.

---

## 4. File Changes

| File | Change |
|---|---|
| `src/datalog.zig` | Add `anon_counter: u32 = 0` to Parser struct |
| `src/datalog.zig` | Reset `anon_counter` in `parseRule` and query parsing |
| `src/datalog.zig` | Add wildcard check in `parseTerm` |

One file, ~15 lines of implementation.

---

## 5. Tests

### 5a. Parser tests in `datalog.zig`

```zig
test "parser wildcard desugaring" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator, "is_member(U) :- member_of(U, _).");
    defer parser.deinit();

    const result = try parser.parseProgram();

    try std.testing.expectEqual(@as(usize, 1), result.rules.len);
    const rule = result.rules[0];
    const body_atom = rule.body[0].atom;

    // Second term should be a variable (not constant), with generated name
    try std.testing.expect(body_atom.terms[1] == .variable);
    // Name contains # so it can't collide with user variables
    try std.testing.expect(std.mem.indexOfScalar(u8, body_atom.terms[1].variable, '#') != null);
}

test "parser multiple wildcards are independent" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator,
        "cross(U) :- member_of(U, _), member_of(_, U)."
    );
    defer parser.deinit();

    const result = try parser.parseProgram();

    const rule = result.rules[0];
    const wc1 = rule.body[0].atom.terms[1]; // first _
    const wc2 = rule.body[1].atom.terms[0]; // second _

    // Both are variables
    try std.testing.expect(wc1 == .variable);
    try std.testing.expect(wc2 == .variable);
    // They have different names
    try std.testing.expect(!std.mem.eql(u8, wc1.variable, wc2.variable));
}

test "parser wildcard counter resets per rule" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator,
        \\a(X) :- b(X, _).
        \\c(X) :- d(X, _).
    );
    defer parser.deinit();

    const result = try parser.parseProgram();

    // Both rules should have _#0 as their wildcard (counter resets)
    const wc1 = result.rules[0].body[0].atom.terms[1];
    const wc2 = result.rules[1].body[0].atom.terms[1];
    try std.testing.expectEqualStrings(wc1.variable, wc2.variable);
}

test "parser underscore prefix identifiers are not wildcards" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator, "foo(_bar).");
    defer parser.deinit();

    const result = try parser.parseProgram();

    // _bar starts with lowercase, so it's a constant — not a wildcard
    const term = result.rules[0].head.terms[0];
    try std.testing.expect(term == .constant);
    try std.testing.expectEqualStrings("_bar", term.constant);
}
```

### 5b. Evaluator integration test

```zig
test "bitmap eval: wildcard in rule body" {
    const allocator = std.testing.allocator;

    var parser = datalog.Parser.init(allocator,
        \\member_of("alice", "developers").
        \\member_of("bob", "admins").
        \\member_of("charlie", "developers").
        \\is_member(U) :- member_of(U, _).
    );
    defer parser.deinit();
    const parsed = try parser.parseProgram();

    var eval = BitmapEvaluator.init(allocator, parsed.rules);
    defer eval.deinit();
    try eval.addGroundFacts(parsed.rules);
    try eval.evaluate();

    var q_terms = [_]Term{.{ .variable = "X" }};
    const results = try eval.query(.{ .predicate = "is_member", .terms = &q_terms });
    defer eval.freeQueryResults(results);

    try std.testing.expectEqual(@as(usize, 3), results.len);
}

test "bitmap eval: multiple independent wildcards" {
    const allocator = std.testing.allocator;

    var parser = datalog.Parser.init(allocator,
        \\edge("a", "b"). edge("b", "c"). edge("c", "a").
        \\has_both(X) :- edge(X, _), edge(_, X).
    );
    defer parser.deinit();
    const parsed = try parser.parseProgram();

    var eval = BitmapEvaluator.init(allocator, parsed.rules);
    defer eval.deinit();
    try eval.addGroundFacts(parsed.rules);
    try eval.evaluate();

    var q_terms = [_]Term{.{ .variable = "X" }};
    const results = try eval.query(.{ .predicate = "has_both", .terms = &q_terms });
    defer eval.freeQueryResults(results);

    // a→b and c→a, b→c and a→b, c→a and b→c — all three nodes qualify
    try std.testing.expectEqual(@as(usize, 3), results.len);
}
```

---

## 6. What Does NOT Change

- **Lexer** — `_` already lexes as `identifier`. No token changes.
- **AST types** — `Term`, `Atom`, `BodyElement`, `Rule` unchanged.
- **Evaluator** — anonymous variables are normal variables. No special handling.
- **Bitmap evaluator** — same. Variables not in head are naturally ignored.
- **Stratification** — wildcards create no new dependencies.
- **Entity key** — unrelated.
- **String interner** — unrelated.
- **Fact store** — unrelated.

---

## 7. Checklist

- [ ] Add `anon_counter: u32 = 0` to Parser struct
- [ ] Reset counter at start of `parseRule`
- [ ] Reset counter at start of query parsing
- [ ] Add `_` check in `parseTerm` — generate `_#N` variable
- [ ] Only bare `_` is wildcard, not `_foo` or `_Unused`
- [ ] Test: wildcard desugars to variable with `#` in name
- [ ] Test: multiple wildcards get different names
- [ ] Test: counter resets per rule
- [ ] Test: `_bar` is not a wildcard
- [ ] Test: evaluator integration — wildcard in rule body
- [ ] Test: evaluator integration — multiple independent wildcards
- [ ] `zig build test` passes
- [ ] `zig build test-all` passes
