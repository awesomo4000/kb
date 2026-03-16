# Step 14: Bare Number Literals

Adds numeric literals to the lexer so `Pts >= 20` works without quotes.
Pure lexer/parser convenience — no evaluator or storage changes. Numbers
become string constants internally, identical to `"20"`.

---

## 0. Motivation

Currently all values must be quoted strings:

```prolog
high_scorer(P) :- points(P, Pts), Pts >= "20".
points("Alice", "28").
```

This is noisy for numeric data. With bare number literals:

```prolog
high_scorer(P) :- points(P, Pts), Pts >= 20.
points("Alice", 28).
```

Both forms produce identical AST nodes (`Term{ .constant = "20" }`). The
evaluator already does `parseInt(i64, value, 10)` at comparison time, so
no evaluator changes are needed.

---

## 1. Scope

**In scope:**

- Lexer: add `number` token type, read sequences of digits
- Parser: treat `number` tokens as constants in `parseTerm`
- Works in facts, rule bodies, rule heads, comparisons, queries

**Out of scope:**

- Negative literals (`-42`) — would need special handling for the minus sign
  since `-` is not currently a token. Follow-up if needed.
- Floating point (`3.14`) — the `.` conflicts with the statement terminator.
  Would need lookahead to distinguish `3.14` from `3.` (end of fact). Not
  worth the complexity now.
- Any evaluator changes — numbers are still strings internally
- Any storage changes — entity keys still store as strings

---

## 2. Lexer Changes (datalog.zig)

### New token type

```zig
pub const TokenType = enum {
    // ... existing ...
    number,    // 42, 443, 8080
};
```

### Lexer.next() addition

Before the "unknown character" fallback, add digit detection:

```zig
// Numeric literal
if (c >= '0' and c <= '9') {
    return self.readNumber(start, start_col);
}
```

### readNumber method

```zig
fn readNumber(self: *Lexer, start: usize, start_col: usize) Token {
    while (self.pos < self.source.len and
           self.source[self.pos] >= '0' and self.source[self.pos] <= '9')
    {
        self.pos += 1;
        self.col += 1;
    }
    return .{
        .type = .number,
        .text = self.source[start..self.pos],
        .line = self.line,
        .col = start_col,
    };
}
```

This reads the longest sequence of digits. No sign handling, no decimal
point — just `[0-9]+`.

---

## 3. Parser Changes (datalog.zig)

### parseTerm addition

Add a check for number tokens before the identifier check:

```zig
fn parseTerm(self: *Parser) ParseError!Term {
    if (self.current.type == .string) {
        // ... existing string handling ...
    }

    // Bare number literal → constant with the digit text
    if (self.current.type == .number) {
        const text = try self.allocator().dupe(u8, self.current.text);
        self.advance();
        return .{ .constant = text };
    }

    if (self.current.type == .identifier) {
        // ... existing wildcard + identifier handling ...
    }

    self.setError("expected variable, constant, or string");
    return error.UnexpectedToken;
}
```

The result is `Term{ .constant = "28" }` — identical to what `"28"` produces.

### parseBodyElement — comparison disambiguation

`isComparisonOp` already checks the *next* token type via `peekNextType`.
A number token followed by a comparison op (e.g., `20 <`) would only occur
if someone wrote `20 < X` with the literal on the left. The current
disambiguation checks `if (self.current.type == .identifier or
self.current.type == .string)` — add `.number`:

```zig
if (self.current.type == .identifier or
    self.current.type == .string or
    self.current.type == .number)
{
    const peek_type = self.peekNextType();
    if (isComparisonOp(peek_type)) {
        // parse as comparison
    }
}
```

This handles `20 < X` as well as `X > 20`.

---

## 4. File Changes

| File | Change |
|---|---|
| `src/datalog.zig` | Add `number` to `TokenType`, add `readNumber` to Lexer, handle `.number` in `parseTerm`, update comparison disambiguation |

One file. ~20 lines.

---

## 5. Tests

### Lexer tests

```zig
test "lexer number literal" {
    var lexer = Lexer.init("42");
    const tok = lexer.next();
    try expectEqual(TokenType.number, tok.type);
    try expectEqualStrings("42", tok.text);
}

test "lexer number in context" {
    var lexer = Lexer.init("points(X, 28).");
    try expectEqual(TokenType.identifier, lexer.next().type); // points
    try expectEqual(TokenType.lparen, lexer.next().type);
    try expectEqual(TokenType.identifier, lexer.next().type); // X
    try expectEqual(TokenType.comma, lexer.next().type);
    try expectEqual(TokenType.number, lexer.next().type);     // 28
    try expectEqual(TokenType.rparen, lexer.next().type);
    try expectEqual(TokenType.dot, lexer.next().type);
}
```

### Parser tests

```zig
test "parser bare number in fact" {
    // points("Alice", 28). → constant "28"
    const rule = ...;
    try expect(rule.head.terms[1] == .constant);
    try expectEqualStrings("28", rule.head.terms[1].constant);
}

test "parser bare number in comparison" {
    // high(X) :- score(X, S), S > 20.
    // comparison.right should be constant "20"
    const cmp = ...;
    try expect(cmp.right == .constant);
    try expectEqualStrings("20", cmp.right.constant);
}

test "parser bare number identical to quoted" {
    // score("Alice", 28) and score("Alice", "28") produce same AST
}

test "parser number on left side of comparison" {
    // check(X) :- val(X), 10 < X.
    // Should parse as comparison with left = constant "10"
}
```

### Evaluator integration test

```zig
test "bitmap eval: bare number in comparison" {
    // points("Alice", 28). points("Bob", 12).
    // high(X) :- points(X, S), S > 20.
    // Should produce: high("Alice")
}
```

---

## 6. What Does NOT Change

- **AST types** — `Term` still has `.variable` and `.constant`. No new variant.
- **Evaluator** — numbers are strings. `numericOrStringCompare` already parses.
- **Bitmap evaluator** — no changes.
- **Stratification** — no changes.
- **Entity key** — no changes.
- **String interner** — no changes.

---

## 7. Checklist

- [ ] Add `number` to `TokenType`
- [ ] Add `readNumber` to Lexer
- [ ] Add digit detection in `Lexer.next()`
- [ ] Handle `.number` in `parseTerm` → `Term{ .constant = text }`
- [ ] Update comparison disambiguation to include `.number`
- [ ] Test: lexer tokenizes bare numbers
- [ ] Test: lexer numbers in context (inside atoms)
- [ ] Test: parser bare number in fact
- [ ] Test: parser bare number in comparison
- [ ] Test: bare number produces same AST as quoted equivalent
- [ ] Test: number on left side of comparison
- [ ] Test: evaluator integration with bare numbers
- [ ] `zig build test` passes
- [ ] `zig build test-all` passes
