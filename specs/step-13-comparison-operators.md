# Step 13: Comparison Operators

Adds infix comparison operators (`=`, `!=`, `<`, `>`, `<=`, `>=`) to rule
bodies. Comparisons filter bindings — they never generate new facts, only
discard bindings that don't satisfy the condition.

---

## 0. Motivation

Without comparisons, the only way to filter by value is pattern matching in
atoms. This can't express numeric ranges, inequality checks, or threshold
conditions:

```prolog
% Can't do this today:
stale(R, Age) :- record_age(R, Age), Age > "365".
high_port(H, P) :- connection(H, P), P > "1024".
non_default(H, P) :- setting(H, P), P != "default".
mid_range(I, S) :- rating(I, S), S >= "50", S < "80".
```

These are essential for real-world queries over security data, network logs,
and configuration analysis.

---

## 1. Scope

**In scope:**

- Lexer: new tokens `!=`, `<`, `>`, `<=`, `>=` (reuse existing `=`)
- AST: `Comparison` struct, `CompOp` enum, extend `BodyElement` union
- Parser: disambiguate atoms vs comparisons via one-token lookahead
- Evaluator: comparison checking in rule execution (single-atom and two-atom)
- Stratification: comparisons are transparent (no predicate dependencies)

**Out of scope:**

- Typed comparison via entity_key (future — runtime string parsing works now)
- Comparisons in queries (`?-` lines) — only rule bodies for now
- Numeric literals in the lexer — numbers are still string constants

---

## 2. AST Changes (ast.zig)

### New types

```zig
pub const CompOp = enum {
    eq,     // =
    neq,    // !=
    lt,     // <
    gt,     // >
    le,     // <=
    ge,     // >=
};

pub const Comparison = struct {
    left: Term,
    op: CompOp,
    right: Term,
};
```

### Extend BodyElement

```zig
pub const BodyElement = union(enum) {
    atom: Atom,
    negated_atom: Atom,
    comparison: Comparison,   // ← new variant

    // Existing methods updated:
    pub fn getAtom(self: BodyElement) ?Atom {  // ← returns optional now
        return switch (self) {
            .atom => |a| a,
            .negated_atom => |a| a,
            .comparison => null,
        };
    }

    // format, dupe, free updated to handle .comparison
};
```

**Breaking change to `getAtom`:** Returns `?Atom` instead of `Atom`. Callers
that use `getAtom()` must handle the null case. Currently only `stratify.zig`
calls `getAtom()` — it uses `elem.getAtom()` to extract predicates for the
dependency graph. Comparisons have no predicate, so they should be skipped.

Alternatively, keep `getAtom()` as-is and add the comparison variant with an
unreachable, then use a separate pattern match where comparisons need
handling. But `?Atom` is cleaner and safer.

### Re-exports from datalog.zig

Add to the existing re-export block:

```zig
pub const CompOp = ast.CompOp;
pub const Comparison = ast.Comparison;
```

---

## 3. Lexer Changes (datalog.zig)

### New token types

```zig
pub const TokenType = enum {
    // ... existing ...
    not_equal,    // !=
    less_than,    // <
    greater_than, // >
    less_eq,      // <=
    greater_eq,   // >=
    // Note: .equals already exists for =
};
```

### Lexer.next() additions

In the single-character dispatch section, add handling for `!`, `<`, `>`:

```zig
// != operator
if (c == '!') {
    if (self.peek(1) == '=') {
        self.pos += 2;
        self.col += 2;
        return .{ .type = .not_equal, .text = "!=", .line = self.line, .col = start_col };
    }
    // bare ! is an error
    self.pos += 1;
    self.col += 1;
    return .{ .type = .err, .text = self.source[start..self.pos], .line = self.line, .col = start_col };
}

// < or <=
if (c == '<') {
    if (self.peek(1) == '=') {
        self.pos += 2;
        self.col += 2;
        return .{ .type = .less_eq, .text = "<=", .line = self.line, .col = start_col };
    }
    return self.advance(.less_than);
}

// > or >=
if (c == '>') {
    if (self.peek(1) == '=') {
        self.pos += 2;
        self.col += 2;
        return .{ .type = .greater_eq, .text = ">=", .line = self.line, .col = start_col };
    }
    return self.advance(.greater_than);
}
```

The existing `=` token (`.equals`) is reused for equality comparison. The
parser disambiguates `=` in `@map` context vs comparison context based on
position.

---

## 4. Parser Changes (datalog.zig)

### Lookahead

The parser needs to peek at the next token to disambiguate `identifier(` (atom)
from `identifier op` (comparison). Add a peek method:

```zig
fn peekNextType(self: *Parser) TokenType {
    // Save lexer state
    const saved_pos = self.lexer.pos;
    const saved_line = self.lexer.line;
    const saved_col = self.lexer.col;
    // Peek
    const next_tok = self.lexer.next();
    // Restore
    self.lexer.pos = saved_pos;
    self.lexer.line = saved_line;
    self.lexer.col = saved_col;
    return next_tok.type;
}

fn isComparisonOp(tt: TokenType) bool {
    return switch (tt) {
        .equals, .not_equal, .less_than, .greater_than, .less_eq, .greater_eq => true,
        else => false,
    };
}
```

### Updated parseBodyElement

```zig
fn parseBodyElement(self: *Parser) ParseError!BodyElement {
    // Check for `not` contextual keyword
    if (self.current.type == .identifier and
        std.mem.eql(u8, self.current.text, "not"))
    {
        self.advance();
        const atom = try self.parseAtom();
        return .{ .negated_atom = atom };
    }

    // Lookahead: identifier/string followed by comparison op → comparison
    if (self.current.type == .identifier or self.current.type == .string) {
        if (isComparisonOp(self.peekNextType())) {
            const left = try self.parseTerm();
            const op = try self.parseCompOp();
            const right = try self.parseTerm();
            return .{ .comparison = .{ .left = left, .op = op, .right = right } };
        }
    }

    // Default: parse as atom
    const atom = try self.parseAtom();
    return .{ .atom = atom };
}

fn parseCompOp(self: *Parser) ParseError!CompOp {
    const op: CompOp = switch (self.current.type) {
        .equals => .eq,
        .not_equal => .neq,
        .less_than => .lt,
        .greater_than => .gt,
        .less_eq => .le,
        .greater_eq => .ge,
        else => {
            self.setError("expected comparison operator");
            return error.UnexpectedToken;
        },
    };
    self.advance();
    return op;
}
```

### Disambiguation rules

- `identifier(` → atom (predicate followed by open paren)
- `identifier op` where op is `=`, `!=`, `<`, `>`, `<=`, `>=` → comparison
- `not identifier(` → negated atom
- `identifier.` or `identifier,` → 0-arity would need parens, so this is
  already handled by existing parseAtom expecting `(`

The lookahead is safe because `parseTerm` doesn't consume the comparison
operator — it returns after consuming the identifier/string. Then
`parseCompOp` consumes the operator.

---

## 5. Evaluator Changes (bitmap_evaluator.zig)

### Comparison checking function

The evaluator works with u32 IDs internally. Comparisons need string values,
so we resolve IDs through the interner:

```zig
fn checkComparison(
    self: *BitmapEvaluator,
    cmp: Comparison,
    var_bindings: *const std.StringHashMapUnmanaged(u32),
) bool {
    // Resolve left side to string
    const left_str = self.resolveTermToString(cmp.left, var_bindings) orelse return false;
    const right_str = self.resolveTermToString(cmp.right, var_bindings) orelse return false;

    return switch (cmp.op) {
        .eq => std.mem.eql(u8, left_str, right_str),
        .neq => !std.mem.eql(u8, left_str, right_str),
        .lt, .gt, .le, .ge => numericOrStringCompare(left_str, right_str, cmp.op),
    };
}

fn resolveTermToString(
    self: *BitmapEvaluator,
    term: Term,
    var_bindings: *const std.StringHashMapUnmanaged(u32),
) ?[]const u8 {
    return switch (term) {
        .constant => |c| c,
        .variable => |v| {
            const id = var_bindings.get(v) orelse return null;
            return self.interner.resolve(id);
        },
    };
}

fn numericOrStringCompare(left: []const u8, right: []const u8, op: CompOp) bool {
    // Try numeric comparison first
    const left_int = std.fmt.parseInt(i64, left, 10) catch null;
    const right_int = std.fmt.parseInt(i64, right, 10) catch null;

    if (left_int != null and right_int != null) {
        return switch (op) {
            .lt => left_int.? < right_int.?,
            .gt => left_int.? > right_int.?,
            .le => left_int.? <= right_int.?,
            .ge => left_int.? >= right_int.?,
            else => unreachable,
        };
    }

    // Fall back to lexicographic string comparison
    const order = std.mem.order(u8, left, right);
    return switch (op) {
        .lt => order == .lt,
        .gt => order == .gt,
        .le => order != .gt,
        .ge => order != .lt,
        else => unreachable,
    };
}
```

### Integration into rule execution

`checkAllNegations` becomes `checkAllFilters` (or similar) — it already
iterates body elements looking for `.negated_atom`. Extend it to also check
`.comparison`:

```zig
fn checkAllFilters(
    self: *BitmapEvaluator,
    body: []const BodyElement,
    var_bindings: *const std.StringHashMapUnmanaged(u32),
) bool {
    for (body) |elem| {
        switch (elem) {
            .negated_atom => |neg| {
                if (!self.checkNegation(neg, var_bindings)) return false;
            },
            .comparison => |cmp| {
                if (!self.checkComparison(cmp, var_bindings)) return false;
            },
            .atom => {},
        }
    }
    return true;
}
```

This replaces `checkAllNegations` everywhere it's called in
`executeSingleAtomRule` and `executeTwoAtomRule`. The existing `has_negation`
flag check becomes `has_filters` which is true if any body element is
`.negated_atom` or `.comparison`.

### `countPositiveAtoms` and `getPositiveAtom`

These already skip `.negated_atom` — they'll naturally skip `.comparison` too
since they only match `.atom`. No changes needed if the switch is exhaustive
(Zig requires it).

---

## 6. Stratification Changes (stratify.zig)

Comparisons create no predicate dependencies — they're pure filters on bound
values. The stratification code uses `elem.getAtom()` to extract predicates.
With `getAtom()` returning `?Atom`, comparisons return `null` and are skipped:

```zig
// In buildDepGraph:
for (rule.body) |elem| {
    const atom = elem.getAtom() orelse continue;  // skip comparisons
    // ... existing dependency edge logic ...
}
```

Similarly in `validateSafety` — comparison variables need to be checked for
safety (must be bound by a positive atom), but comparisons don't define
predicates.

### Safety validation for comparison variables

All variables in a comparison must appear in a positive atom in the same rule
body. This is the same safety requirement as negation. Update `validateSafety`
to also check comparison terms:

```zig
.comparison => |cmp| {
    for ([_]Term{ cmp.left, cmp.right }) |term| {
        switch (term) {
            .variable => |v| {
                // check v appears in a positive atom
                if (!isBoundByPositiveAtom(v, rule.body))
                    return error.UnsafeComparison;
            },
            .constant => {},
        }
    }
},
```

---

## 7. Numeric Comparison Semantics

Values are stored as strings. At comparison time:

1. Attempt `std.fmt.parseInt(i64, value, 10)` on both sides
2. If both parse → compare as integers
3. If either fails → compare as strings (lexicographic via `std.mem.order`)

This means:
- `"443" > "80"` → numeric: 443 > 80 → **true**
- `"abc" > "abd"` → string: lexicographic → **false**
- `"443" > "abc"` → mixed: string comparison → result depends on byte values
- `"9" > "10"` → numeric: 9 > 10 → **false** (correct, unlike string "9" > "1")

The numeric-first approach is pragmatic. It handles the common case (port
numbers, timestamps, counts) without requiring type annotations. String
fallback means non-numeric values still work, just with lexicographic
ordering.

---

## 8. File Changes

| File | Change |
|---|---|
| `src/ast.zig` | Add `CompOp`, `Comparison`. Extend `BodyElement` with `.comparison`. Update `getAtom` → `?Atom`. Update `format`, `dupe`, `free`. |
| `src/datalog.zig` | Add 5 token types. Lexer: handle `!`, `<`, `>`. Parser: `peekNextType`, `isComparisonOp`, `parseCompOp`, update `parseBodyElement`. Re-export `CompOp`, `Comparison`. |
| `src/bitmap_evaluator.zig` | Add `checkComparison`, `resolveTermToString`, `numericOrStringCompare`. Rename `checkAllNegations` → `checkAllFilters`. Update `hasNegatedAtoms` → `hasFilters`. |
| `src/stratify.zig` | Handle `?Atom` from `getAtom`. Add comparison variable safety check. |

---

## 9. Tests

### 9a. Lexer tests (datalog.zig)

```zig
test "lexer comparison operators" {
    var lexer = Lexer.init("X != Y. A < B. C >= D. E <= F. G > H.");
    try expectEqual(TokenType.identifier, lexer.next().type); // X
    try expectEqual(TokenType.not_equal, lexer.next().type);
    try expectEqual(TokenType.identifier, lexer.next().type); // Y
    // ... etc for all operators
}
```

### 9b. Parser tests (datalog.zig)

```zig
test "parser comparison in rule body" {
    // stale(R, Age) :- record_age(R, Age), Age > "365".
    // Body should have: atom + comparison
    // comparison.left = variable "Age", .op = .gt, .right = constant "365"
}

test "parser chained comparisons" {
    // mid_range(I, S) :- rating(I, S), S >= "50", S < "80".
    // Body should have: atom + comparison + comparison
}

test "parser equality comparison" {
    // eq_check(X, Y) :- pair(X, Y), X = Y.
    // Disambiguation: X = Y is a comparison, not a @map equals
}

test "parser not-equal comparison" {
    // neq_check(X, Y) :- pair(X, Y), X != Y.
}
```

### 9c. Evaluator integration tests (bitmap_evaluator.zig)

```zig
test "bitmap eval: numeric greater-than comparison" {
    // val("5"). val("10"). val("20"). val("3").
    // big(X) :- val(X), X > "4".
    // → big: "5", "10", "20"
}

test "bitmap eval: equality comparison" {
    // pair("a", "a"). pair("a", "b"). pair("b", "b").
    // same(X, Y) :- pair(X, Y), X = Y.
    // → same: ("a","a"), ("b","b")
}

test "bitmap eval: not-equal comparison" {
    // pair("a", "a"). pair("a", "b"). pair("b", "b").
    // diff(X, Y) :- pair(X, Y), X != Y.
    // → diff: ("a","b")
}

test "bitmap eval: chained numeric range" {
    // val("1"). val("50"). val("75"). val("100").
    // mid(X) :- val(X), X >= "50", X < "100".
    // → mid: "50", "75"
}

test "bitmap eval: comparison with negation" {
    // val("5"). val("10"). val("20").
    // excluded("10").
    // big_not_excluded(X) :- val(X), X > "4", not excluded(X).
    // → "5", "20"
}

test "bitmap eval: string comparison fallback" {
    // word("apple"). word("banana"). word("cherry").
    // after_b(X) :- word(X), X > "banana".
    // → "cherry"
}
```

### 9d. Stratification tests (stratify.zig)

```zig
test "stratify: comparisons don't create dependencies" {
    // Comparisons should not affect stratification
}

test "unsafe comparison variable rejected" {
    // bad(X) :- val(Y), X > "10".
    // X is not bound by any positive atom → error
}
```

---

## 10. What Does NOT Change

- **Entity key** — future typed comparisons will use `entity_key.compareValues()`,
  but this step uses runtime string parsing
- **String interner** — unrelated
- **Fact store** — unrelated
- **Bitmap ingest** — unrelated
- **Relation types** — unrelated
- **Main.zig** — no changes needed (comparisons only in rule bodies, not queries)

---

## 11. Checklist

- [ ] `ast.zig`: Add `CompOp` enum
- [ ] `ast.zig`: Add `Comparison` struct
- [ ] `ast.zig`: Extend `BodyElement` with `.comparison` variant
- [ ] `ast.zig`: Update `getAtom` to return `?Atom`
- [ ] `ast.zig`: Update `format`, `dupe`, `free` for `.comparison`
- [ ] `datalog.zig`: Add `not_equal`, `less_than`, `greater_than`, `less_eq`, `greater_eq` to `TokenType`
- [ ] `datalog.zig`: Lexer handles `!`, `<`, `>` with lookahead for `=`
- [ ] `datalog.zig`: Add `peekNextType` method
- [ ] `datalog.zig`: Add `isComparisonOp`, `parseCompOp`
- [ ] `datalog.zig`: Update `parseBodyElement` with comparison disambiguation
- [ ] `datalog.zig`: Re-export `CompOp`, `Comparison`
- [ ] `bitmap_evaluator.zig`: Add `checkComparison`, `resolveTermToString`, `numericOrStringCompare`
- [ ] `bitmap_evaluator.zig`: Rename `checkAllNegations` → `checkAllFilters`
- [ ] `bitmap_evaluator.zig`: Update `hasNegatedAtoms` → `hasFilters`
- [ ] `stratify.zig`: Handle `?Atom` from `getAtom` (skip comparisons)
- [ ] `stratify.zig`: Add safety validation for comparison variables
- [ ] Test: lexer tokenizes all comparison operators
- [ ] Test: parser disambiguates atom vs comparison
- [ ] Test: parser handles chained comparisons
- [ ] Test: evaluator numeric greater-than
- [ ] Test: evaluator equality
- [ ] Test: evaluator not-equal
- [ ] Test: evaluator chained numeric range
- [ ] Test: evaluator comparison + negation combined
- [ ] Test: evaluator string comparison fallback
- [ ] Test: stratification ignores comparisons
- [ ] Test: unsafe comparison variable rejected
- [ ] `zig build test` passes
- [ ] `zig build test-all` passes
