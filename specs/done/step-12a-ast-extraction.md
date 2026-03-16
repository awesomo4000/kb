# Step 12a: Extract AST types from datalog.zig

> **STATUS: DONE** — Merged in PR #30 (commit d0c26e7).

Pure refactor — no behavioral changes. Moves AST type definitions out of
`datalog.zig` into a dedicated `ast.zig` module to separate data types from
parsing logic. Prepares for the comparison operator step, which adds new AST
types (`Comparison`, `CompOp`) and new tokens to the lexer simultaneously.

---

## 0. Motivation

`datalog.zig` is currently 39KB containing three distinct concerns:

1. **AST types** — `Term`, `Atom`, `BodyElement`, `Rule`, `Mapping`, `Binding`
2. **Lexer** — `TokenType`, `Token`, `Lexer`
3. **Parser** — `ParseError`, `Parser`, `ParseResult`

These have different consumers:

| Consumer | Needs AST types | Needs Lexer/Parser |
|---|---|---|
| `bitmap_evaluator.zig` | `Rule`, `Atom`, `Term`, `Binding`, `Mapping`, `BodyElement` | no |
| `stratify.zig` | `Rule`, `BodyElement` | no |
| `bitmap_ingest.zig` | `Mapping` | no |
| `main.zig` | all AST types | `Parser`, `ParseResult` |

Every consumer imports AST types. Only `main.zig` needs the parser. This
becomes a problem with comparison operators: the diff will mix AST additions
(`Comparison`, `CompOp`, extending `BodyElement`) with lexer additions
(`!=`, `<`, `>`, `<=`, `>=`) and parser disambiguation logic in a single
file. Splitting first makes the comparison operator diff cleaner — AST
changes in `ast.zig`, lexer/parser changes in `datalog.zig`.

---

## 1. What Moves to `ast.zig`

Everything in the current `// AST Types` section:

```
Term           (union: variable, constant; methods: eql, format, dupe, free)
Atom           (struct: predicate, terms; methods: eql, hash, format, dupe, free)
BodyElement    (union: atom, negated_atom; methods: getAtom, isNegated, format, dupe, free)
Rule           (struct: head, body; methods: format, dupe, free)
Mapping        (struct: predicate, args, pattern, PatternElement; method: free)
Binding        (type alias: std.StringHashMap([]const u8))
```

This is a clean cut — these types have no dependency on the Lexer or Parser.
They only depend on `std`.

---

## 2. What Stays in `datalog.zig`

```
TokenType      (enum)
Token          (struct)
Lexer          (struct with all methods)
ParseError     (error set)
Parser         (struct with all methods, including ParseResult)
```

The Parser produces AST types, so `datalog.zig` imports `ast.zig`. The
dependency is one-way: `ast.zig` has zero imports from `datalog.zig`.

---

## 3. Import Updates

### `datalog.zig`

Add at top:
```zig
const ast = @import("ast.zig");
pub const Term = ast.Term;
pub const Atom = ast.Atom;
pub const BodyElement = ast.BodyElement;
pub const Rule = ast.Rule;
pub const Mapping = ast.Mapping;
pub const Binding = ast.Binding;
```

This preserves backward compatibility — existing code that does
`const Term = datalog.Term` continues to work unchanged. The re-exports
mean no downstream file needs to change its imports in this step.

### `lib.zig`

Add:
```zig
pub const ast = @import("ast.zig");
```

And in the test block:
```zig
_ = @import("ast.zig");
```

### All other files

**No changes.** The re-exports from `datalog.zig` preserve the existing
import paths. Files that do `const Rule = datalog.Rule` continue to work
because `datalog.Rule` is re-exported from `ast.Rule`.

---

## 4. File Changes

| File | Change |
|---|---|
| `src/ast.zig` | **New.** Term, Atom, BodyElement, Rule, Mapping, Binding |
| `src/datalog.zig` | Remove AST type definitions, add `ast.zig` import + re-exports |
| `src/lib.zig` | Add `ast` export and test import |

---

## 5. Checklist

- [x] Create `src/ast.zig` with Term, Atom, BodyElement, Rule, Mapping, Binding
- [x] Remove AST type definitions from `src/datalog.zig`
- [x] Add `ast.zig` import and re-exports to `src/datalog.zig`
- [x] Add `ast` export to `src/lib.zig`
- [x] Add `ast.zig` to test block in `src/lib.zig`
- [x] `zig build test` passes
- [x] `zig build test-all` passes
- [x] No downstream import changes needed
