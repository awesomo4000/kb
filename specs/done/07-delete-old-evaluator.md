# Step 7: Delete Old Evaluator

## Context

The bitmap evaluator is fully wired up and passing all e2e tests (step 6). The old binding-based `Evaluator`, `MemoryFactSource`, `FactSource` vtable, and `HypergraphFactSource` were kept around as a safety net during the transition. They are now dead code — nothing in `main.zig` or the test suite references them. Remove them.

Pure deletion. No behavior change. All existing tests pass unchanged.

## Deletions

### 1. Delete `src/hypergraph_source.zig` entirely

This file contains `HypergraphFactSource`, which adapts the old `FactSource` vtable (returning `[]Binding`). It has been replaced by `HypergraphFetcher` in `fact_fetcher.zig` (returning `[]Fact`).

Nothing imports it except `lib.zig`.

### 2. Trim `src/datalog.zig` — remove old evaluator and supporting types

Remove everything below the `Parser` section. Specifically, delete:

- `Binding` type alias (`pub const Binding = std.StringHashMap([]const u8);`)
- `FactSource` struct (vtable interface with `matchAtom`)
- `MemoryFactSource` struct (in-memory fact storage)
- `AtomContext` struct (hash map context for Atom dedup)
- `ArgKey` struct and `ArgKeyContext` struct (argument index keys)
- `RuleProfile` struct (per-rule profiling stats)
- `TimingStats` struct (global timing stats)
- `Evaluator` struct (the entire old evaluator — `init`, `initWithSource`, `enableProfiling`, `printProfile`, `deinit`, `addFact`, `indexFact`, `factExistsInDerived`, `factExists`, `preloadBaseFacts`, `evaluate`, `addToDelta`, `hasDeltaFacts`, `matchBodySemiNaive`, `matchAtomInDelta`, `query`, `matchAtom`, `matchAtomAgainstList`, `matchBody`, `substituteAtom`, `substitute`, `mergeBindings`)

Also delete these test blocks that exercise the old evaluator:

- `test "evaluator simple query"`
- `test "evaluator transitive closure"`
- `test "evaluator member_of transitive"`
- `test "books and literary influence"`

**Keep everything else in `datalog.zig`:**

- AST types: `Term`, `Atom`, `Rule`, `Mapping`
- `Lexer` struct
- `Parser` struct (including `ParseError`, `ParseResult`, `AccumulatedParse` support)
- All lexer tests: `"lexer basic tokens"`, `"lexer strings"`, `"lexer turnstile and query"`, `"lexer comments"`, `"lexer @map tokens"`
- All parser tests: `"parser simple fact"`, `"parser rule with body"`, `"parser query"`, `"parser @map directive"`, `"parser error formatting"`

### 3. Update `src/lib.zig`

Remove the `HypergraphFactSource` import and the `hypergraph_source.zig` test reference:

```zig
// DELETE this line:
pub const HypergraphFactSource = @import("hypergraph_source.zig").HypergraphFactSource;

// DELETE from the test block:
_ = @import("hypergraph_source.zig");
```

Everything else in `lib.zig` stays: `FactStore`, `Fact`, `Entity`, `datalog`, `StringInterner`, `relation`, `fact_fetcher`, `bitmap_ingest`, `bitmap_evaluator`, `test_helpers`.

## Files NOT changed

- `src/main.zig` — already uses `BitmapEvaluator` + `HypergraphFetcher`, no old evaluator references
- `src/bitmap_evaluator.zig` — no dependency on old evaluator
- `src/bitmap_ingest.zig` — no dependency on old evaluator
- `src/fact_fetcher.zig` — no dependency on old evaluator
- `src/fact_store.zig` — no dependency on old evaluator
- `src/fact.zig` — no dependency on old evaluator
- `src/relation.zig` — no dependency on old evaluator
- `src/string_interner.zig` — no dependency on old evaluator
- `src/test_helpers.zig` — no dependency on old evaluator
- `tests/e2e.sh` — tests the new bitmap evaluator path
- `build.zig` / `build.zig.zon` — no changes needed

## What `datalog.zig` looks like after

The file retains:

1. **AST types** (~170 lines): `Term`, `Atom`, `Rule`, `Mapping` with their `eql`, `hash`, `format`, `dupe`, `free` methods
2. **Lexer** (~130 lines): `TokenType`, `Token`, `Lexer` with tokenization logic
3. **Parser** (~250 lines): `ParseError`, `Parser` with `parseProgram`, `parseRule`, `parseBody`, `parseAtom`, `parseTerm`, `parseMapping`, error formatting
4. **Lexer tests** (~70 lines): 5 test blocks
5. **Parser tests** (~80 lines): 5 test blocks

Estimated total: ~700 lines (down from ~1,600). The file becomes a clean parser module with no evaluation logic.

## Test plan

- `zig build test` — all remaining tests pass (lexer, parser, string_interner, relation, fact_fetcher, bitmap_ingest, bitmap_evaluator, test_helpers)
- `zig build test-all` — integration tests pass
- `tests/e2e.sh` — all 4 e2e tests pass (ingest, entity queries, datalog with @map, pure datalog)
- `zig build` — binary builds clean

No new tests needed. This is pure dead code removal.

## Checklist

- [ ] `src/hypergraph_source.zig` deleted
- [ ] `src/datalog.zig`: `Binding`, `FactSource`, `MemoryFactSource` removed
- [ ] `src/datalog.zig`: `AtomContext`, `ArgKey`, `ArgKeyContext` removed
- [ ] `src/datalog.zig`: `RuleProfile`, `TimingStats` removed
- [ ] `src/datalog.zig`: `Evaluator` struct removed entirely
- [ ] `src/datalog.zig`: 4 old evaluator test blocks removed
- [ ] `src/datalog.zig`: AST types, Lexer, Parser, and their tests preserved
- [ ] `src/lib.zig`: `HypergraphFactSource` import removed
- [ ] `src/lib.zig`: `hypergraph_source.zig` removed from test block
- [ ] `zig build` succeeds
- [ ] `zig build test` passes
- [ ] `zig build test-all` passes
- [ ] `tests/e2e.sh` passes (all 4 tests)
