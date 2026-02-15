# Step 5a: Relation Module Refactor

## Context

Step 5 (BitmapEvaluator) needs `RelationMap`, `deinitRelations`, `loadRelations`, and temp LMDB helpers — all currently in `bitmap_ingest.zig`. These are relation infrastructure, not ingest-specific. Move them before building the evaluator on top.

Pure mechanical refactor. No behavior change. All existing tests pass unchanged.

## Moves

### 1. `relation.zig` gains RelationMap + lifecycle + LMDB persistence

Move from `bitmap_ingest.zig` to `relation.zig`:

- `pub const RelationMap = std.StringHashMapUnmanaged(Relation);`
- `pub fn deinitRelations(relations: *RelationMap, allocator: std.mem.Allocator) void`
- `pub fn persistRelations(relations: *RelationMap, env: lmdb.Environment, allocator: std.mem.Allocator) !void`
- `pub fn loadRelations(env: lmdb.Environment, allocator: std.mem.Allocator) !RelationMap`
- `fn putBitmap(...)` (internal helper, stays private)
- `fn loadBitmapEntry(...)` (internal helper, stays private)

This adds `lmdb` as an import to `relation.zig`. Natural — a relation module that can serialize itself.

### 2. `test_helpers.zig` for shared test utilities

Extract from `bitmap_ingest.zig`:

- `pub fn createTempLmdbEnv(allocator) !struct { env, path }`
- `pub fn destroyTempLmdbEnv(state, allocator) void`

The evaluator tests will need these too.

### 3. `bitmap_ingest.zig` imports from relation.zig

After the move, `bitmap_ingest.zig`:
- Removes the moved functions
- Imports `RelationMap`, `deinitRelations`, `persistRelations`, `loadRelations` from `relation.zig`
- Imports `createTempLmdbEnv`, `destroyTempLmdbEnv` from `test_helpers.zig`
- Keeps: `ingest`, `buildArgPositions`, `internEntity`, `MockFetcher`, all tests

### 4. `lib.zig` adds test_helpers

```zig
// In test block only:
_ = @import("test_helpers.zig");
```

No public export needed — it's test-only infrastructure.

## What stays where after the refactor

### `relation.zig` (~450 lines, was ~250)
- `BinaryRelation`, `UnaryRelation`, `Relation` (existing)
- `RelationMap` type alias (moved from bitmap_ingest)
- `deinitRelations` (moved)
- `persistRelations` + `putBitmap` (moved)
- `loadRelations` + `loadBitmapEntry` (moved)
- All existing relation tests
- Persist/load roundtrip tests (moved from bitmap_ingest: "persist and load relations roundtrip")

### `bitmap_ingest.zig` (~450 lines, was ~700)
- `ingest` (unchanged)
- `buildArgPositions`, `internEntity` (unchanged)
- `MockFetcher` (unchanged)
- Tests: buildArgPositions, internEntity, ingest binary, ingest multiple mappings, ingest empty, full roundtrip, dedup
- Loses: "persist and load relations roundtrip" test (moves to relation.zig)

### `test_helpers.zig` (~40 lines, new)
- `createTempLmdbEnv`
- `destroyTempLmdbEnv`

## Detailed changes

### relation.zig additions

At the top, add imports:

```zig
const lmdb = @import("lmdb");
```

Add after the existing `Relation` union:

```zig
pub const RelationMap = std.StringHashMapUnmanaged(Relation);

/// Free all relations in a RelationMap, including duped predicate keys.
pub fn deinitRelations(relations: *RelationMap, allocator: std.mem.Allocator) void {
    // ... exact code from bitmap_ingest.zig, unchanged ...
}

/// Persist all relations to the LMDB "bitmaps" database.
pub fn persistRelations(
    relations: *RelationMap,
    env: lmdb.Environment,
    allocator: std.mem.Allocator,
) !void {
    // ... exact code from bitmap_ingest.zig, unchanged ...
}

/// Load all relations from the LMDB "bitmaps" database.
pub fn loadRelations(
    env: lmdb.Environment,
    allocator: std.mem.Allocator,
) !RelationMap {
    // ... exact code from bitmap_ingest.zig, unchanged ...
}

// Internal helpers (private)
fn putBitmap(...) !void { ... }
fn loadBitmapEntry(...) !void { ... }
```

### bitmap_ingest.zig changes

Replace local definitions with imports. `bitmap_ingest.zig` already imports `relation_mod`. Add the new names to that import block and re-export them so the public API of bitmap_ingest is unchanged:

```zig
pub const RelationMap = relation_mod.RelationMap;
pub const deinitRelations = relation_mod.deinitRelations;
pub const persistRelations = relation_mod.persistRelations;
pub const loadRelations = relation_mod.loadRelations;
```

This keeps callers that already import `bitmap_ingest.RelationMap` etc. working. The re-exports are the bridge.

**Important:** `bitmap_ingest.zig` tests still call `persistRelations`, `loadRelations`, `deinitRelations` directly. With re-exports, those calls resolve through the import chain. No test changes needed.

### test_helpers.zig

```zig
const std = @import("std");
const lmdb = @import("lmdb");

pub fn createTempLmdbEnv(allocator: std.mem.Allocator) !struct {
    env: lmdb.Environment,
    path: [:0]const u8,
} {
    // ... exact code from bitmap_ingest.zig, unchanged ...
}

pub fn destroyTempLmdbEnv(state: anytype, allocator: std.mem.Allocator) void {
    // ... exact code from bitmap_ingest.zig, unchanged ...
}
```

### bitmap_ingest.zig test helper replacement

```zig
// Was:
fn createTempLmdbEnv(...) ...
fn destroyTempLmdbEnv(...) ...

// Now:
const test_helpers = @import("test_helpers.zig");
const createTempLmdbEnv = test_helpers.createTempLmdbEnv;
const destroyTempLmdbEnv = test_helpers.destroyTempLmdbEnv;
```

### lib.zig test block

```zig
test {
    std.testing.refAllDecls(@This());
    _ = @import("datalog.zig");
    _ = @import("hypergraph_source.zig");
    _ = @import("string_interner.zig");
    _ = @import("relation.zig");
    _ = @import("fact_fetcher.zig");
    _ = @import("bitmap_ingest.zig");
    _ = @import("test_helpers.zig");  // NEW
}
```

## Test plan

`zig build test` — all existing tests pass with zero changes to test code. The "persist and load relations roundtrip" test moves to `relation.zig` and the `bitmap_ingest.zig` tests that call `persistRelations`/`loadRelations` use re-exports.

No new tests needed. This is a pure code motion refactor.

## Checklist

- [ ] `RelationMap`, `deinitRelations`, `persistRelations`, `loadRelations`, `putBitmap`, `loadBitmapEntry` moved to `relation.zig`
- [ ] `relation.zig` imports `lmdb`
- [ ] "persist and load relations roundtrip" test moved to `relation.zig`
- [ ] `createTempLmdbEnv`, `destroyTempLmdbEnv` extracted to `test_helpers.zig`
- [ ] `bitmap_ingest.zig` re-exports moved types/functions from `relation.zig`
- [ ] `bitmap_ingest.zig` imports test helpers from `test_helpers.zig`
- [ ] `lib.zig` test block includes `test_helpers.zig`
- [ ] `zig build test` passes — all existing tests, zero behavior change
- [ ] No changes to `string_interner.zig`, `fact_fetcher.zig`, `datalog.zig`, `main.zig`
