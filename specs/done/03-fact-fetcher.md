# Step 3: FactFetcher Interface

## Context

The bitmap evaluator's ingest pipeline (step 4) needs raw facts from LMDB. The current `FactSource` + `HypergraphFactSource` return `[]Binding` (string→string maps) because the old evaluator works with string bindings. The bitmap evaluator works with integer IDs — it needs raw `Fact` objects (hyperedges with typed entities) so it can intern the strings and build bitmap relations itself.

This step introduces a new `FactFetcher` interface that returns `[]const Fact` given a `@map` directive. The old `FactSource` and `HypergraphFactSource` remain untouched — both evaluators coexist until step 7.

Depends on: Steps 01-02 conceptually. No code dependency.

## Why a new interface instead of reshaping the old one

The old evaluator uses `FactSource.matchAtom(pattern) → []Binding` at evaluation time, per-query. The bitmap evaluator uses fact fetching at ingest time, per-mapping, once. Different consumers, different shapes. Both need to work simultaneously. Cleanest solution: separate types. At step 7 we delete the old types and optionally rename `FactFetcher` to `FactSource`.

## API

Create `src/fact_fetcher.zig`:

```zig
const std = @import("std");
const Fact = @import("fact.zig").Fact;
const Entity = @import("fact.zig").Entity;
const Mapping = @import("datalog.zig").Mapping;
const FactStore = @import("fact_store.zig").FactStore;

/// Interface for fetching raw facts for a mapping.
/// Used by bitmap ingest to pull facts from storage.
pub const FactFetcher = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        fetchFacts: *const fn (
            ptr: *anyopaque,
            mapping: Mapping,
            allocator: std.mem.Allocator,
        ) anyerror![]const Fact,
    };

    pub fn fetchFacts(
        self: FactFetcher,
        mapping: Mapping,
        allocator: std.mem.Allocator,
    ) ![]const Fact {
        return self.vtable.fetchFacts(self.ptr, mapping, allocator);
    }
};
```

### HypergraphFetcher

The LMDB-backed implementation:

```zig
pub const HypergraphFetcher = struct {
    store: *FactStore,

    pub fn init(store: *FactStore) HypergraphFetcher {
        return .{ .store = store };
    }

    pub fn fetcher(self: *HypergraphFetcher) FactFetcher {
        return .{
            .ptr = self,
            .vtable = &.{ .fetchFacts = fetchFactsImpl },
        };
    }

    fn fetchFactsImpl(
        ptr: *anyopaque,
        mapping: Mapping,
        allocator: std.mem.Allocator,
    ) anyerror![]const Fact {
        const self: *HypergraphFetcher = @ptrCast(@alignCast(ptr));
        return self.fetchFacts(mapping, allocator);
    }

    pub fn fetchFacts(
        self: *HypergraphFetcher,
        mapping: Mapping,
        allocator: std.mem.Allocator,
    ) ![]const Fact { ... }
};
```

## Implementation: HypergraphFetcher.fetchFacts

This is the core logic. Given a `@map` directive, find the anchor entity, query LMDB, filter, return raw facts.

```zig
pub fn fetchFacts(
    self: *HypergraphFetcher,
    mapping: Mapping,
    allocator: std.mem.Allocator,
) ![]const Fact {
    // 1. Find anchor: first constant in the mapping pattern
    const anchor = findAnchor(mapping) orelse return &[_]Fact{};

    // 2. Query LMDB for fact IDs containing this entity
    const fact_ids = try self.store.getFactsByEntity(anchor, allocator);
    defer allocator.free(fact_ids);

    // 3. Fetch each fact, filter by mapping pattern match
    var results = std.ArrayList(Fact).init(allocator);
    errdefer {
        for (results.items) |*f| f.deinit(allocator);
        results.deinit();
    }

    for (fact_ids) |fact_id| {
        const fact = try self.store.getFact(fact_id, allocator) orelse continue;
        errdefer fact.deinit(allocator);

        if (matchesMappingPattern(fact, mapping)) {
            try results.append(fact);
        } else {
            fact.deinit(allocator);
        }
    }

    return results.toOwnedSlice();
}
```

### findAnchor

Extract the first constant entity from the mapping pattern:

```zig
fn findAnchor(mapping: Mapping) ?Entity {
    for (mapping.pattern) |elem| {
        switch (elem.value) {
            .constant => |c| return Entity{ .type = elem.entity_type, .id = c },
            .variable => continue,
        }
    }
    return null;
}
```

If a mapping has no constants (all variables), we return empty. This matches the current behavior. A full-scan fallback could be added later if needed.

### matchesMappingPattern

Check that a fact's entity structure matches the mapping. This filters out unrelated facts that happen to share the same anchor entity:

```zig
fn matchesMappingPattern(fact: Fact, mapping: Mapping) bool {
    // Arity must match
    if (fact.entities.len != mapping.pattern.len) return false;

    // Every pattern element must match positionally
    for (mapping.pattern, fact.entities) |elem, entity| {
        // Entity type must match
        if (!std.mem.eql(u8, elem.entity_type, entity.type)) return false;

        // Constants must match exactly
        switch (elem.value) {
            .constant => |c| {
                if (!std.mem.eql(u8, c, entity.id)) return false;
            },
            .variable => {}, // Variables match anything
        }
    }

    return true;
}
```

## Memory ownership

`fetchFacts` returns facts allocated with the caller's allocator. Each `Fact` in the returned slice owns its entities. The caller is responsible for calling `fact.deinit(allocator)` on each fact and freeing the slice itself.

This matches `FactStore.getFact()` — it already allocates entities into the caller's allocator.

## Tests

All tests in `src/fact_fetcher.zig`.

### MockFetcher for unit testing

```zig
const MockFetcher = struct {
    facts_by_predicate: std.StringHashMapUnmanaged([]const Fact),

    fn init() MockFetcher {
        return .{ .facts_by_predicate = .{} };
    }

    fn addFacts(
        self: *MockFetcher,
        predicate: []const u8,
        facts: []const Fact,
        allocator: std.mem.Allocator,
    ) !void {
        try self.facts_by_predicate.put(allocator, predicate, facts);
    }

    fn fetcher(self: *MockFetcher) FactFetcher {
        return .{
            .ptr = self,
            .vtable = &.{ .fetchFacts = mockFetchImpl },
        };
    }

    fn mockFetchImpl(
        ptr: *anyopaque,
        mapping: Mapping,
        allocator: std.mem.Allocator,
    ) anyerror![]const Fact {
        const self: *MockFetcher = @ptrCast(@alignCast(ptr));
        const stored = self.facts_by_predicate.get(mapping.predicate) orelse
            return &[_]Fact{};

        // Clone facts for caller ownership
        const result = try allocator.alloc(Fact, stored.len);
        for (stored, 0..) |fact, i| {
            result[i] = try fact.clone(allocator);
        }
        return result;
    }
};
```

Note: `Fact.clone()` may not exist yet. If not, the mock can build facts from scratch or we add a simple `clone` to Fact. See implementation notes below.

### Test 1: findAnchor extracts first constant

```zig
test "findAnchor returns first constant entity" {
    // @map influenced(A, B) [rel:influenced-by, author:$A, author:$B]
    const pattern = [_]Mapping.PatternElement{
        .{ .entity_type = "rel", .value = .{ .constant = "influenced-by" } },
        .{ .entity_type = "author", .value = .{ .variable = "A" } },
        .{ .entity_type = "author", .value = .{ .variable = "B" } },
    };
    const mapping = Mapping{
        .predicate = "influenced",
        .args = &.{ "A", "B" },
        .pattern = &pattern,
    };

    const anchor = findAnchor(mapping).?;
    try std.testing.expectEqualStrings("rel", anchor.type);
    try std.testing.expectEqualStrings("influenced-by", anchor.id);
}
```

### Test 2: findAnchor returns null for all-variable pattern

```zig
test "findAnchor returns null for all-variable pattern" {
    const pattern = [_]Mapping.PatternElement{
        .{ .entity_type = "rel", .value = .{ .variable = "R" } },
        .{ .entity_type = "thing", .value = .{ .variable = "X" } },
    };
    const mapping = Mapping{
        .predicate = "any",
        .args = &.{ "R", "X" },
        .pattern = &pattern,
    };

    try std.testing.expectEqual(@as(?Entity, null), findAnchor(mapping));
}
```

### Test 3: matchesMappingPattern accepts matching fact

```zig
test "matchesMappingPattern accepts matching fact" {
    const pattern = [_]Mapping.PatternElement{
        .{ .entity_type = "rel", .value = .{ .constant = "wrote" } },
        .{ .entity_type = "author", .value = .{ .variable = "A" } },
        .{ .entity_type = "book", .value = .{ .variable = "B" } },
    };
    const mapping = Mapping{
        .predicate = "wrote",
        .args = &.{ "A", "B" },
        .pattern = &pattern,
    };

    const entities = [_]Entity{
        .{ .type = "rel", .id = "wrote" },
        .{ .type = "author", .id = "Homer" },
        .{ .type = "book", .id = "Iliad" },
    };
    const fact = Fact{ .id = 1, .entities = &entities, .source = null };

    try std.testing.expect(matchesMappingPattern(fact, mapping));
}
```

### Test 4: matchesMappingPattern rejects wrong arity

```zig
test "matchesMappingPattern rejects wrong arity" {
    const pattern = [_]Mapping.PatternElement{
        .{ .entity_type = "rel", .value = .{ .constant = "wrote" } },
        .{ .entity_type = "author", .value = .{ .variable = "A" } },
    };
    const mapping = Mapping{
        .predicate = "wrote",
        .args = &.{"A"},
        .pattern = &pattern,
    };

    // Fact has 3 entities, mapping expects 2
    const entities = [_]Entity{
        .{ .type = "rel", .id = "wrote" },
        .{ .type = "author", .id = "Homer" },
        .{ .type = "book", .id = "Iliad" },
    };
    const fact = Fact{ .id = 1, .entities = &entities, .source = null };

    try std.testing.expect(!matchesMappingPattern(fact, mapping));
}
```

### Test 5: matchesMappingPattern rejects wrong constant

```zig
test "matchesMappingPattern rejects wrong constant" {
    const pattern = [_]Mapping.PatternElement{
        .{ .entity_type = "rel", .value = .{ .constant = "wrote" } },
        .{ .entity_type = "author", .value = .{ .variable = "A" } },
        .{ .entity_type = "book", .value = .{ .variable = "B" } },
    };
    const mapping = Mapping{
        .predicate = "wrote",
        .args = &.{ "A", "B" },
        .pattern = &pattern,
    };

    // Fact has "translated" instead of "wrote"
    const entities = [_]Entity{
        .{ .type = "rel", .id = "translated" },
        .{ .type = "author", .id = "Lattimore" },
        .{ .type = "book", .id = "Iliad" },
    };
    const fact = Fact{ .id = 1, .entities = &entities, .source = null };

    try std.testing.expect(!matchesMappingPattern(fact, mapping));
}
```

### Test 6: matchesMappingPattern rejects wrong entity type

```zig
test "matchesMappingPattern rejects wrong entity type" {
    const pattern = [_]Mapping.PatternElement{
        .{ .entity_type = "rel", .value = .{ .constant = "wrote" } },
        .{ .entity_type = "author", .value = .{ .variable = "A" } },
        .{ .entity_type = "book", .value = .{ .variable = "B" } },
    };
    const mapping = Mapping{
        .predicate = "wrote",
        .args = &.{ "A", "B" },
        .pattern = &pattern,
    };

    // Second entity is "editor" not "author"
    const entities = [_]Entity{
        .{ .type = "rel", .id = "wrote" },
        .{ .type = "editor", .id = "Homer" },
        .{ .type = "book", .id = "Iliad" },
    };
    const fact = Fact{ .id = 1, .entities = &entities, .source = null };

    try std.testing.expect(!matchesMappingPattern(fact, mapping));
}
```

### Test 7: MockFetcher interface works

```zig
test "FactFetcher interface with mock" {
    const allocator = std.testing.allocator;

    // Build some mock facts
    var mock_entities = [_]Entity{
        .{ .type = "rel", .id = "wrote" },
        .{ .type = "author", .id = "Homer" },
        .{ .type = "book", .id = "Iliad" },
    };
    var mock_facts = [_]Fact{
        .{ .id = 1, .entities = &mock_entities, .source = null },
    };

    var mock = MockFetcher.init();
    try mock.addFacts("wrote", &mock_facts, allocator);
    defer mock.facts_by_predicate.deinit(allocator);

    var f = mock.fetcher();

    const args = [_][]const u8{ "A", "B" };
    const pattern = [_]Mapping.PatternElement{
        .{ .entity_type = "rel", .value = .{ .constant = "wrote" } },
        .{ .entity_type = "author", .value = .{ .variable = "A" } },
        .{ .entity_type = "book", .value = .{ .variable = "B" } },
    };
    const mapping = Mapping{
        .predicate = "wrote",
        .args = &args,
        .pattern = &pattern,
    };

    const facts = try f.fetchFacts(mapping, allocator);
    defer allocator.free(facts);
    // Note: mock returns cloned facts, each needs deinit.
    // If Fact.clone isn't available, mock can return
    // stack-backed facts and skip deinit.

    try std.testing.expectEqual(@as(usize, 1), facts.len);
    try std.testing.expectEqualStrings("Homer", facts[0].entities[1].id);
}
```

## Implementation notes

### Fact.clone

The mock needs to clone facts. Check if `Fact` already has a `clone` method. If not, add one to `src/fact.zig`:

```zig
pub fn clone(self: Fact, allocator: std.mem.Allocator) !Fact {
    const entities = try allocator.alloc(Entity, self.entities.len);
    for (self.entities, 0..) |entity, i| {
        entities[i] = .{
            .type = try allocator.dupe(u8, entity.type),
            .id = try allocator.dupe(u8, entity.id),
        };
    }
    return .{
        .id = self.id,
        .entities = entities,
        .source = if (self.source) |s| try allocator.dupe(u8, s) else null,
    };
}
```

If adding `clone` feels like scope creep, the mock can instead return facts that reference stack/static data (no ownership, no deinit needed). Test 7 already does this with stack-allocated entity arrays. The mock's `fetchFacts` just needs to return a heap-allocated slice of `Fact` structs whose `.entities` point to the stored (stack/static) data. In that case, the caller frees only the outer slice. Just make sure the test documents this ownership difference.

### Integration test (optional)

An LMDB integration test for `HypergraphFetcher` would go in `tests/` like the interner tests. But this requires an ingested LMDB database with facts, which is heavier setup. Skip this for now — step 4 (bitmap_ingest) will be the real integration test of the full pipeline. The unit tests above cover the logic.

## Wire into the build

Add to `src/lib.zig`:

```zig
pub const fact_fetcher = @import("fact_fetcher.zig");
```

And in the test block:

```zig
test {
    // ... existing ...
    _ = @import("fact_fetcher.zig");
}
```

Verify: `zig build test` runs the new tests.

## What NOT to do

- Don't touch `FactSource` in `datalog.zig` — old evaluator still uses it
- Don't touch `HypergraphFactSource` in `hypergraph_source.zig` — same reason
- Don't add persistence or bitmap operations
- Don't handle the no-anchor case (all-variable mappings) — return empty, same as current code
- Don't worry about variable-arity expansion yet (e.g., repeated tag positions) — that's step 4's job when applying the mapping to extract tuples

## Checklist

- [ ] `src/fact_fetcher.zig` created with `FactFetcher` interface and `HypergraphFetcher`
- [ ] `findAnchor` extracts first constant from mapping pattern
- [ ] `matchesMappingPattern` validates fact structure against mapping
- [ ] `HypergraphFetcher.fetchFacts` queries LMDB and filters
- [ ] `MockFetcher` for unit testing without LMDB
- [ ] `Fact.clone` added if needed (or mock uses non-owning approach)
- [ ] Tests 1-7 passing
- [ ] Wired into `lib.zig` and `zig build test` passes
- [ ] No changes to existing `FactSource` or `HypergraphFactSource`
