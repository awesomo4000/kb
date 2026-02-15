# Step 4: Bitmap Ingest

## Context

Steps 1–3 built the pieces: StringInterner (string↔u32), Relation types (bitmap-backed binary/unary relations), and FactFetcher (raw facts from LMDB). Step 4 connects them into the ingest pipeline.

The ingest pipeline is the "build step" — it runs once, transforms raw hypergraph facts into interned bitmap relations, and persists everything to LMDB. After ingest, the evaluator (step 5) loads pre-built bitmaps and operates entirely on integers.

Depends on: Steps 01 (StringInterner), 02 (Relation), 03 (FactFetcher).

## What this step does

For each `@map` directive in the rules:

1. Fetch matching facts from LMDB via FactFetcher
2. For each fact, extract variable positions per the mapping to form tuples
3. Intern each entity value as `"type\x00id"` → u32
4. Insert tuples into the appropriate Relation (unary or binary)
5. Persist all relations and the interner to LMDB

After this step, LMDB contains:
- The original fact store (unchanged)
- `intern_fwd` / `intern_rev` (from step 1)
- `bitmaps` database (new — serialized roaring bitmaps for each relation)

## API

Create `src/bitmap_ingest.zig`:

```zig
const std = @import("std");
const lmdb = @import("lmdb");
const rawr = @import("rawr");
const StringInterner = @import("string_interner.zig").StringInterner;
const relation_mod = @import("relation.zig");
const BinaryRelation = relation_mod.BinaryRelation;
const UnaryRelation = relation_mod.UnaryRelation;
const Relation = relation_mod.Relation;
const FactFetcher = @import("fact_fetcher.zig").FactFetcher;
const Mapping = @import("datalog.zig").Mapping;
const Fact = @import("fact.zig").Fact;
const Entity = @import("fact.zig").Entity;

pub const RelationMap = std.StringHashMapUnmanaged(Relation);

/// Run the full ingest pipeline: fetch facts, intern, build relations.
/// Does NOT persist — call persistRelations and interner.persist separately.
pub fn ingest(
    ff: FactFetcher,
    mappings: []const Mapping,
    interner: *StringInterner,
    relations: *RelationMap,
    allocator: std.mem.Allocator,
) !void

/// Persist all relations to the LMDB "bitmaps" database.
pub fn persistRelations(
    relations: *RelationMap,
    env: lmdb.Environment,
    allocator: std.mem.Allocator,
) !void

/// Load all relations from the LMDB "bitmaps" database.
/// Returns empty map if database doesn't exist (first run).
pub fn loadRelations(
    env: lmdb.Environment,
    allocator: std.mem.Allocator,
) !RelationMap

/// Free all relations in a RelationMap.
pub fn deinitRelations(relations: *RelationMap, allocator: std.mem.Allocator) void
```

## Tuple Extraction

Given a Fact and a Mapping, extract the tuple of u32 IDs.

### How mapping variables connect to fact positions

Consider `@map influenced_by(A, B) = [rel:"influenced-by", author:A, author:B]`:

- `mapping.args` = `["A", "B"]` — defines the tuple columns
- `mapping.pattern[0]` = `{type: "rel", value: .constant("influenced-by")}` — skip (anchor)
- `mapping.pattern[1]` = `{type: "author", value: .variable("A")}` — tuple column 0
- `mapping.pattern[2]` = `{type: "author", value: .variable("B")}` — tuple column 1

For fact `[rel:influenced-by, author:Virgil, author:Homer]`:
- Variable "A" is at pattern position 1 → `fact.entities[1]` = `author:Virgil`
- Variable "B" is at pattern position 2 → `fact.entities[2]` = `author:Homer`
- Intern `"author\x00Virgil"` → some u32, intern `"author\x00Homer"` → some u32
- Insert `(id_virgil, id_homer)` into `BinaryRelation` for predicate `"influenced_by"`

### Algorithm

```zig
/// Build a lookup: for each arg name, which pattern position has that variable?
/// Returns array parallel to mapping.args: arg_positions[i] = pattern index for args[i].
fn buildArgPositions(mapping: Mapping, allocator: std.mem.Allocator) ![]usize {
    const positions = try allocator.alloc(usize, mapping.args.len);
    for (mapping.args, 0..) |arg, i| {
        positions[i] = for (mapping.pattern, 0..) |elem, j| {
            switch (elem.value) {
                .variable => |v| {
                    if (std.mem.eql(u8, v, arg)) break j;
                },
                .constant => {},
            }
        } else {
            return error.MappingArgNotInPattern; // arg name not found in pattern
        };
    }
    return positions;
}

/// Intern an entity as "type\x00id".
fn internEntity(interner: *StringInterner, entity: Entity) !u32 {
    // Build "type\x00id" key
    // Use stack buffer for common case, arena for long keys
    var buf: [512]u8 = undefined;
    const key = if (entity.type.len + 1 + entity.id.len <= buf.len) blk: {
        @memcpy(buf[0..entity.type.len], entity.type);
        buf[entity.type.len] = 0;
        @memcpy(buf[entity.type.len + 1 ..][0..entity.id.len], entity.id);
        break :blk buf[0 .. entity.type.len + 1 + entity.id.len];
    } else {
        // Fallback: this only constructs the key temporarily for intern lookup.
        // The interner will dupe it into its arena if it's new.
        // We need a temp allocation here — use the interner's arena since
        // intern() dupes anyway and this temp is only needed for the lookup.
        @panic("entity key too long for stack buffer");
        // In practice, entity keys are well under 512 bytes.
        // Add heap fallback if this ever triggers.
    };

    return interner.intern(key);
}
```

### Ingest loop

```zig
pub fn ingest(
    ff: FactFetcher,
    mappings: []const Mapping,
    interner: *StringInterner,
    relations: *RelationMap,
    allocator: std.mem.Allocator,
) !void {
    for (mappings) |mapping| {
        // 1. Determine relation arity
        const arity = mapping.args.len;
        if (arity == 0 or arity > 2) continue; // skip unsupported arities

        // 2. Build arg → pattern position lookup
        const arg_positions = try buildArgPositions(mapping, allocator);
        defer allocator.free(arg_positions);

        // 3. Ensure relation exists in the map
        const rel_gop = try relations.getOrPut(allocator, mapping.predicate);
        if (!rel_gop.found_existing) {
            rel_gop.value_ptr.* = switch (arity) {
                1 => .{ .unary = try UnaryRelation.init(allocator) },
                2 => .{ .binary = try BinaryRelation.init(allocator) },
                else => unreachable,
            };
        }

        // 4. Fetch facts matching this mapping
        const facts = try ff.fetchFacts(mapping, allocator);
        defer {
            for (facts) |f| f.deinit(allocator);
            allocator.free(facts);
        }

        // 5. For each fact, extract tuple and insert
        for (facts) |fact| {
            switch (arity) {
                1 => {
                    const id = try internEntity(interner, fact.entities[arg_positions[0]]);
                    try rel_gop.value_ptr.unary.insert(id);
                },
                2 => {
                    const a = try internEntity(interner, fact.entities[arg_positions[0]]);
                    const b = try internEntity(interner, fact.entities[arg_positions[1]]);
                    try rel_gop.value_ptr.binary.insert(a, b);
                },
                else => unreachable,
            }
        }
    }
}
```

## Relation Persistence

### LMDB key format

All relation bitmaps go in a single LMDB database called `"bitmaps"`.

Key format uses `\x00` separators:

| Relation type | Component | Key | Value |
|---|---|---|---|
| Binary | domain | `pred\x00domain` | serialized bitmap |
| Binary | range | `pred\x00range` | serialized bitmap |
| Binary | forward entry | `pred\x00fwd\x00{u32 big-endian}` | serialized bitmap |
| Binary | reverse entry | `pred\x00rev\x00{u32 big-endian}` | serialized bitmap |
| Unary | members | `pred\x00members` | serialized bitmap |

Big-endian u32 in forward/reverse keys ensures LMDB's byte-order comparison groups entries for the same predicate together.

### persistRelations

```zig
pub fn persistRelations(
    relations: *RelationMap,
    env: lmdb.Environment,
    allocator: std.mem.Allocator,
) !void {
    const txn = try env.transaction(.{ .mode = .ReadWrite });
    errdefer txn.abort();

    const bm_db = try txn.database("bitmaps", .{ .create = true });

    var rel_iter = relations.iterator();
    while (rel_iter.next()) |entry| {
        const pred = entry.key_ptr.*;
        switch (entry.value_ptr.*) {
            .binary => |*bin| {
                // Domain
                try putBitmap(bm_db, pred, "domain", null, &bin.domain, allocator);
                // Range
                try putBitmap(bm_db, pred, "range", null, &bin.range, allocator);
                // Forward entries
                var fwd_iter = bin.forward.iterator();
                while (fwd_iter.next()) |fwd_entry| {
                    try putBitmap(bm_db, pred, "fwd", fwd_entry.key_ptr.*, fwd_entry.value_ptr, allocator);
                }
                // Reverse entries
                var rev_iter = bin.reverse.iterator();
                while (rev_iter.next()) |rev_entry| {
                    try putBitmap(bm_db, pred, "rev", rev_entry.key_ptr.*, rev_entry.value_ptr, allocator);
                }
            },
            .unary => |*un| {
                try putBitmap(bm_db, pred, "members", null, &un.members, allocator);
            },
        }
    }

    try txn.commit();
}

/// Serialize a bitmap and store it in LMDB.
fn putBitmap(
    db: anytype, // LMDB database handle
    pred: []const u8,
    component: []const u8,
    entity_id: ?u32,
    bitmap: *rawr.RoaringBitmap,
    allocator: std.mem.Allocator,
) !void {
    // Build key: "pred\x00component[\x00{u32_be}]"
    var key_buf: [512]u8 = undefined;
    var pos: usize = 0;
    @memcpy(key_buf[pos..][0..pred.len], pred);
    pos += pred.len;
    key_buf[pos] = 0;
    pos += 1;
    @memcpy(key_buf[pos..][0..component.len], component);
    pos += component.len;

    if (entity_id) |eid| {
        key_buf[pos] = 0;
        pos += 1;
        const be_bytes = std.mem.toBytes(std.mem.nativeToBig(u32, eid));
        @memcpy(key_buf[pos..][0..4], &be_bytes);
        pos += 4;
    }

    const key = key_buf[0..pos];

    // Serialize bitmap
    const bytes = try bitmap.serialize(allocator);
    defer allocator.free(bytes);

    try db.set(key, bytes);
}
```

### loadRelations

```zig
pub fn loadRelations(
    env: lmdb.Environment,
    allocator: std.mem.Allocator,
) !RelationMap {
    var relations: RelationMap = .{};
    errdefer deinitRelations(&relations, allocator);

    const txn = try env.transaction(.{ .mode = .ReadOnly });
    defer txn.abort();

    const bm_db = txn.database("bitmaps", .{}) catch |err| switch (err) {
        error.MDB_NOTFOUND => return relations,
        else => return err,
    };

    var cursor = try bm_db.cursor();
    defer cursor.deinit();

    // Iterate all entries
    _ = try cursor.goToFirst() orelse return relations;

    while (true) {
        const entry = try cursor.getCurrentEntry();
        try loadBitmapEntry(&relations, entry.key, entry.value, allocator);
        _ = try cursor.goToNext() orelse break;
    }

    return relations;
}
```

The `loadBitmapEntry` function parses the key to determine predicate, component, and optional entity ID, then deserializes the bitmap into the appropriate relation field. Key parsing:

```zig
fn loadBitmapEntry(
    relations: *RelationMap,
    key: []const u8,
    value: []const u8,
    allocator: std.mem.Allocator,
) !void {
    // Parse key: split on \x00
    // key[0..first_null] = predicate
    // key[first_null+1..second_null or end] = component
    // key[second_null+1..] = optional entity_id (4 bytes, big-endian)

    const first_null = std.mem.indexOfScalar(u8, key, 0) orelse return;
    const pred = key[0..first_null];
    const rest = key[first_null + 1 ..];

    const second_null = std.mem.indexOfScalar(u8, rest, 0);
    const component = if (second_null) |sn| rest[0..sn] else rest;
    const entity_id: ?u32 = if (second_null) |sn| blk: {
        if (rest[sn + 1 ..].len >= 4) {
            break :blk std.mem.bigToNative(u32, std.mem.bytesToValue(u32, rest[sn + 1 ..][0..4]));
        }
        break :blk null;
    } else null;

    // Ensure relation exists — IMPORTANT: dupe pred before getOrPut (see Implementation Note)
    const owned_pred = try allocator.dupe(u8, pred);
    const gop = try relations.getOrPut(allocator, owned_pred);
    if (!gop.found_existing) {
        // Determine arity from component name
        if (std.mem.eql(u8, component, "members")) {
            gop.value_ptr.* = .{ .unary = try UnaryRelation.init(allocator) };
        } else {
            gop.value_ptr.* = .{ .binary = try BinaryRelation.init(allocator) };
        }
    } else {
        // Key already existed — free the dupe we just made
        allocator.free(owned_pred);
    }

    // Deserialize bitmap and assign to the right field
    var bitmap = try rawr.RoaringBitmap.deserialize(allocator, value);

    if (std.mem.eql(u8, component, "members")) {
        gop.value_ptr.unary.members.deinit();
        gop.value_ptr.unary.members = bitmap;
    } else if (std.mem.eql(u8, component, "domain")) {
        gop.value_ptr.binary.domain.deinit();
        gop.value_ptr.binary.domain = bitmap;
    } else if (std.mem.eql(u8, component, "range")) {
        gop.value_ptr.binary.range.deinit();
        gop.value_ptr.binary.range = bitmap;
    } else if (std.mem.eql(u8, component, "fwd")) {
        const eid = entity_id orelse return;
        try gop.value_ptr.binary.forward.put(allocator, eid, bitmap);
    } else if (std.mem.eql(u8, component, "rev")) {
        const eid = entity_id orelse return;
        try gop.value_ptr.binary.reverse.put(allocator, eid, bitmap);
    } else {
        bitmap.deinit(); // unknown component, discard
    }
}
```

### deinitRelations

```zig
pub fn deinitRelations(relations: *RelationMap, allocator: std.mem.Allocator) void {
    var iter = relations.iterator();
    while (iter.next()) |entry| {
        allocator.free(entry.key_ptr.*);
        entry.value_ptr.deinit();
    }
    relations.deinit(allocator);
}
```

## Memory Ownership

| Data | Owner | Lifetime |
|---|---|---|
| Facts from FactFetcher | Caller frees after processing each mapping | Per-mapping |
| arg_positions array | Caller frees after processing each mapping | Per-mapping |
| Interned strings | StringInterner arena | Until interner.deinit() |
| Relations + bitmaps | Caller-provided allocator | Until deinitRelations() |
| Serialized bitmap bytes | Temp allocation, freed after LMDB put | Per-bitmap persist |
| RelationMap keys | Duped into caller's allocator | Until deinitRelations() |

**Key ownership rule:** Both `ingest()` and `loadRelations()` dupe predicate strings into the caller's allocator. `deinitRelations()` always frees these keys. This avoids ambiguity about whether keys point to parser arena memory or heap memory.

In `ingest()`, when inserting a new relation:
```zig
const owned_pred = try allocator.dupe(u8, mapping.predicate);
const rel_gop = try relations.getOrPut(allocator, owned_pred);
// If already existed, free the dupe:
if (rel_gop.found_existing) allocator.free(owned_pred);
```

## Implementation Note: LMDB Cursor Key Lifetime

In `loadBitmapEntry`, the `key` and `value` slices come from an LMDB cursor (`getCurrentEntry`). These point into LMDB's mmap'd pages and **may become invalid when the cursor moves to the next entry**. This means:

- The predicate string parsed from the key must be duped **before** the next `cursor.goToNext()` call — but since `loadBitmapEntry` is called per-entry with the key passed in, this should be safe as long as the dupe happens inside that call before returning.
- The bitmap bytes (`value`) are passed to `RoaringBitmap.deserialize`, which copies the data into its own containers. So the value slice doesn't need to outlive the call.
- However: verify that `relations.getOrPut(allocator, pred)` doesn't store a dangling pointer. If `pred` is a slice into cursor memory, and `getOrPut` stores that slice as the key, it will dangle after the cursor moves. **The dupe of `pred` must happen before `getOrPut`**, not after. The code sketches above show this correctly but it's a subtle ordering constraint — get it wrong and you get silent corruption.

If unsure about the Zig LMDB wrapper's lifetime guarantees, test by adding a second entry and verifying the first entry's key is still valid after the cursor advances. If it's not, the dupe-before-getOrPut pattern is mandatory.

## Tests

All tests in `src/bitmap_ingest.zig`.

### Test 1: buildArgPositions

```zig
test "buildArgPositions maps args to pattern positions" {
    const allocator = std.testing.allocator;

    // @map influenced_by(A, B) = [rel:"influenced-by", author:A, author:B]
    const args = [_][]const u8{ "A", "B" };
    const pattern = [_]Mapping.PatternElement{
        .{ .entity_type = "rel", .value = .{ .constant = "influenced-by" } },
        .{ .entity_type = "author", .value = .{ .variable = "A" } },
        .{ .entity_type = "author", .value = .{ .variable = "B" } },
    };
    const mapping = Mapping{
        .predicate = "influenced_by",
        .args = &args,
        .pattern = &pattern,
    };

    const positions = try buildArgPositions(mapping, allocator);
    defer allocator.free(positions);

    try std.testing.expectEqual(@as(usize, 1), positions[0]); // A at pattern pos 1
    try std.testing.expectEqual(@as(usize, 2), positions[1]); // B at pattern pos 2
}
```

### Test 2: internEntity produces type\x00id keys

```zig
test "internEntity uses type-null-id format" {
    var interner = StringInterner.init(std.testing.allocator);
    defer interner.deinit();

    const id = try internEntity(&interner, Entity{ .type = "author", .id = "Homer" });
    try std.testing.expectEqualStrings("author\x00Homer", interner.resolve(id));

    // Different type, same id → different intern ID
    const id2 = try internEntity(&interner, Entity{ .type = "book", .id = "Homer" });
    try std.testing.expect(id != id2);
    try std.testing.expectEqualStrings("book\x00Homer", interner.resolve(id2));
}
```

### Test 3: ingest with mock fetcher — binary relation

```zig
test "ingest builds binary relation from mock facts" {
    const allocator = std.testing.allocator;

    // Set up mock with influenced-by facts
    var mock_entities_1 = [_]Entity{
        .{ .type = "rel", .id = "influenced-by" },
        .{ .type = "author", .id = "Virgil" },
        .{ .type = "author", .id = "Homer" },
    };
    var mock_entities_2 = [_]Entity{
        .{ .type = "rel", .id = "influenced-by" },
        .{ .type = "author", .id = "Dante" },
        .{ .type = "author", .id = "Virgil" },
    };
    var mock_facts = [_]Fact{
        .{ .id = 1, .entities = &mock_entities_1, .source = null },
        .{ .id = 2, .entities = &mock_entities_2, .source = null },
    };

    var mock = MockFetcher.init(allocator);
    defer mock.deinit();
    try mock.addFacts("influenced_by", &mock_facts);
    var ff = mock.fetcher();

    // Mapping
    const args = [_][]const u8{ "A", "B" };
    const pattern = [_]Mapping.PatternElement{
        .{ .entity_type = "rel", .value = .{ .constant = "influenced-by" } },
        .{ .entity_type = "author", .value = .{ .variable = "A" } },
        .{ .entity_type = "author", .value = .{ .variable = "B" } },
    };
    const mappings = [_]Mapping{.{
        .predicate = "influenced_by",
        .args = &args,
        .pattern = &pattern,
    }};

    var interner = StringInterner.init(allocator);
    defer interner.deinit();
    var relations: RelationMap = .{};
    defer deinitRelations(&relations, allocator);

    try ingest(ff, &mappings, &interner, &relations, allocator);

    // Verify relation was created
    const rel = relations.get("influenced_by") orelse return error.RelationNotFound;
    const bin = rel.binary;

    // Should have 2 tuples: (Virgil, Homer) and (Dante, Virgil)
    // Note: tupleCount() needs mutable pointer for rawr cardinality caching
    var bin_mut = relations.getPtr("influenced_by").?.binary;
    try std.testing.expectEqual(@as(u64, 2), bin_mut.tupleCount());

    // Verify specific tuples via interner
    const virgil_id = interner.lookup("author\x00Virgil").?;
    const homer_id = interner.lookup("author\x00Homer").?;
    const dante_id = interner.lookup("author\x00Dante").?;

    try std.testing.expect(bin.contains(virgil_id, homer_id));
    try std.testing.expect(bin.contains(dante_id, virgil_id));
    try std.testing.expect(!bin.contains(homer_id, virgil_id)); // not reversed
}
```

### Test 4: ingest with multiple mappings

```zig
test "ingest handles multiple mappings" {
    const allocator = std.testing.allocator;

    // Two different relations
    var infl_entities = [_]Entity{
        .{ .type = "rel", .id = "influenced-by" },
        .{ .type = "author", .id = "Virgil" },
        .{ .type = "author", .id = "Homer" },
    };
    var wrote_entities = [_]Entity{
        .{ .type = "rel", .id = "wrote" },
        .{ .type = "author", .id = "Homer" },
        .{ .type = "book", .id = "Iliad" },
    };

    var infl_facts = [_]Fact{
        .{ .id = 1, .entities = &infl_entities, .source = null },
    };
    var wrote_facts = [_]Fact{
        .{ .id = 2, .entities = &wrote_entities, .source = null },
    };

    var mock = MockFetcher.init(allocator);
    defer mock.deinit();
    try mock.addFacts("influenced_by", &infl_facts);
    try mock.addFacts("wrote", &wrote_facts);
    var ff = mock.fetcher();

    const args_ab = [_][]const u8{ "A", "B" };
    const pattern_infl = [_]Mapping.PatternElement{
        .{ .entity_type = "rel", .value = .{ .constant = "influenced-by" } },
        .{ .entity_type = "author", .value = .{ .variable = "A" } },
        .{ .entity_type = "author", .value = .{ .variable = "B" } },
    };
    const pattern_wrote = [_]Mapping.PatternElement{
        .{ .entity_type = "rel", .value = .{ .constant = "wrote" } },
        .{ .entity_type = "author", .value = .{ .variable = "A" } },
        .{ .entity_type = "book", .value = .{ .variable = "B" } },
    };
    const mappings = [_]Mapping{
        .{ .predicate = "influenced_by", .args = &args_ab, .pattern = &pattern_infl },
        .{ .predicate = "wrote", .args = &args_ab, .pattern = &pattern_wrote },
    };

    var interner = StringInterner.init(allocator);
    defer interner.deinit();
    var relations: RelationMap = .{};
    defer deinitRelations(&relations, allocator);

    try ingest(ff, &mappings, &interner, &relations, allocator);

    // Both relations should exist
    try std.testing.expect(relations.get("influenced_by") != null);
    try std.testing.expect(relations.get("wrote") != null);

    // "wrote" should have (Homer, Iliad) — note different entity types
    const homer_id = interner.lookup("author\x00Homer").?;
    const iliad_id = interner.lookup("book\x00Iliad").?;
    try std.testing.expect(relations.get("wrote").?.binary.contains(homer_id, iliad_id));
}
```

### Test 5: ingest with empty facts

```zig
test "ingest handles mapping with no matching facts" {
    const allocator = std.testing.allocator;

    var mock = MockFetcher.init(allocator);
    defer mock.deinit();
    // Don't add any facts
    var ff = mock.fetcher();

    const args = [_][]const u8{ "A", "B" };
    const pattern = [_]Mapping.PatternElement{
        .{ .entity_type = "rel", .value = .{ .constant = "wrote" } },
        .{ .entity_type = "author", .value = .{ .variable = "A" } },
        .{ .entity_type = "book", .value = .{ .variable = "B" } },
    };
    const mappings = [_]Mapping{.{
        .predicate = "wrote",
        .args = &args,
        .pattern = &pattern,
    }};

    var interner = StringInterner.init(allocator);
    defer interner.deinit();
    var relations: RelationMap = .{};
    defer deinitRelations(&relations, allocator);

    try ingest(ff, &mappings, &interner, &relations, allocator);

    // Relation should exist but be empty
    const rel = relations.get("wrote").?;
    try std.testing.expect(rel.binary.isEmpty());
}
```

### Test 6: persistRelations and loadRelations roundtrip

This test needs a temporary LMDB environment (same pattern as string_interner.zig test 6).

```zig
test "persist and load relations roundtrip" {
    const allocator = std.testing.allocator;

    var env = try createTempLmdbEnv(allocator);
    defer destroyTempLmdbEnv(&env, allocator);

    // Build and persist
    {
        var relations: RelationMap = .{};
        defer deinitRelations(&relations, allocator);

        // Create a binary relation with some data
        const pred = try allocator.dupe(u8, "influenced_by");
        try relations.put(allocator, pred, .{ .binary = try BinaryRelation.init(allocator) });
        var bin = &relations.getPtr("influenced_by").?.binary;
        try bin.insert(0, 1);  // (0, 1)
        try bin.insert(0, 2);  // (0, 2)
        try bin.insert(3, 1);  // (3, 1)

        try persistRelations(&relations, env, allocator);
    }

    // Load into fresh map
    {
        var relations = try loadRelations(env, allocator);
        defer deinitRelations(&relations, allocator);

        const rel = relations.get("influenced_by") orelse return error.RelationNotFound;
        const bin = rel.binary;

        try std.testing.expect(bin.contains(0, 1));
        try std.testing.expect(bin.contains(0, 2));
        try std.testing.expect(bin.contains(3, 1));
        try std.testing.expect(!bin.contains(1, 0));

        // Check domain/range
        try std.testing.expect(bin.domain.contains(0));
        try std.testing.expect(bin.domain.contains(3));
        try std.testing.expect(bin.range.contains(1));
        try std.testing.expect(bin.range.contains(2));
    }
}
```

### Test 7: full pipeline roundtrip (ingest → persist → load → verify)

```zig
test "full ingest-persist-load roundtrip" {
    const allocator = std.testing.allocator;

    var env = try createTempLmdbEnv(allocator);
    defer destroyTempLmdbEnv(&env, allocator);

    // Ingest and persist
    {
        var mock_entities = [_]Entity{
            .{ .type = "rel", .id = "wrote" },
            .{ .type = "author", .id = "Homer" },
            .{ .type = "book", .id = "Iliad" },
        };
        var mock_facts = [_]Fact{
            .{ .id = 1, .entities = &mock_entities, .source = null },
        };

        var mock = MockFetcher.init(allocator);
        defer mock.deinit();
        try mock.addFacts("wrote", &mock_facts);
        var ff = mock.fetcher();

        const args = [_][]const u8{ "A", "B" };
        const pattern = [_]Mapping.PatternElement{
            .{ .entity_type = "rel", .value = .{ .constant = "wrote" } },
            .{ .entity_type = "author", .value = .{ .variable = "A" } },
            .{ .entity_type = "book", .value = .{ .variable = "B" } },
        };
        const mappings = [_]Mapping{.{
            .predicate = "wrote",
            .args = &args,
            .pattern = &pattern,
        }};

        var interner = StringInterner.init(allocator);
        defer interner.deinit();
        var relations: RelationMap = .{};
        defer deinitRelations(&relations, allocator);

        try ingest(ff, &mappings, &interner, &relations, allocator);
        try interner.persist(env);
        try persistRelations(&relations, env, allocator);
    }

    // Load fresh and verify
    {
        var interner = try StringInterner.load(allocator, env);
        defer interner.deinit();
        var relations = try loadRelations(env, allocator);
        defer deinitRelations(&relations, allocator);

        // Interner should have the entities
        const homer_id = interner.lookup("author\x00Homer").?;
        const iliad_id = interner.lookup("book\x00Iliad").?;

        // Relation should have the tuple
        const rel = relations.get("wrote") orelse return error.RelationNotFound;
        try std.testing.expect(rel.binary.contains(homer_id, iliad_id));
    }
}
```

### Test 8: duplicate facts are idempotent

```zig
test "ingest deduplicates via bitmap set semantics" {
    const allocator = std.testing.allocator;

    // Same fact twice
    var entities = [_]Entity{
        .{ .type = "rel", .id = "wrote" },
        .{ .type = "author", .id = "Homer" },
        .{ .type = "book", .id = "Iliad" },
    };
    var facts = [_]Fact{
        .{ .id = 1, .entities = &entities, .source = null },
        .{ .id = 2, .entities = &entities, .source = null },
    };

    var mock = MockFetcher.init(allocator);
    defer mock.deinit();
    try mock.addFacts("wrote", &facts);
    var ff = mock.fetcher();

    const args = [_][]const u8{ "A", "B" };
    const pattern = [_]Mapping.PatternElement{
        .{ .entity_type = "rel", .value = .{ .constant = "wrote" } },
        .{ .entity_type = "author", .value = .{ .variable = "A" } },
        .{ .entity_type = "book", .value = .{ .variable = "B" } },
    };
    const mappings = [_]Mapping{.{
        .predicate = "wrote",
        .args = &args,
        .pattern = &pattern,
    }};

    var interner = StringInterner.init(allocator);
    defer interner.deinit();
    var relations: RelationMap = .{};
    defer deinitRelations(&relations, allocator);

    try ingest(ff, &mappings, &interner, &relations, allocator);

    // Should still be 1 tuple — bitmap insert is idempotent
    var bin = &relations.getPtr("wrote").?.binary;
    try std.testing.expectEqual(@as(u64, 1), bin.tupleCount());
}
```

## Wire into the build

Add to `src/lib.zig`:

```zig
pub const bitmap_ingest = @import("bitmap_ingest.zig");
```

And in the test block:

```zig
test {
    // ... existing ...
    _ = @import("bitmap_ingest.zig");
}
```

Verify: `zig build test` runs the new tests.

## What NOT to do

- Don't modify `fact_fetcher.zig`. Use it as-is through the FactFetcher interface.
- Don't modify `string_interner.zig` or `relation.zig`. They're done.
- Don't touch the old evaluator in `datalog.zig`.
- Don't handle variable-arity hyperedge expansion yet (e.g., facts with repeated tag positions). The existing test data uses exact-arity patterns only. Variable-arity support will be added in a follow-up.
- Don't wire into the CLI yet — that's step 6.
- Don't handle ground facts from `.dl` files — those bypass FactFetcher and are handled at evaluation time (step 5).
- Don't add ternary or higher-arity relations. Binary and unary only.

## Checklist

- [ ] `src/bitmap_ingest.zig` created with `ingest`, `persistRelations`, `loadRelations`, `deinitRelations`
- [ ] `buildArgPositions` maps mapping args to pattern positions
- [ ] `internEntity` produces `"type\x00id"` keys
- [ ] `ingest` builds relations from mock facts via FactFetcher
- [ ] `persistRelations` serializes all bitmaps to LMDB `"bitmaps"` database
- [ ] `loadRelations` deserializes bitmaps from LMDB
- [ ] `deinitRelations` frees all relation memory including map keys
- [ ] Tests 1–8 passing
- [ ] Wired into `lib.zig` and `zig build test` passes
- [ ] No changes to existing files except `lib.zig`
