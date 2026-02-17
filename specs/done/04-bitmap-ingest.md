> **Status: DONE** — Implemented and merged.

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
pub const RelationMap = std.StringHashMapUnmanaged(Relation);

pub fn ingest(
    ff: FactFetcher,
    mappings: []const Mapping,
    interner: *StringInterner,
    relations: *RelationMap,
    allocator: std.mem.Allocator,
) !void

pub fn persistRelations(
    relations: *RelationMap,
    env: lmdb.Environment,
    allocator: std.mem.Allocator,
) !void

pub fn loadRelations(
    env: lmdb.Environment,
    allocator: std.mem.Allocator,
) !RelationMap

pub fn deinitRelations(relations: *RelationMap, allocator: std.mem.Allocator) void
```

## Tuple Extraction

### How mapping variables connect to fact positions

Consider `@map influenced_by(A, B) = [rel:"influenced-by", author:A, author:B]`:

- `mapping.args` = `["A", "B"]` — defines the tuple columns
- `mapping.pattern[0]` = `{type: "rel", value: .constant("influenced-by")}` — skip (anchor)
- `mapping.pattern[1]` = `{type: "author", value: .variable("A")}` — tuple column 0
- `mapping.pattern[2]` = `{type: "author", value: .variable("B")}` — tuple column 1

For fact `[rel:influenced-by, author:Virgil, author:Homer]`:
- Variable "A" → `fact.entities[1]` = `author:Virgil`
- Variable "B" → `fact.entities[2]` = `author:Homer`
- Intern `"author\x00Virgil"` → some u32, intern `"author\x00Homer"` → some u32
- Insert `(id_virgil, id_homer)` into `BinaryRelation` for predicate `"influenced_by"`

### Algorithm

```zig
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
            return error.MappingArgNotInPattern;
        };
    }
    return positions;
}

fn internEntity(interner: *StringInterner, entity: Entity) !u32 {
    var buf: [512]u8 = undefined;
    const key = if (entity.type.len + 1 + entity.id.len <= buf.len) blk: {
        @memcpy(buf[0..entity.type.len], entity.type);
        buf[entity.type.len] = 0;
        @memcpy(buf[entity.type.len + 1 ..][0..entity.id.len], entity.id);
        break :blk buf[0 .. entity.type.len + 1 + entity.id.len];
    } else {
        @panic("entity key too long for stack buffer");
    };
    return interner.intern(key);
}
```

## Relation Persistence

### LMDB key format

All relation bitmaps go in a single LMDB database called `"bitmaps"`.

| Relation type | Component | Key | Value |
|---|---|---|---|
| Binary | domain | `pred\x00domain` | serialized bitmap |
| Binary | range | `pred\x00range` | serialized bitmap |
| Binary | forward entry | `pred\x00fwd\x00{u32 big-endian}` | serialized bitmap |
| Binary | reverse entry | `pred\x00rev\x00{u32 big-endian}` | serialized bitmap |
| Unary | members | `pred\x00members` | serialized bitmap |

Big-endian u32 in forward/reverse keys ensures LMDB's byte-order comparison groups entries for the same predicate together.

## Memory Ownership

| Data | Owner | Lifetime |
|---|---|---|
| Facts from FactFetcher | Caller frees after processing each mapping | Per-mapping |
| arg_positions array | Caller frees after processing each mapping | Per-mapping |
| Interned strings | StringInterner arena | Until interner.deinit() |
| Relations + bitmaps | Caller-provided allocator | Until deinitRelations() |
| Serialized bitmap bytes | Temp allocation, freed after LMDB put | Per-bitmap persist |
| RelationMap keys | Always duped into caller's allocator | Until deinitRelations() |

## Tests

All tests in `src/bitmap_ingest.zig`:

1. `buildArgPositions` maps args to pattern positions
2. `internEntity` produces `"type\x00id"` keys
3. Ingest with mock fetcher — binary relation
4. Ingest with multiple mappings
5. Ingest with empty facts
6. `persistRelations` and `loadRelations` roundtrip
7. Full pipeline roundtrip (ingest → persist → load → verify)
8. Duplicate facts are idempotent (bitmap set semantics)

## What NOT to do

- Don't modify `fact_fetcher.zig`, `string_interner.zig`, or `relation.zig`
- Don't touch the old evaluator in `datalog.zig`
- Don't handle variable-arity hyperedge expansion yet
- Don't wire into the CLI yet — that's step 6
- Don't handle ground facts from `.dl` files
- Don't add ternary or higher-arity relations

## Checklist

- [x] `src/bitmap_ingest.zig` created with `ingest`, `persistRelations`, `loadRelations`, `deinitRelations`
- [x] `buildArgPositions` maps mapping args to pattern positions
- [x] `internEntity` produces `"type\x00id"` keys
- [x] `ingest` builds relations from mock facts via FactFetcher
- [x] `persistRelations` serializes all bitmaps to LMDB `"bitmaps"` database
- [x] `loadRelations` deserializes bitmaps from LMDB
- [x] `deinitRelations` frees all relation memory including map keys
- [x] Tests 1–8 passing
- [x] Wired into `lib.zig` and `zig build test` passes
- [x] No changes to existing files except `lib.zig`
