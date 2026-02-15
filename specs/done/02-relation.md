# Step 2: Relation Types

## Context

The bitmap evaluator represents Datalog relations as sets of integer tuples. A binary relation like `influenced_by(A, B)` stores two column indexes as roaring bitmaps — one forward (A→set of B's), one reverse (B→set of A's). This gives O(1) lookup in either direction and O(intersection) joins.

This step introduces the `Relation` types and adds rawr (our roaring bitmap library) as a dependency.

Depends on: Step 01 (StringInterner) — but only conceptually. No code dependency between the two yet.

## Add rawr dependency to kb

### build.zig.zon

```zig
.dependencies = .{
    .lmdb = .{ ... },  // existing
    .rawr = .{
        .url = "git+https://github.com/awesomo4000/rawr#main",
        .hash = "...",  // zig build will tell you the hash on first fetch
    },
},
```

Run `zig build` once — it'll error with the expected hash. Copy-paste the hash into the zon file.

### build.zig

Wire the rawr module into the kb library module:

```zig
// After the lmdb_dep setup:
const rawr_dep = b.dependency("rawr", .{
    .target = target,
    .optimize = optimize,
});
const rawr_mod = rawr_dep.module("rawr");

// Add to lib_mod (alongside lmdb):
lib_mod.addImport("rawr", rawr_mod);
```

Also add `rawr_mod` to any test modules that need it (the relation tests will import kb which imports rawr).

## API

Create `src/relation.zig`:

```zig
const std = @import("std");
const rawr = @import("rawr");
const RoaringBitmap = rawr.RoaringBitmap;

pub const BinaryRelation = struct { ... };
pub const UnaryRelation = struct { ... };
pub const Relation = union(enum) { ... };
```

### BinaryRelation

```zig
pub const BinaryRelation = struct {
    /// All IDs appearing in position 0
    domain: RoaringBitmap,
    /// All IDs appearing in position 1
    range: RoaringBitmap,
    /// forward[a] = bitmap of all b where R(a, b)
    forward: std.AutoHashMapUnmanaged(u32, RoaringBitmap),
    /// reverse[b] = bitmap of all a where R(a, b)
    reverse: std.AutoHashMapUnmanaged(u32, RoaringBitmap),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) !BinaryRelation
    pub fn deinit(self: *BinaryRelation) void

    /// Insert a tuple (a, b). Idempotent — duplicates are no-ops.
    pub fn insert(self: *BinaryRelation, a: u32, b: u32) !void

    /// Check if (a, b) exists.
    pub fn contains(self: *const BinaryRelation, a: u32, b: u32) bool

    /// Is the relation empty?
    pub fn isEmpty(self: *const BinaryRelation) bool

    /// Number of tuples (not entities).
    pub fn tupleCount(self: *BinaryRelation) u64

    /// Get all b values for a given a. Returns null if a not in domain.
    /// Returned pointer is mutable — rawr's cardinality() writes to cache.
    pub fn getForward(self: *BinaryRelation, a: u32) ?*RoaringBitmap

    /// Get all a values for a given b. Returns null if b not in range.
    /// Returned pointer is mutable — rawr's cardinality() writes to cache.
    pub fn getReverse(self: *BinaryRelation, b: u32) ?*RoaringBitmap
};
```

### UnaryRelation

```zig
pub const UnaryRelation = struct {
    members: RoaringBitmap,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) !UnaryRelation
    pub fn deinit(self: *UnaryRelation) void

    /// Insert a value. Idempotent.
    pub fn insert(self: *UnaryRelation, a: u32) !void

    /// Check if a value exists.
    pub fn contains(self: *const UnaryRelation, a: u32) bool

    /// Is the relation empty?
    pub fn isEmpty(self: *const UnaryRelation) bool

    /// Number of members.
    pub fn count(self: *UnaryRelation) u64
};
```

### Tagged union

```zig
pub const Relation = union(enum) {
    unary: UnaryRelation,
    binary: BinaryRelation,

    pub fn deinit(self: *Relation) void {
        switch (self.*) {
            .unary => |*u| u.deinit(),
            .binary => |*b| b.deinit(),
        }
    }
};
```

## Implementation Details

### BinaryRelation.init

```zig
pub fn init(allocator: std.mem.Allocator) !BinaryRelation {
    return .{
        .domain = try RoaringBitmap.init(allocator),
        .range = try RoaringBitmap.init(allocator),
        .forward = .{},
        .reverse = .{},
        .allocator = allocator,
    };
}
```

### BinaryRelation.insert

```zig
pub fn insert(self: *BinaryRelation, a: u32, b: u32) !void {
    _ = try self.domain.add(a);
    _ = try self.range.add(b);

    // forward[a] |= {b}
    const fwd_gop = try self.forward.getOrPut(self.allocator, a);
    if (!fwd_gop.found_existing) {
        fwd_gop.value_ptr.* = try RoaringBitmap.init(self.allocator);
    }
    _ = try fwd_gop.value_ptr.add(b);

    // reverse[b] |= {a}
    const rev_gop = try self.reverse.getOrPut(self.allocator, b);
    if (!rev_gop.found_existing) {
        rev_gop.value_ptr.* = try RoaringBitmap.init(self.allocator);
    }
    _ = try rev_gop.value_ptr.add(a);
}
```

Note: `RoaringBitmap.add()` returns `bool` (true if new). We ignore it — `insert` is idempotent at the relation level because `domain`/`range` and the per-key bitmaps all handle duplicates naturally.

### BinaryRelation.contains

```zig
pub fn contains(self: *const BinaryRelation, a: u32, b: u32) bool {
    const fwd = self.forward.get(a) orelse return false;
    return fwd.contains(b);
}
```

### BinaryRelation.tupleCount

```zig
pub fn tupleCount(self: *BinaryRelation) u64 {
    var total: u64 = 0;
    var it = self.forward.valueIterator();
    while (it.next()) |bm| {
        total += bm.cardinality();
    }
    return total;
}
```

Note: this iterates all forward bitmaps. It's O(distinct-a-values), not O(tuples). Each `cardinality()` is O(1) thanks to rawr's caching. Good enough for diagnostics. Don't call it in a hot loop.

### BinaryRelation.getForward / getReverse

```zig
pub fn getForward(self: *BinaryRelation, a: u32) ?*RoaringBitmap {
    return self.forward.getPtr(a);
}

pub fn getReverse(self: *BinaryRelation, b: u32) ?*RoaringBitmap {
    return self.reverse.getPtr(b);
}
```

These return pointers to the internal bitmaps. The caller can iterate or intersect them without copying. The pointers are valid until the relation is mutated.

### BinaryRelation.deinit

```zig
pub fn deinit(self: *BinaryRelation) void {
    self.domain.deinit();
    self.range.deinit();

    var fwd_it = self.forward.valueIterator();
    while (fwd_it.next()) |bm| {
        bm.deinit();
    }
    self.forward.deinit(self.allocator);

    var rev_it = self.reverse.valueIterator();
    while (rev_it.next()) |bm| {
        bm.deinit();
    }
    self.reverse.deinit(self.allocator);
}
```

Every bitmap gets individually deinit'd, then the hashmaps themselves.

### UnaryRelation

Straightforward — thin wrapper around a single `RoaringBitmap`:

```zig
pub fn init(allocator: std.mem.Allocator) !UnaryRelation {
    return .{
        .members = try RoaringBitmap.init(allocator),
        .allocator = allocator,
    };
}

pub fn deinit(self: *UnaryRelation) void {
    self.members.deinit();
}

pub fn insert(self: *UnaryRelation, a: u32) !void {
    _ = try self.members.add(a);
}

pub fn contains(self: *const UnaryRelation, a: u32) bool {
    return self.members.contains(a);
}

pub fn isEmpty(self: *const UnaryRelation) bool {
    return self.members.isEmpty();
}

pub fn count(self: *UnaryRelation) u64 {
    return self.members.cardinality();
}
```

## Tests

Write all tests in `src/relation.zig`.

### Test 1: BinaryRelation insert and contains

```zig
test "binary relation insert and contains" {
    var rel = try BinaryRelation.init(std.testing.allocator);
    defer rel.deinit();

    try rel.insert(1, 10);
    try rel.insert(1, 20);
    try rel.insert(2, 10);

    try std.testing.expect(rel.contains(1, 10));
    try std.testing.expect(rel.contains(1, 20));
    try std.testing.expect(rel.contains(2, 10));
    try std.testing.expect(!rel.contains(2, 20));
    try std.testing.expect(!rel.contains(3, 10));
}
```

### Test 2: duplicate insert is idempotent

```zig
test "binary relation duplicate insert" {
    var rel = try BinaryRelation.init(std.testing.allocator);
    defer rel.deinit();

    try rel.insert(1, 10);
    try rel.insert(1, 10);
    try rel.insert(1, 10);

    try std.testing.expectEqual(@as(u64, 1), rel.tupleCount());
    try std.testing.expect(rel.contains(1, 10));
}
```

### Test 3: domain and range

```zig
test "binary relation domain and range" {
    var rel = try BinaryRelation.init(std.testing.allocator);
    defer rel.deinit();

    try rel.insert(1, 10);
    try rel.insert(2, 20);
    try rel.insert(3, 10);

    // domain = {1, 2, 3}
    try std.testing.expectEqual(@as(u64, 3), rel.domain.cardinality());
    try std.testing.expect(rel.domain.contains(1));
    try std.testing.expect(rel.domain.contains(2));
    try std.testing.expect(rel.domain.contains(3));

    // range = {10, 20}
    try std.testing.expectEqual(@as(u64, 2), rel.range.cardinality());
    try std.testing.expect(rel.range.contains(10));
    try std.testing.expect(rel.range.contains(20));
}
```

### Test 4: forward and reverse lookup

```zig
test "binary relation forward and reverse lookup" {
    var rel = try BinaryRelation.init(std.testing.allocator);
    defer rel.deinit();

    try rel.insert(1, 10);
    try rel.insert(1, 20);
    try rel.insert(2, 10);

    // forward[1] = {10, 20}
    const fwd1 = rel.getForward(1).?;
    try std.testing.expectEqual(@as(u64, 2), fwd1.cardinality());
    try std.testing.expect(fwd1.contains(10));
    try std.testing.expect(fwd1.contains(20));

    // forward[2] = {10}
    const fwd2 = rel.getForward(2).?;
    try std.testing.expectEqual(@as(u64, 1), fwd2.cardinality());

    // forward[99] = null
    try std.testing.expectEqual(@as(?*const RoaringBitmap, null), rel.getForward(99));

    // reverse[10] = {1, 2}
    const rev10 = rel.getReverse(10).?;
    try std.testing.expectEqual(@as(u64, 2), rev10.cardinality());
    try std.testing.expect(rev10.contains(1));
    try std.testing.expect(rev10.contains(2));
}
```

### Test 5: empty relation

```zig
test "binary relation empty" {
    var rel = try BinaryRelation.init(std.testing.allocator);
    defer rel.deinit();

    try std.testing.expect(rel.isEmpty());
    try std.testing.expectEqual(@as(u64, 0), rel.tupleCount());

    try rel.insert(1, 2);
    try std.testing.expect(!rel.isEmpty());
}
```

### Test 6: tuple count

```zig
test "binary relation tuple count" {
    var rel = try BinaryRelation.init(std.testing.allocator);
    defer rel.deinit();

    try rel.insert(1, 10);
    try rel.insert(1, 20);
    try rel.insert(2, 10);
    try rel.insert(2, 20);
    try rel.insert(3, 30);

    try std.testing.expectEqual(@as(u64, 5), rel.tupleCount());
}
```

### Test 7: UnaryRelation basics

```zig
test "unary relation basics" {
    var rel = try UnaryRelation.init(std.testing.allocator);
    defer rel.deinit();

    try std.testing.expect(rel.isEmpty());

    try rel.insert(42);
    try rel.insert(100);
    try rel.insert(42); // duplicate

    try std.testing.expect(!rel.isEmpty());
    try std.testing.expectEqual(@as(u64, 2), rel.count());
    try std.testing.expect(rel.contains(42));
    try std.testing.expect(rel.contains(100));
    try std.testing.expect(!rel.contains(99));
}
```

### Test 8: Relation tagged union

```zig
test "relation tagged union" {
    var rel = Relation{ .binary = try BinaryRelation.init(std.testing.allocator) };
    defer rel.deinit();

    switch (rel) {
        .binary => |*b| {
            try b.insert(1, 2);
            try std.testing.expect(b.contains(1, 2));
        },
        .unary => unreachable,
    }
}
```

## Wire into the build

Add to `src/lib.zig`:

```zig
pub const relation = @import("relation.zig");
```

And in the test block:

```zig
test {
    // ... existing ...
    _ = @import("relation.zig");
}
```

Verify: `zig build test` runs the new tests.

## What NOT to do

- Don't add persistence (serialization to LMDB) yet. That's step 04.
- Don't add set operations between relations (bitwiseAnd on domains, etc). That's step 05 (evaluator).
- Don't touch the existing evaluator or any other existing code.
- Don't worry about arena-backed bitmaps (`OwnedBitmap`) for now. Use `RoaringBitmap` with `smp_allocator` or whatever the caller provides. The evaluator step will decide allocator strategy.

## Checklist

- [ ] rawr added as dependency in `build.zig.zon` and wired in `build.zig`
- [ ] `src/relation.zig` created with `BinaryRelation`, `UnaryRelation`, `Relation`
- [ ] `init`, `deinit`, `insert`, `contains`, `isEmpty` for both types
- [ ] `tupleCount`, `getForward`, `getReverse` for BinaryRelation
- [ ] Tests 1-8 passing
- [ ] Wired into `lib.zig` and `zig build test` passes
- [ ] No changes to existing code except build files
