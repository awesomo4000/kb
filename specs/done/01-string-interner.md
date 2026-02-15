# Step 1: StringInterner

## Context

kb is getting a bitmap-backed Datalog evaluator. The evaluator operates on `u32` integer IDs, not strings. The StringInterner is the bridge — it assigns a unique `u32` to every entity string, and can resolve IDs back to strings for query output.

The interner is persisted in LMDB so that IDs are stable across runs. At multi-million entity scale, re-interning from scratch on every startup would take seconds. With persistence, startup loads the interner via mmap'd sequential read — nearly instant.

This is the foundation everything else builds on. It has no dependencies on rawr or the evaluator — just LMDB and the standard library.

## API

Create `src/string_interner.zig`:

```zig
pub const StringInterner = struct {
    /// string → ID (lookup during ingest)
    forward: std.StringHashMapUnmanaged(u32),
    /// ID → string (lookup during query output)
    reverse: std.ArrayListUnmanaged([]const u8),
    /// Next ID to assign
    next_id: u32,
    /// Owns all interned string memory
    arena: std.heap.ArenaAllocator,

    pub fn init(backing: std.mem.Allocator) StringInterner
    pub fn deinit(self: *StringInterner) void

    /// Intern a string. Returns existing ID or assigns a new one.
    /// The string is copied into the arena — caller's copy can be freed.
    pub fn intern(self: *StringInterner, str: []const u8) !u32

    /// Resolve an ID back to its string. Returned slice is arena-owned
    /// and valid until deinit().
    pub fn resolve(self: *const StringInterner, id: u32) []const u8

    /// Number of interned strings.
    pub fn count(self: *const StringInterner) u32

    /// Check if a string is already interned without assigning an ID.
    pub fn lookup(self: *const StringInterner, str: []const u8) ?u32

    /// Persist entire interner to LMDB. Call after ingest completes.
    pub fn persist(self: *const StringInterner, env: lmdb.Environment) !void

    /// Load interner from LMDB. Call at evaluation startup.
    pub fn load(backing: std.mem.Allocator, env: lmdb.Environment) !StringInterner
};
```

## Behavior

### intern()

```zig
pub fn intern(self: *StringInterner, str: []const u8) !u32 {
    if (self.forward.get(str)) |id| return id;
    const id = self.next_id;
    self.next_id += 1;
    const owned = try self.arena.allocator().dupe(u8, str);
    try self.forward.put(self.arena.allocator(), owned, id);
    try self.reverse.append(self.arena.allocator(), owned);
    return id;
}
```

Key properties:
- Same string → same ID (idempotent)
- IDs are sequential starting at 0
- IDs are never reused
- The arena owns all string memory — callers don't need to keep their copies alive
- `forward` keys point into arena memory (the duped copy), so the hashmap doesn't hold dangling pointers

### resolve()

```zig
pub fn resolve(self: *const StringInterner, id: u32) []const u8 {
    return self.reverse.items[id];
}
```

No bounds check in release. Callers must only resolve IDs that came from `intern()`. Debug builds will catch out-of-bounds via Zig's safety checks.

### lookup()

```zig
pub fn lookup(self: *const StringInterner, str: []const u8) ?u32 {
    return self.forward.get(str);
}
```

For cases where you want to check without assigning. Used during query resolution — if a constant in a query pattern hasn't been interned, it means no entity with that value exists, so the query result is empty. No need to assign an ID just to discover there's nothing there.

## LMDB Persistence

### Database layout

Two new LMDB named databases:

```
intern_fwd:  key = string bytes      → value = u32 (4 bytes, little-endian)
intern_rev:  key = u32 (big-endian)  → value = string bytes
```

`intern_rev` uses big-endian keys so that LMDB's default byte-order comparison keeps IDs in ascending numeric order. This makes sequential cursor iteration yield IDs 0, 1, 2, ... in order, which is how `load()` rebuilds the `reverse` array.

`intern_fwd` keys are raw string bytes — no null terminator, no prefix. The string is the key.

### persist()

One write transaction. Iterate all interned strings and write both databases:

```zig
pub fn persist(self: *const StringInterner, env: lmdb.Environment) !void {
    const txn = try env.transaction(.{ .mode = .ReadWrite });
    errdefer txn.abort();

    const fwd_db = try txn.database("intern_fwd", .{ .create = true });
    const rev_db = try txn.database("intern_rev", .{ .create = true });

    for (self.reverse.items, 0..) |str, i| {
        const id: u32 = @intCast(i);
        const id_le = std.mem.toBytes(id);               // little-endian value
        const id_be = std.mem.toBytes(std.mem.nativeToBig(u32, id)); // big-endian key

        try fwd_db.set(str, &id_le);
        try rev_db.set(&id_be, str);
    }

    try txn.commit();
}
```

### load()

One read transaction. Cursor through `intern_rev` (sorted by ID) to rebuild both the `reverse` array and the `forward` map:

```zig
pub fn load(backing: std.mem.Allocator, env: lmdb.Environment) !StringInterner {
    var self = StringInterner.init(backing);
    errdefer self.deinit();

    const txn = try env.transaction(.{ .mode = .ReadOnly });
    defer txn.abort();

    const rev_db = try txn.database("intern_rev", .{});
    var cursor = try rev_db.cursor();
    defer cursor.deinit();

    // Iterate all entries in ID order (big-endian keys sort numerically)
    var entry = try cursor.goToFirst();
    while (entry) |e| {
        const alloc = self.arena.allocator();
        const owned = try alloc.dupe(u8, e.value);
        try self.reverse.append(alloc, owned);
        try self.forward.put(alloc, owned, self.next_id);
        self.next_id += 1;
        entry = try cursor.goToNext();
    }

    return self;
}
```

After `load()`, the interner is in the same state as if every string had been `intern()`ed in ID order. New calls to `intern()` will assign IDs continuing from where the persisted data left off.

**Important:** `load()` does NOT read from `intern_fwd`. The reverse database has all the data needed. We load from `intern_rev` because it's sorted by ID, so the `reverse` array gets populated in order. The `forward` map gets populated as a side effect.

## max_dbs

`FactStore` currently opens LMDB with `max_dbs = 4`. The interner adds 2 more databases. Later steps will add more (bitmap storage). Bump to `max_dbs = 8` now to avoid revisiting this.

This is a one-line change in `fact_store.zig`:

```zig
.max_dbs = 8,  // was 4
```

## Entity key format

The interner keys are **entity values only**, not `"type\x00id"` composite keys.

Why: the interner is type-blind. It interns whatever string the caller passes. The caller (bitmap_ingest, a future step) decides what to intern. For binary relations like `influenced_by(A, B)` where both A and B are authors, the interner just sees strings like `"Homer"` and `"Virgil"`.

Entity type discrimination happens at the relation level — `influenced_by.forward` only contains author IDs because only authors were inserted. The interner doesn't need to know about types.

If two different entity types share a name (unlikely but possible), the ingest layer can prepend the type to disambiguate: `intern("author\x00Homer")` vs `intern("book\x00Homer")`. That's a caller decision, not an interner decision. The interner is a generic string→u32 map.

## Tests

Write all tests in `src/string_interner.zig` at the bottom of the file.

### Test 1: basic intern and resolve

```zig
test "intern assigns sequential IDs" {
    var si = StringInterner.init(std.testing.allocator);
    defer si.deinit();

    const a = try si.intern("alice");
    const b = try si.intern("bob");
    const c = try si.intern("charlie");

    try std.testing.expectEqual(@as(u32, 0), a);
    try std.testing.expectEqual(@as(u32, 1), b);
    try std.testing.expectEqual(@as(u32, 2), c);

    try std.testing.expectEqualStrings("alice", si.resolve(a));
    try std.testing.expectEqualStrings("bob", si.resolve(b));
    try std.testing.expectEqualStrings("charlie", si.resolve(c));
}
```

### Test 2: idempotent

```zig
test "intern same string returns same ID" {
    var si = StringInterner.init(std.testing.allocator);
    defer si.deinit();

    const first = try si.intern("homer");
    const second = try si.intern("homer");
    const third = try si.intern("homer");

    try std.testing.expectEqual(first, second);
    try std.testing.expectEqual(first, third);
    try std.testing.expectEqual(@as(u32, 1), si.count());
}
```

### Test 3: lookup without assigning

```zig
test "lookup returns null for unknown strings" {
    var si = StringInterner.init(std.testing.allocator);
    defer si.deinit();

    try std.testing.expectEqual(@as(?u32, null), si.lookup("unknown"));

    _ = try si.intern("known");
    try std.testing.expectEqual(@as(?u32, 0), si.lookup("known"));
    try std.testing.expectEqual(@as(?u32, null), si.lookup("unknown"));
}
```

### Test 4: caller string can be freed

```zig
test "intern copies string — caller can free original" {
    var si = StringInterner.init(std.testing.allocator);
    defer si.deinit();

    // Create a heap string, intern it, then free the original
    const str = try std.testing.allocator.dupe(u8, "temporary");
    const id = try si.intern(str);
    std.testing.allocator.free(str);

    // Resolve still works — interner owns its copy
    try std.testing.expectEqualStrings("temporary", si.resolve(id));
}
```

### Test 5: count

```zig
test "count tracks unique strings" {
    var si = StringInterner.init(std.testing.allocator);
    defer si.deinit();

    try std.testing.expectEqual(@as(u32, 0), si.count());
    _ = try si.intern("a");
    try std.testing.expectEqual(@as(u32, 1), si.count());
    _ = try si.intern("b");
    try std.testing.expectEqual(@as(u32, 2), si.count());
    _ = try si.intern("a"); // duplicate
    try std.testing.expectEqual(@as(u32, 2), si.count());
}
```

### Test 6: persist and load roundtrip

This test needs a temporary LMDB environment. Follow the pattern from `tests/integration.zig` for creating a temp directory.

```zig
test "persist and load roundtrip" {
    const allocator = std.testing.allocator;

    // Create temp LMDB environment
    // (see integration.zig for temp dir pattern)
    var env = try createTempLmdbEnv(allocator);
    defer destroyTempLmdbEnv(&env, allocator);

    // Populate and persist
    {
        var si = StringInterner.init(allocator);
        defer si.deinit();

        _ = try si.intern("homer");
        _ = try si.intern("virgil");
        _ = try si.intern("dante");

        try si.persist(env);
    }

    // Load into fresh interner
    {
        var si = try StringInterner.load(allocator, env);
        defer si.deinit();

        // Same IDs as before
        try std.testing.expectEqual(@as(u32, 3), si.count());
        try std.testing.expectEqualStrings("homer", si.resolve(0));
        try std.testing.expectEqualStrings("virgil", si.resolve(1));
        try std.testing.expectEqualStrings("dante", si.resolve(2));

        // Lookup works
        try std.testing.expectEqual(@as(?u32, 0), si.lookup("homer"));

        // New interns continue from where we left off
        const id = try si.intern("new_entity");
        try std.testing.expectEqual(@as(u32, 3), id);
    }
}
```

### Test 7: empty roundtrip

```zig
test "persist and load empty interner" {
    const allocator = std.testing.allocator;

    var env = try createTempLmdbEnv(allocator);
    defer destroyTempLmdbEnv(&env, allocator);

    {
        var si = StringInterner.init(allocator);
        defer si.deinit();
        try si.persist(env);
    }

    {
        var si = try StringInterner.load(allocator, env);
        defer si.deinit();
        try std.testing.expectEqual(@as(u32, 0), si.count());
    }
}
```

## Wire into the build

Add to `src/lib.zig`:

```zig
pub const StringInterner = @import("string_interner.zig").StringInterner;
```

And in the test block:

```zig
test {
    // ... existing ...
    _ = @import("string_interner.zig");
}
```

Verify: `zig build test` runs the new tests.

## What NOT to do

- Don't use `"type\x00id"` format in the interner itself. The caller decides what strings to intern.
- Don't add rawr as a dependency yet. The interner doesn't need bitmaps.
- Don't touch the existing evaluator. It keeps working as-is.
- Don't worry about thread safety. Single-threaded ingest and evaluation for now.
- Don't persist incrementally. `persist()` writes the entire interner. Incremental updates come later if profiling shows it matters.

## Checklist

- [ ] `src/string_interner.zig` created with `StringInterner` struct
- [ ] `init`, `deinit`, `intern`, `resolve`, `lookup`, `count` implemented
- [ ] `persist` and `load` implemented with LMDB
- [ ] `max_dbs` bumped to 8 in `fact_store.zig`
- [ ] Tests 1-7 passing
- [ ] Wired into `lib.zig` and `zig build test` passes
- [ ] No changes to existing code except `max_dbs`
