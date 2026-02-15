const std = @import("std");
const lmdb = @import("lmdb");
const rawr = @import("rawr");
const RoaringBitmap = rawr.RoaringBitmap;

/// A binary relation R(a, b) stored as dual bitmap indexes.
/// forward[a] = {b : R(a,b)}, reverse[b] = {a : R(a,b)}
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

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) !Self {
        return .{
            .domain = try RoaringBitmap.init(allocator),
            .range = try RoaringBitmap.init(allocator),
            .forward = .{},
            .reverse = .{},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
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

    /// Insert a tuple (a, b). Idempotent - duplicates are no-ops.
    pub fn insert(self: *Self, a: u32, b: u32) !void {
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

    /// Check if (a, b) exists.
    pub fn contains(self: *const Self, a: u32, b: u32) bool {
        const fwd = self.forward.get(a) orelse return false;
        return fwd.contains(b);
    }

    /// Is the relation empty?
    pub fn isEmpty(self: *const Self) bool {
        return self.domain.isEmpty();
    }

    /// Number of tuples (not entities).
    pub fn tupleCount(self: *Self) u64 {
        var total: u64 = 0;
        var it = self.forward.valueIterator();
        while (it.next()) |bm| {
            total += bm.cardinality();
        }
        return total;
    }

    /// Get all b values for a given a. Returns null if a not in domain.
    /// Returned pointer is mutable - rawr's cardinality() writes to cache.
    pub fn getForward(self: *Self, a: u32) ?*RoaringBitmap {
        return self.forward.getPtr(a);
    }

    /// Get all a values for a given b. Returns null if b not in range.
    /// Returned pointer is mutable - rawr's cardinality() writes to cache.
    pub fn getReverse(self: *Self, b: u32) ?*RoaringBitmap {
        return self.reverse.getPtr(b);
    }
};

/// A unary relation R(a) stored as a single bitmap.
pub const UnaryRelation = struct {
    members: RoaringBitmap,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) !Self {
        return .{
            .members = try RoaringBitmap.init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.members.deinit();
    }

    /// Insert a value. Idempotent.
    pub fn insert(self: *Self, a: u32) !void {
        _ = try self.members.add(a);
    }

    /// Check if a value exists.
    pub fn contains(self: *const Self, a: u32) bool {
        return self.members.contains(a);
    }

    /// Is the relation empty?
    pub fn isEmpty(self: *const Self) bool {
        return self.members.isEmpty();
    }

    /// Number of members.
    pub fn count(self: *Self) u64 {
        return self.members.cardinality();
    }
};

/// Tagged union for either unary or binary relations.
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

pub const RelationMap = std.StringHashMapUnmanaged(Relation);

/// Free all relations in a RelationMap, including duped predicate keys.
pub fn deinitRelations(relations: *RelationMap, allocator: std.mem.Allocator) void {
    var iter = relations.iterator();
    while (iter.next()) |entry| {
        allocator.free(@constCast(entry.key_ptr.*));
        entry.value_ptr.deinit();
    }
    relations.deinit(allocator);
}

/// Persist all relations to the LMDB "bitmaps" database.
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

/// Load all relations from the LMDB "bitmaps" database.
/// Returns empty map if database does not exist (first run).
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

/// Serialize a bitmap and store it in LMDB.
fn putBitmap(
    db: anytype,
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

/// Parse a single LMDB entry from the "bitmaps" database and insert into the relation map.
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

    // Ensure relation exists -- IMPORTANT: dupe pred before getOrPut (cursor key lifetime)
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
        // Key already existed -- free the dupe we just made
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
        const eid = entity_id orelse {
            bitmap.deinit();
            return;
        };
        try gop.value_ptr.binary.forward.put(allocator, eid, bitmap);
    } else if (std.mem.eql(u8, component, "rev")) {
        const eid = entity_id orelse {
            bitmap.deinit();
            return;
        };
        try gop.value_ptr.binary.reverse.put(allocator, eid, bitmap);
    } else {
        bitmap.deinit(); // unknown component, discard
    }
}

// =============================================================================
// Tests
// =============================================================================

const test_helpers = @import("test_helpers.zig");
const createTempLmdbEnv = test_helpers.createTempLmdbEnv;
const destroyTempLmdbEnv = test_helpers.destroyTempLmdbEnv;

test "persist and load relations roundtrip" {
    const allocator = std.testing.allocator;

    var state = try createTempLmdbEnv(allocator);
    defer destroyTempLmdbEnv(&state, allocator);

    // Build and persist
    {
        var relations: RelationMap = .{};
        defer deinitRelations(&relations, allocator);

        // Create a binary relation with some data
        const pred = try allocator.dupe(u8, "influenced_by");
        try relations.put(allocator, pred, .{ .binary = try BinaryRelation.init(allocator) });
        var bin = &relations.getPtr("influenced_by").?.binary;
        try bin.insert(0, 1);
        try bin.insert(0, 2);
        try bin.insert(3, 1);

        try persistRelations(&relations, state.env, allocator);
    }

    // Load into fresh map
    {
        var relations = try loadRelations(state.env, allocator);
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

test "binary relation duplicate insert" {
    var rel = try BinaryRelation.init(std.testing.allocator);
    defer rel.deinit();

    try rel.insert(1, 10);
    try rel.insert(1, 10);
    try rel.insert(1, 10);

    try std.testing.expectEqual(@as(u64, 1), rel.tupleCount());
    try std.testing.expect(rel.contains(1, 10));
}

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
    try std.testing.expectEqual(@as(?*RoaringBitmap, null), rel.getForward(99));

    // reverse[10] = {1, 2}
    const rev10 = rel.getReverse(10).?;
    try std.testing.expectEqual(@as(u64, 2), rev10.cardinality());
    try std.testing.expect(rev10.contains(1));
    try std.testing.expect(rev10.contains(2));
}

test "binary relation empty" {
    var rel = try BinaryRelation.init(std.testing.allocator);
    defer rel.deinit();

    try std.testing.expect(rel.isEmpty());
    try std.testing.expectEqual(@as(u64, 0), rel.tupleCount());

    try rel.insert(1, 2);
    try std.testing.expect(!rel.isEmpty());
}

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
