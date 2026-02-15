const std = @import("std");
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

// =============================================================================
// Tests
// =============================================================================

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
