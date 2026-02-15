const std = @import("std");
const lmdb = @import("lmdb");

/// Maps strings to unique u32 IDs and back. Persists to LMDB for stable IDs across runs.
pub const StringInterner = struct {
    /// string -> ID (lookup during ingest)
    forward: std.StringHashMapUnmanaged(u32),
    /// ID -> string (lookup during query output)
    reverse: std.ArrayListUnmanaged([]const u8),
    /// Next ID to assign
    next_id: u32,
    /// Owns all interned string memory
    arena: std.heap.ArenaAllocator,

    const Self = @This();

    pub fn init(backing: std.mem.Allocator) Self {
        return .{
            .forward = .{},
            .reverse = .{},
            .next_id = 0,
            .arena = std.heap.ArenaAllocator.init(backing),
        };
    }

    pub fn deinit(self: *Self) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// Intern a string. Returns existing ID or assigns a new one.
    /// The string is copied into the arena - caller's copy can be freed.
    pub fn intern(self: *Self, str: []const u8) !u32 {
        if (self.forward.get(str)) |id| return id;

        const id = self.next_id;
        self.next_id += 1;

        const alloc = self.arena.allocator();
        const owned = try alloc.dupe(u8, str);
        try self.forward.put(alloc, owned, id);
        try self.reverse.append(alloc, owned);

        return id;
    }

    /// Resolve an ID back to its string. Returned slice is arena-owned
    /// and valid until deinit().
    pub fn resolve(self: *const Self, id: u32) []const u8 {
        return self.reverse.items[id];
    }

    /// Number of interned strings.
    pub fn count(self: *const Self) u32 {
        return @intCast(self.reverse.items.len);
    }

    /// Check if a string is already interned without assigning an ID.
    pub fn lookup(self: *const Self, str: []const u8) ?u32 {
        return self.forward.get(str);
    }

    /// Persist entire interner to LMDB. Call after ingest completes.
    pub fn persist(self: *const Self, env: lmdb.Environment) !void {
        const txn = try env.transaction(.{ .mode = .ReadWrite });
        errdefer txn.abort();

        const fwd_db = try txn.database("intern_fwd", .{ .create = true });
        const rev_db = try txn.database("intern_rev", .{ .create = true });

        for (self.reverse.items, 0..) |str, i| {
            const id: u32 = @intCast(i);
            const id_le = std.mem.toBytes(id); // little-endian value
            const id_be = std.mem.toBytes(std.mem.nativeToBig(u32, id)); // big-endian key

            try fwd_db.set(str, &id_le);
            try rev_db.set(&id_be, str);
        }

        try txn.commit();
    }

    /// Load interner from LMDB. Call at evaluation startup.
    /// Returns empty interner if database doesn't exist (first run).
    pub fn load(backing: std.mem.Allocator, env: lmdb.Environment) !Self {
        var self = Self.init(backing);
        errdefer self.deinit();

        const txn = try env.transaction(.{ .mode = .ReadOnly });
        defer txn.abort();

        // Database may not exist on first run - return empty interner
        const rev_db = txn.database("intern_rev", .{}) catch |err| switch (err) {
            error.MDB_NOTFOUND => return self,
            else => return err,
        };

        var cursor = try rev_db.cursor();
        defer cursor.deinit();

        // Iterate all entries in ID order (big-endian keys sort numerically)
        _ = try cursor.goToFirst() orelse return self;

        while (true) {
            const entry = try cursor.getCurrentEntry();
            const alloc = self.arena.allocator();
            const owned = try alloc.dupe(u8, entry.value);
            try self.reverse.append(alloc, owned);
            try self.forward.put(alloc, owned, self.next_id);
            self.next_id += 1;

            _ = try cursor.goToNext() orelse break;
        }

        return self;
    }
};

// =============================================================================
// Unit Tests (no LMDB)
// =============================================================================

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

test "lookup returns null for unknown strings" {
    var si = StringInterner.init(std.testing.allocator);
    defer si.deinit();

    try std.testing.expectEqual(@as(?u32, null), si.lookup("unknown"));

    _ = try si.intern("known");
    try std.testing.expectEqual(@as(?u32, 0), si.lookup("known"));
    try std.testing.expectEqual(@as(?u32, null), si.lookup("unknown"));
}

test "intern copies string - caller can free original" {
    var si = StringInterner.init(std.testing.allocator);
    defer si.deinit();

    // Create a heap string, intern it, then free the original
    const str = try std.testing.allocator.dupe(u8, "temporary");
    const id = try si.intern(str);
    std.testing.allocator.free(str);

    // Resolve still works - interner owns its copy
    try std.testing.expectEqualStrings("temporary", si.resolve(id));
}

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
