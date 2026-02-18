const std = @import("std");
const lmdb = @import("lmdb");
const Fact = @import("fact.zig").Fact;
const Entity = @import("fact.zig").Entity;
const ek = @import("entity_key.zig");

/// Hypergraph fact store backed by LMDB.
///
/// LMDB databases (all indices use DUPSORT for O(log n) insert):
/// - facts: fact_id -> packed fact record
/// - by_entity: entity_key -> fact_id (DUPSORT)
/// - fact_edges: fact_id -> entity_key (DUPSORT)
/// - entity_list: "type" -> id (DUPSORT)
pub const FactStore = struct {
    env: lmdb.Environment,
    allocator: std.mem.Allocator,
    next_fact_id: u64,

    const Self = @This();

    pub const Options = struct {
        path: [:0]const u8,
        map_size: usize = 10 * 1024 * 1024 * 1024, // 10 GB default
        /// Skip fsync on commit. Faster but risks data loss on power failure.
        /// Safe to use for ingest-heavy workloads where data can be re-ingested.
        no_sync: bool = false,
    };

    pub fn open(allocator: std.mem.Allocator, options: Options) !Self {
        // Create directory if it doesn't exist
        std.fs.makeDirAbsolute(options.path) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        const env = try lmdb.Environment.init(options.path, .{
            .map_size = options.map_size,
            .max_dbs = 8,
            .no_sync = options.no_sync,
        });

        return Self{
            .env = env,
            .allocator = allocator,
            .next_fact_id = 1,
        };
    }

    pub fn close(self: *Self) void {
        self.env.deinit();
    }

    /// A fact to be added (for batch operations).
    pub const FactInput = struct {
        entities: []const Entity,
        source: ?[]const u8,
    };

    /// Add a fact to the store. Returns the assigned fact ID.
    pub fn addFact(self: *Self, entities: []const Entity, source: ?[]const u8) !u64 {
        const ids = try self.addFacts(&.{FactInput{ .entities = entities, .source = source }});
        defer self.allocator.free(ids);
        return ids[0];
    }

    /// Add multiple facts in a single transaction. Returns assigned fact IDs.
    /// Much faster than calling addFact repeatedly due to amortized transaction overhead.
    pub fn addFacts(self: *Self, facts: []const FactInput) ![]u64 {
        if (facts.len == 0) return &[_]u64{};

        const first_id = self.next_fact_id;
        self.next_fact_id += facts.len;

        const txn = try self.env.transaction(.{ .mode = .ReadWrite });
        errdefer txn.abort();

        // Open databases - indices use DUPSORT for efficient multi-value storage
        const facts_db = try txn.database("facts", .{ .create = true });
        const by_entity_db = try txn.database("by_entity", .{ .dup_sort = true, .create = true });
        const fact_edges_db = try txn.database("fact_edges", .{ .dup_sort = true, .create = true });
        const entity_list_db = try txn.database("entity_list", .{ .dup_sort = true, .create = true });

        // Allocate result array
        const ids = try self.allocator.alloc(u64, facts.len);
        errdefer self.allocator.free(ids);

        for (facts, 0..) |input, i| {
            const fact_id = first_id + i;
            ids[i] = fact_id;

            // Create and serialize the fact
            const fact = Fact{
                .id = fact_id,
                .entities = input.entities,
                .source = input.source,
            };
            var fact_buf: [4096]u8 = undefined;
            const fact_data = try fact.serialize(&fact_buf);

            // Store the fact record
            try facts_db.set(&encodeU64(fact_id), fact_data);

            const fact_id_bytes = encodeU64(fact_id);

            // Index by each entity - DUPSORT means set() adds a value, doesn't replace
            for (input.entities) |entity| {
                var key_buf: [512]u8 = undefined;
                const key = try ek.encodeString(&key_buf, entity.type, entity.id);

                // Add fact_id to by_entity index (DUPSORT: adds to existing key)
                try by_entity_db.set(key.asBytes(), &fact_id_bytes);

                // Add entity_key to fact_edges (DUPSORT: adds to existing key)
                try fact_edges_db.set(&fact_id_bytes, key.asBytes());

                // Add entity id to entity_list (DUPSORT: adds to existing key)
                try entity_list_db.set(entity.type, entity.id);
            }
        }

        try txn.commit();
        return ids;
    }

    /// Get a fact by ID. Caller owns the returned Fact and must call deinit.
    pub fn getFact(self: *Self, fact_id: u64, allocator: std.mem.Allocator) !?Fact {
        const txn = try self.env.transaction(.{ .mode = .ReadOnly });
        defer txn.abort();

        const facts_db = try txn.database("facts", .{});
        const data = try facts_db.get(&encodeU64(fact_id)) orelse return null;

        return try Fact.deserialize(data, allocator);
    }

    /// Query facts by entity. Returns array of fact IDs.
    pub fn getFactsByEntity(self: *Self, entity: Entity, allocator: std.mem.Allocator) ![]u64 {
        const txn = try self.env.transaction(.{ .mode = .ReadOnly });
        defer txn.abort();

        const by_entity_db = try txn.database("by_entity", .{ .dup_sort = true });

        // Build entity key
        var key_buf: [512]u8 = undefined;
        const key = try ek.encodeString(&key_buf, entity.type, entity.id);

        // Use cursor to iterate all values for this key
        var cursor = try by_entity_db.cursor();
        defer cursor.deinit();

        var ids: std.ArrayList(u64) = .{};
        errdefer ids.deinit(allocator);

        // Seek to key, get first value
        var value = try cursor.goToKeyValue(key.asBytes());
        while (value) |v| {
            if (v.len == 8) {
                try ids.append(allocator, decodeU64(v));
            }
            value = try cursor.goToNextDup();
        }

        return ids.toOwnedSlice(allocator);
    }

    /// List all entities of a given type.
    pub fn listEntities(self: *Self, entity_type: []const u8, allocator: std.mem.Allocator) ![][]const u8 {
        const txn = try self.env.transaction(.{ .mode = .ReadOnly });
        defer txn.abort();

        const entity_list_db = try txn.database("entity_list", .{ .dup_sort = true });

        var cursor = try entity_list_db.cursor();
        defer cursor.deinit();

        var entities: std.ArrayList([]const u8) = .{};
        errdefer {
            for (entities.items) |e| allocator.free(e);
            entities.deinit(allocator);
        }

        // Seek to entity type, iterate all values (entity IDs)
        var value = try cursor.goToKeyValue(entity_type);
        while (value) |v| {
            const copy = try allocator.dupe(u8, v);
            try entities.append(allocator, copy);
            value = try cursor.goToNextDup();
        }

        return entities.toOwnedSlice(allocator);
    }

    /// List all entity types.
    pub fn listEntityTypes(self: *Self, allocator: std.mem.Allocator) ![][]const u8 {
        const txn = try self.env.transaction(.{ .mode = .ReadOnly });
        defer txn.abort();

        const entity_list_db = try txn.database("entity_list", .{ .dup_sort = true });

        var types: std.ArrayList([]const u8) = .{};
        errdefer {
            for (types.items) |t| allocator.free(t);
            types.deinit(allocator);
        }

        var cursor = try entity_list_db.cursor();
        defer cursor.deinit();

        // Get first entry
        _ = try cursor.goToFirst() orelse return types.toOwnedSlice(allocator);
        const first_entry = try cursor.getCurrentEntry();
        try types.append(allocator, try allocator.dupe(u8, first_entry.key));

        // Iterate unique keys only (skip duplicates)
        while (try cursor.goToNextNoDup()) |entry| {
            try types.append(allocator, try allocator.dupe(u8, entry.key));
        }

        return types.toOwnedSlice(allocator);
    }
};

fn encodeU64(val: u64) [8]u8 {
    return std.mem.toBytes(std.mem.nativeToBig(u64, val));
}

fn decodeU64(bytes: []const u8) u64 {
    if (bytes.len < 8) return 0;
    return std.mem.bigToNative(u64, std.mem.bytesToValue(u64, bytes[0..8]));
}

test "entity key encoding" {
    const allocator = std.testing.allocator;
    const entity = Entity{ .type = "author", .id = "homer" };
    const key = try entity.toKey(allocator);
    defer allocator.free(key);
    const decoded = try ek.fromBytes(key);
    try std.testing.expectEqualStrings("author", decoded.entityType());
    try std.testing.expectEqualStrings("homer", decoded.value().string);
}

test "u64 encoding roundtrip" {
    const val: u64 = 12345678901234;
    const encoded = encodeU64(val);
    const decoded = decodeU64(&encoded);
    try std.testing.expectEqual(val, decoded);
}
