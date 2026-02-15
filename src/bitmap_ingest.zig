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
/// Does NOT persist -- call persistRelations and interner.persist separately.
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

        // 2. Build arg -> pattern position lookup
        const arg_positions = try buildArgPositions(mapping, allocator);
        defer allocator.free(arg_positions);

        // 3. Ensure relation exists in the map (dupe key for ownership)
        const owned_pred = try allocator.dupe(u8, mapping.predicate);
        const rel_gop = try relations.getOrPut(allocator, owned_pred);
        if (!rel_gop.found_existing) {
            rel_gop.value_ptr.* = switch (arity) {
                1 => .{ .unary = try UnaryRelation.init(allocator) },
                2 => .{ .binary = try BinaryRelation.init(allocator) },
                else => unreachable,
            };
        } else {
            allocator.free(owned_pred);
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

/// Free all relations in a RelationMap, including duped predicate keys.
pub fn deinitRelations(relations: *RelationMap, allocator: std.mem.Allocator) void {
    var iter = relations.iterator();
    while (iter.next()) |entry| {
        allocator.free(@constCast(entry.key_ptr.*));
        entry.value_ptr.deinit();
    }
    relations.deinit(allocator);
}

// =============================================================================
// Internal helpers
// =============================================================================

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
            allocator.free(positions);
            return error.MappingArgNotInPattern;
        };
    }
    return positions;
}

/// Intern an entity as "type\x00id".
fn internEntity(interner: *StringInterner, entity: Entity) !u32 {
    // Build "type\x00id" key using stack buffer for common case
    var buf: [512]u8 = undefined;
    const total_len = entity.type.len + 1 + entity.id.len;
    if (total_len > buf.len) {
        @panic("entity key too long for stack buffer");
    }
    @memcpy(buf[0..entity.type.len], entity.type);
    buf[entity.type.len] = 0;
    @memcpy(buf[entity.type.len + 1 ..][0..entity.id.len], entity.id);
    const key = buf[0..total_len];

    return interner.intern(key);
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
// Test helpers
// =============================================================================

const MockFetcher = struct {
    facts_by_predicate: std.StringHashMapUnmanaged([]const Fact),
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator) MockFetcher {
        return .{
            .facts_by_predicate = .{},
            .allocator = allocator,
        };
    }

    fn deinit(self: *MockFetcher) void {
        self.facts_by_predicate.deinit(self.allocator);
    }

    fn addFacts(self: *MockFetcher, predicate: []const u8, facts: []const Fact) !void {
        try self.facts_by_predicate.put(self.allocator, predicate, facts);
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

fn createTempLmdbEnv(allocator: std.mem.Allocator) !struct { env: lmdb.Environment, path: [:0]const u8 } {
    const random = std.crypto.random.int(u64);
    const path = try std.fmt.allocPrint(allocator, "/tmp/kb-bitmap-test-{x}", .{random});
    const path_z = try allocator.realloc(path, path.len + 1);
    path_z[path.len] = 0;
    const path_sentinel = path_z[0..path.len :0];

    std.fs.makeDirAbsolute(path_sentinel) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };

    const env = try lmdb.Environment.init(path_sentinel, .{ .max_dbs = 8 });
    return .{ .env = env, .path = path_sentinel };
}

fn destroyTempLmdbEnv(state: anytype, allocator: std.mem.Allocator) void {
    state.env.deinit();
    std.fs.deleteTreeAbsolute(state.path) catch {};
    allocator.free(state.path);
}

// =============================================================================
// Tests
// =============================================================================

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

test "internEntity uses type-null-id format" {
    var interner = StringInterner.init(std.testing.allocator);
    defer interner.deinit();

    const id = try internEntity(&interner, Entity{ .type = "author", .id = "Homer" });
    try std.testing.expectEqualStrings("author\x00Homer", interner.resolve(id));

    // Different type, same id -> different intern ID
    const id2 = try internEntity(&interner, Entity{ .type = "book", .id = "Homer" });
    try std.testing.expect(id != id2);
    try std.testing.expectEqualStrings("book\x00Homer", interner.resolve(id2));
}

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
    const ff = mock.fetcher();

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
    try std.testing.expect(relations.get("influenced_by") != null);

    // Should have 2 tuples: (Virgil, Homer) and (Dante, Virgil)
    var bin_mut = &relations.getPtr("influenced_by").?.binary;
    try std.testing.expectEqual(@as(u64, 2), bin_mut.tupleCount());

    // Verify specific tuples via interner
    const virgil_id = interner.lookup("author\x00Virgil").?;
    const homer_id = interner.lookup("author\x00Homer").?;
    const dante_id = interner.lookup("author\x00Dante").?;

    const bin = relations.get("influenced_by").?.binary;
    try std.testing.expect(bin.contains(virgil_id, homer_id));
    try std.testing.expect(bin.contains(dante_id, virgil_id));
    try std.testing.expect(!bin.contains(homer_id, virgil_id)); // not reversed
}

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
    const ff = mock.fetcher();

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

    // "wrote" should have (Homer, Iliad) -- note different entity types
    const homer_id = interner.lookup("author\x00Homer").?;
    const iliad_id = interner.lookup("book\x00Iliad").?;
    try std.testing.expect(relations.get("wrote").?.binary.contains(homer_id, iliad_id));
}

test "ingest handles mapping with no matching facts" {
    const allocator = std.testing.allocator;

    var mock = MockFetcher.init(allocator);
    defer mock.deinit();
    // No facts added
    const ff = mock.fetcher();

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

test "full ingest-persist-load roundtrip" {
    const allocator = std.testing.allocator;

    var state = try createTempLmdbEnv(allocator);
    defer destroyTempLmdbEnv(&state, allocator);

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
        const ff = mock.fetcher();

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
        try interner.persist(state.env);
        try persistRelations(&relations, state.env, allocator);
    }

    // Load fresh and verify
    {
        var interner = try StringInterner.load(allocator, state.env);
        defer interner.deinit();
        var relations = try loadRelations(state.env, allocator);
        defer deinitRelations(&relations, allocator);

        // Interner should have the entities
        const homer_id = interner.lookup("author\x00Homer").?;
        const iliad_id = interner.lookup("book\x00Iliad").?;

        // Relation should have the tuple
        const rel = relations.get("wrote") orelse return error.RelationNotFound;
        try std.testing.expect(rel.binary.contains(homer_id, iliad_id));
    }
}

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
    const ff = mock.fetcher();

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

    // Should still be 1 tuple -- bitmap insert is idempotent
    var bin = &relations.getPtr("wrote").?.binary;
    try std.testing.expectEqual(@as(u64, 1), bin.tupleCount());
}
