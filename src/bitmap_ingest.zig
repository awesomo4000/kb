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

// Re-exports from relation.zig (public API unchanged)
pub const RelationMap = relation_mod.RelationMap;
pub const deinitRelations = relation_mod.deinitRelations;
pub const persistRelations = relation_mod.persistRelations;
pub const loadRelations = relation_mod.loadRelations;

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

const test_helpers = @import("test_helpers.zig");
const createTempLmdbEnv = test_helpers.createTempLmdbEnv;
const destroyTempLmdbEnv = test_helpers.destroyTempLmdbEnv;

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
