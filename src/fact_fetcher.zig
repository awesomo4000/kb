const std = @import("std");
const Fact = @import("fact.zig").Fact;
const Entity = @import("fact.zig").Entity;
const Mapping = @import("datalog.zig").Mapping;
const FactStore = @import("fact_store.zig").FactStore;

/// Interface for fetching raw facts for a mapping.
/// Used by bitmap ingest to pull facts from storage.
pub const FactFetcher = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        fetchFacts: *const fn (
            ptr: *anyopaque,
            mapping: Mapping,
            allocator: std.mem.Allocator,
        ) anyerror![]const Fact,
    };

    pub fn fetchFacts(
        self: FactFetcher,
        mapping: Mapping,
        allocator: std.mem.Allocator,
    ) ![]const Fact {
        return self.vtable.fetchFacts(self.ptr, mapping, allocator);
    }
};

/// LMDB-backed implementation of FactFetcher.
pub const HypergraphFetcher = struct {
    store: *FactStore,

    pub fn init(store: *FactStore) HypergraphFetcher {
        return .{ .store = store };
    }

    pub fn fetcher(self: *HypergraphFetcher) FactFetcher {
        return .{
            .ptr = self,
            .vtable = &.{ .fetchFacts = fetchFactsImpl },
        };
    }

    fn fetchFactsImpl(
        ptr: *anyopaque,
        mapping: Mapping,
        allocator: std.mem.Allocator,
    ) anyerror![]const Fact {
        const self: *HypergraphFetcher = @ptrCast(@alignCast(ptr));
        return self.fetchFacts(mapping, allocator);
    }

    pub fn fetchFacts(
        self: *HypergraphFetcher,
        mapping: Mapping,
        allocator: std.mem.Allocator,
    ) ![]const Fact {
        // 1. Find anchor: first constant in the mapping pattern
        const anchor = findAnchor(mapping) orelse return &[_]Fact{};

        // 2. Query LMDB for fact IDs containing this entity
        const fact_ids = try self.store.getFactsByEntity(anchor, allocator);
        defer allocator.free(fact_ids);

        // 3. Fetch each fact, filter by mapping pattern match
        var results: std.ArrayList(Fact) = .{};
        errdefer {
            for (results.items) |*f| f.deinit(allocator);
            results.deinit(allocator);
        }

        for (fact_ids) |fact_id| {
            const fact = try self.store.getFact(fact_id, allocator) orelse continue;
            errdefer fact.deinit(allocator);

            if (matchesMappingPattern(fact, mapping)) {
                try results.append(allocator, fact);
            } else {
                fact.deinit(allocator);
            }
        }

        return results.toOwnedSlice(allocator);
    }
};

/// Extract the first constant entity from the mapping pattern.
/// Returns null if the pattern has no constants (all variables).
pub fn findAnchor(mapping: Mapping) ?Entity {
    for (mapping.pattern) |elem| {
        switch (elem.value) {
            .constant => |c| return Entity{ .type = elem.entity_type, .id = c },
            .variable => continue,
        }
    }
    return null;
}

/// Check that a fact's entity structure matches the mapping pattern.
/// Filters out unrelated facts that happen to share the same anchor entity.
pub fn matchesMappingPattern(fact: Fact, mapping: Mapping) bool {
    // Arity must match
    if (fact.entities.len != mapping.pattern.len) return false;

    // Every pattern element must match positionally
    for (mapping.pattern, fact.entities) |elem, entity| {
        // Entity type must match
        if (!std.mem.eql(u8, elem.entity_type, entity.type)) return false;

        // Constants must match exactly
        switch (elem.value) {
            .constant => |c| {
                if (!std.mem.eql(u8, c, entity.id)) return false;
            },
            .variable => {}, // Variables match anything
        }
    }

    return true;
}

// =============================================================================
// Tests
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

    fn addFacts(
        self: *MockFetcher,
        predicate: []const u8,
        facts: []const Fact,
    ) !void {
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

test "findAnchor returns first constant entity" {
    // @map influenced(A, B) [rel:influenced-by, author:$A, author:$B]
    const pattern = [_]Mapping.PatternElement{
        .{ .entity_type = "rel", .value = .{ .constant = "influenced-by" } },
        .{ .entity_type = "author", .value = .{ .variable = "A" } },
        .{ .entity_type = "author", .value = .{ .variable = "B" } },
    };
    const args = [_][]const u8{ "A", "B" };
    const mapping = Mapping{
        .predicate = "influenced",
        .args = &args,
        .pattern = &pattern,
    };

    const anchor = findAnchor(mapping).?;
    try std.testing.expectEqualStrings("rel", anchor.type);
    try std.testing.expectEqualStrings("influenced-by", anchor.id);
}

test "findAnchor returns null for all-variable pattern" {
    const pattern = [_]Mapping.PatternElement{
        .{ .entity_type = "rel", .value = .{ .variable = "R" } },
        .{ .entity_type = "thing", .value = .{ .variable = "X" } },
    };
    const args = [_][]const u8{ "R", "X" };
    const mapping = Mapping{
        .predicate = "any",
        .args = &args,
        .pattern = &pattern,
    };

    try std.testing.expectEqual(@as(?Entity, null), findAnchor(mapping));
}

test "matchesMappingPattern accepts matching fact" {
    const pattern = [_]Mapping.PatternElement{
        .{ .entity_type = "rel", .value = .{ .constant = "wrote" } },
        .{ .entity_type = "author", .value = .{ .variable = "A" } },
        .{ .entity_type = "book", .value = .{ .variable = "B" } },
    };
    const args = [_][]const u8{ "A", "B" };
    const mapping = Mapping{
        .predicate = "wrote",
        .args = &args,
        .pattern = &pattern,
    };

    const entities = [_]Entity{
        .{ .type = "rel", .id = "wrote" },
        .{ .type = "author", .id = "Homer" },
        .{ .type = "book", .id = "Iliad" },
    };
    const fact = Fact{ .id = 1, .entities = &entities, .source = null };

    try std.testing.expect(matchesMappingPattern(fact, mapping));
}

test "matchesMappingPattern rejects wrong arity" {
    const pattern = [_]Mapping.PatternElement{
        .{ .entity_type = "rel", .value = .{ .constant = "wrote" } },
        .{ .entity_type = "author", .value = .{ .variable = "A" } },
    };
    const args = [_][]const u8{"A"};
    const mapping = Mapping{
        .predicate = "wrote",
        .args = &args,
        .pattern = &pattern,
    };

    // Fact has 3 entities, mapping expects 2
    const entities = [_]Entity{
        .{ .type = "rel", .id = "wrote" },
        .{ .type = "author", .id = "Homer" },
        .{ .type = "book", .id = "Iliad" },
    };
    const fact = Fact{ .id = 1, .entities = &entities, .source = null };

    try std.testing.expect(!matchesMappingPattern(fact, mapping));
}

test "matchesMappingPattern rejects wrong constant" {
    const pattern = [_]Mapping.PatternElement{
        .{ .entity_type = "rel", .value = .{ .constant = "wrote" } },
        .{ .entity_type = "author", .value = .{ .variable = "A" } },
        .{ .entity_type = "book", .value = .{ .variable = "B" } },
    };
    const args = [_][]const u8{ "A", "B" };
    const mapping = Mapping{
        .predicate = "wrote",
        .args = &args,
        .pattern = &pattern,
    };

    // Fact has "translated" instead of "wrote"
    const entities = [_]Entity{
        .{ .type = "rel", .id = "translated" },
        .{ .type = "author", .id = "Lattimore" },
        .{ .type = "book", .id = "Iliad" },
    };
    const fact = Fact{ .id = 1, .entities = &entities, .source = null };

    try std.testing.expect(!matchesMappingPattern(fact, mapping));
}

test "matchesMappingPattern rejects wrong entity type" {
    const pattern = [_]Mapping.PatternElement{
        .{ .entity_type = "rel", .value = .{ .constant = "wrote" } },
        .{ .entity_type = "author", .value = .{ .variable = "A" } },
        .{ .entity_type = "book", .value = .{ .variable = "B" } },
    };
    const args = [_][]const u8{ "A", "B" };
    const mapping = Mapping{
        .predicate = "wrote",
        .args = &args,
        .pattern = &pattern,
    };

    // Second entity is "editor" not "author"
    const entities = [_]Entity{
        .{ .type = "rel", .id = "wrote" },
        .{ .type = "editor", .id = "Homer" },
        .{ .type = "book", .id = "Iliad" },
    };
    const fact = Fact{ .id = 1, .entities = &entities, .source = null };

    try std.testing.expect(!matchesMappingPattern(fact, mapping));
}

test "FactFetcher interface with mock" {
    const allocator = std.testing.allocator;

    // Build some mock facts
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

    var f = mock.fetcher();

    const args = [_][]const u8{ "A", "B" };
    const pattern = [_]Mapping.PatternElement{
        .{ .entity_type = "rel", .value = .{ .constant = "wrote" } },
        .{ .entity_type = "author", .value = .{ .variable = "A" } },
        .{ .entity_type = "book", .value = .{ .variable = "B" } },
    };
    const mapping = Mapping{
        .predicate = "wrote",
        .args = &args,
        .pattern = &pattern,
    };

    const facts = try f.fetchFacts(mapping, allocator);
    defer {
        for (facts) |fact| {
            fact.deinit(allocator);
        }
        allocator.free(facts);
    }

    try std.testing.expectEqual(@as(usize, 1), facts.len);
    try std.testing.expectEqualStrings("Homer", facts[0].entities[1].id);
}
