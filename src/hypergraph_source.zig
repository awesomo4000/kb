const std = @import("std");
const Allocator = std.mem.Allocator;
const datalog = @import("datalog.zig");
const FactStore = @import("fact_store.zig").FactStore;
const Entity = @import("fact.zig").Entity;

/// Adapts hypergraph FactStore to Datalog's FactSource interface.
/// Uses @map directives to translate predicate queries into hypergraph lookups.
pub const HypergraphFactSource = struct {
    store: *FactStore,
    mappings: []const datalog.Mapping,
    alloc: Allocator,

    const Self = @This();

    pub fn init(store: *FactStore, mappings: []const datalog.Mapping, alloc: Allocator) Self {
        return .{
            .store = store,
            .mappings = mappings,
            .alloc = alloc,
        };
    }

    /// Get the FactSource interface for use with Evaluator
    pub fn source(self: *Self) datalog.FactSource {
        return .{
            .ptr = self,
            .vtable = &.{ .matchAtom = matchAtomImpl },
        };
    }

    fn matchAtomImpl(ptr: *anyopaque, pattern: datalog.Atom, alloc: Allocator) Allocator.Error![]datalog.Binding {
        const self: *Self = @ptrCast(@alignCast(ptr));
        return self.matchAtom(pattern, alloc) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return &[_]datalog.Binding{}, // Other errors -> no results
        };
    }

    /// Match a Datalog atom pattern against hypergraph facts
    pub fn matchAtom(self: *Self, pattern: datalog.Atom, alloc: Allocator) ![]datalog.Binding {
        // Find mapping for this predicate
        const mapping = self.findMapping(pattern.predicate) orelse {
            // No mapping -> no results from hypergraph
            return &[_]datalog.Binding{};
        };

        // Validate arity matches
        if (pattern.terms.len != mapping.args.len) {
            return &[_]datalog.Binding{};
        }

        // Find a constant in the pattern to use as anchor for hypergraph query
        // We need at least one constant entity to query by
        var anchor_entity: ?Entity = null;

        for (mapping.pattern) |elem| {
            switch (elem.value) {
                .constant => |c| {
                    // This is a constant in the mapping - use as anchor
                    anchor_entity = .{ .type = elem.entity_type, .id = c };
                    break;
                },
                .variable => |v| {
                    // Check if this variable is bound to a constant in the query
                    const arg_idx = self.findArgIndex(mapping.args, v) orelse continue;
                    if (arg_idx < pattern.terms.len) {
                        switch (pattern.terms[arg_idx]) {
                            .constant => |c| {
                                anchor_entity = .{ .type = elem.entity_type, .id = c };
                                break;
                            },
                            .variable => continue,
                        }
                    }
                },
            }
        }

        const anchor = anchor_entity orelse {
            // No anchor found - would need to scan all facts (not supported yet)
            // TODO: Could iterate all entities of first type in pattern
            return &[_]datalog.Binding{};
        };

        // Query hypergraph for facts containing the anchor entity
        const fact_ids = self.store.getFactsByEntity(anchor, alloc) catch {
            return &[_]datalog.Binding{};
        };
        defer alloc.free(fact_ids);

        var results: std.ArrayList(datalog.Binding) = .{};

        for (fact_ids) |fact_id| {
            const fact = self.store.getFact(fact_id, alloc) catch continue orelse continue;
            defer fact.deinit(alloc);

            // Try to match this fact against the pattern
            if (try self.matchFactAgainstPattern(fact.entities, pattern, mapping, alloc)) |binding| {
                try results.append(alloc, binding);
            }
        }

        return results.toOwnedSlice(alloc);
    }

    /// Match a hypergraph fact against the Datalog pattern using the mapping
    fn matchFactAgainstPattern(
        self: *Self,
        fact_entities: []const Entity,
        pattern: datalog.Atom,
        mapping: datalog.Mapping,
        alloc: Allocator,
    ) !?datalog.Binding {
        var binding = datalog.Binding.init(alloc);

        // For each element in the mapping pattern, find matching entity in fact
        for (mapping.pattern) |elem| {
            // Find entity in fact with matching type
            const fact_entity = self.findEntityByType(fact_entities, elem.entity_type) orelse {
                // Fact doesn't have required entity type
                return null;
            };

            switch (elem.value) {
                .constant => |c| {
                    // Must match exactly
                    if (!std.mem.eql(u8, fact_entity.id, c)) {
                        return null;
                    }
                },
                .variable => |v| {
                    // Find which arg this maps to
                    const arg_idx = self.findArgIndex(mapping.args, v) orelse continue;
                    if (arg_idx >= pattern.terms.len) continue;

                    const pattern_term = pattern.terms[arg_idx];
                    switch (pattern_term) {
                        .constant => |c| {
                            // Query has constant - must match
                            if (!std.mem.eql(u8, fact_entity.id, c)) {
                                return null;
                            }
                        },
                        .variable => |var_name| {
                            // Query has variable - bind it
                            if (binding.get(var_name)) |existing| {
                                // Already bound - check consistency
                                if (!std.mem.eql(u8, existing, fact_entity.id)) {
                                    return null;
                                }
                            } else {
                                try binding.put(var_name, fact_entity.id);
                            }
                        },
                    }
                },
            }
        }

        return binding;
    }

    fn findMapping(self: *Self, predicate: []const u8) ?datalog.Mapping {
        for (self.mappings) |m| {
            if (std.mem.eql(u8, m.predicate, predicate)) {
                return m;
            }
        }
        return null;
    }

    fn findArgIndex(_: *Self, args: []const []const u8, name: []const u8) ?usize {
        for (args, 0..) |arg, i| {
            if (std.mem.eql(u8, arg, name)) {
                return i;
            }
        }
        return null;
    }

    fn findEntityByType(_: *Self, entities: []const Entity, entity_type: []const u8) ?Entity {
        for (entities) |e| {
            if (std.mem.eql(u8, e.type, entity_type)) {
                return e;
            }
        }
        return null;
    }
};

// =============================================================================
// Tests
// =============================================================================

test "hypergraph source basic query" {
    // This test requires a FactStore which needs LMDB
    // For now, just verify the types compile
    const alloc = std.testing.allocator;

    // Create a mock mapping
    var pattern_elems = [_]datalog.Mapping.PatternElement{
        .{ .entity_type = "rel", .value = .{ .constant = "wrote" } },
        .{ .entity_type = "author", .value = .{ .variable = "A" } },
        .{ .entity_type = "book", .value = .{ .variable = "B" } },
    };
    var args = [_][]const u8{ "A", "B" };
    const mapping = datalog.Mapping{
        .predicate = "wrote",
        .args = &args,
        .pattern = &pattern_elems,
    };

    _ = alloc;
    _ = mapping;
    // Full integration test would need actual LMDB store
}
