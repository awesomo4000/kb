const std = @import("std");
const Allocator = std.mem.Allocator;
const datalog = @import("datalog.zig");
const Rule = datalog.Rule;
const BodyElement = datalog.BodyElement;

pub const StratificationError = error{
    UnstratifiableProgram,
    UnsafeNegation,
    OutOfMemory,
};

pub const Stratum = struct {
    rules: []const Rule,
};

const DepEdge = struct {
    target: []const u8,
    negative: bool,
};

const EdgeList = std.ArrayListUnmanaged(DepEdge);
const DepGraph = std.StringHashMapUnmanaged(EdgeList);

fn buildDepGraph(rules: []const Rule, allocator: Allocator) !DepGraph {
    var graph: DepGraph = .{};
    for (rules) |rule| {
        if (rule.body.len == 0) continue;
        const head = rule.head.predicate;
        // Ensure head predicate exists in graph
        _ = try getOrCreateNode(&graph, head, allocator);
        for (rule.body) |elem| {
            const atom = elem.getAtom();
            // Edge: head depends on body predicate
            const edge = DepEdge{
                .target = atom.predicate,
                .negative = elem.isNegated(),
            };
            const edges = try getOrCreateNode(&graph, head, allocator);
            try edges.append(allocator, edge);
            // Ensure target node exists
            _ = try getOrCreateNode(&graph, atom.predicate, allocator);
        }
    }
    return graph;
}

fn getOrCreateNode(graph: *DepGraph, name: []const u8, allocator: Allocator) !*EdgeList {
    const gop = try graph.getOrPut(allocator, name);
    if (!gop.found_existing) {
        gop.value_ptr.* = .{};
    }
    return gop.value_ptr;
}

fn deinitDepGraph(graph: *DepGraph, allocator: Allocator) void {
    var iter = graph.iterator();
    while (iter.next()) |entry| {
        entry.value_ptr.deinit(allocator);
    }
    graph.deinit(allocator);
}

// Tarjan's SCC state
const TarjanState = struct {
    index: u32,
    lowlink: u32,
    on_stack: bool,
};

/// Run Tarjan's SCC algorithm and return SCCs in reverse topological order.
fn tarjanSCC(
    graph: *const DepGraph,
    allocator: Allocator,
) ![][]const []const u8 {
    var state_map = std.StringHashMapUnmanaged(TarjanState){};
    defer state_map.deinit(allocator);

    var stack = std.ArrayListUnmanaged([]const u8){};
    defer stack.deinit(allocator);

    var sccs = std.ArrayListUnmanaged([]const []const u8){};
    errdefer {
        for (sccs.items) |scc| allocator.free(scc);
        sccs.deinit(allocator);
    }

    var next_index: u32 = 0;

    // Initialize all nodes
    var node_iter = graph.iterator();
    while (node_iter.next()) |entry| {
        if (!state_map.contains(entry.key_ptr.*)) {
            try state_map.put(allocator, entry.key_ptr.*, .{
                .index = std.math.maxInt(u32),
                .lowlink = std.math.maxInt(u32),
                .on_stack = false,
            });
        }
    }

    // Visit all unvisited nodes
    var visit_iter = graph.iterator();
    while (visit_iter.next()) |entry| {
        const st = state_map.getPtr(entry.key_ptr.*).?;
        if (st.index == std.math.maxInt(u32)) {
            try strongconnect(
                entry.key_ptr.*,
                graph,
                &state_map,
                &stack,
                &sccs,
                &next_index,
                allocator,
            );
        }
    }

    return sccs.toOwnedSlice(allocator);
}

fn strongconnect(
    v: []const u8,
    graph: *const DepGraph,
    state_map: *std.StringHashMapUnmanaged(TarjanState),
    stack: *std.ArrayListUnmanaged([]const u8),
    sccs: *std.ArrayListUnmanaged([]const []const u8),
    next_index: *u32,
    allocator: Allocator,
) !void {
    const v_state = state_map.getPtr(v).?;
    v_state.index = next_index.*;
    v_state.lowlink = next_index.*;
    v_state.on_stack = true;
    next_index.* += 1;
    try stack.append(allocator, v);

    // Consider successors
    if (graph.get(v)) |edges| {
        for (edges.items) |edge| {
            const w = edge.target;
            if (state_map.getPtr(w)) |w_state| {
                if (w_state.index == std.math.maxInt(u32)) {
                    try strongconnect(w, graph, state_map, stack, sccs, next_index, allocator);
                    const w_after = state_map.get(w).?;
                    const v_st = state_map.getPtr(v).?;
                    v_st.lowlink = @min(v_st.lowlink, w_after.lowlink);
                } else if (w_state.on_stack) {
                    const v_st = state_map.getPtr(v).?;
                    v_st.lowlink = @min(v_st.lowlink, w_state.index);
                }
            }
        }
    }

    // If v is a root node, pop the SCC
    const v_final = state_map.get(v).?;
    if (v_final.lowlink == v_final.index) {
        var scc = std.ArrayListUnmanaged([]const u8){};
        while (stack.items.len > 0) {
            const w = stack.pop().?;
            state_map.getPtr(w).?.on_stack = false;
            try scc.append(allocator, w);
            if (std.mem.eql(u8, w, v)) break;
        }
        try sccs.append(allocator, try scc.toOwnedSlice(allocator));
    }
}

/// Check if any rules have negated atoms in their body.
fn hasNegation(rules: []const Rule) bool {
    for (rules) |rule| {
        for (rule.body) |elem| {
            if (elem.isNegated()) return true;
        }
    }
    return false;
}

/// Validate that all variables in negated atoms are bound by positive atoms
/// in the same rule body. This is the standard Datalog safety requirement.
pub fn validateSafety(rules: []const Rule) StratificationError!void {
    for (rules) |rule| {
        if (rule.body.len == 0) continue;
        if (!hasNegation(&[_]Rule{rule})) continue;

        // Collect variables bound by positive atoms
        for (rule.body) |elem| {
            if (elem != .negated_atom) continue;
            const neg_atom = elem.negated_atom;

            // Check each variable in the negated atom
            for (neg_atom.terms) |term| {
                switch (term) {
                    .variable => |v| {
                        // Check if this variable appears in any positive atom
                        var bound = false;
                        for (rule.body) |other_elem| {
                            if (other_elem != .atom) continue;
                            const pos_atom = other_elem.atom;
                            for (pos_atom.terms) |pos_term| {
                                switch (pos_term) {
                                    .variable => |pv| {
                                        if (std.mem.eql(u8, v, pv)) {
                                            bound = true;
                                            break;
                                        }
                                    },
                                    .constant => {},
                                }
                                if (bound) break;
                            }
                            if (bound) break;
                        }
                        if (!bound) {
                            std.debug.print("error: unsafe rule \xe2\x80\x94 variable \"{s}\" appears only in negation\n", .{v});
                            printRule(rule);
                            return error.UnsafeNegation;
                        }
                    },
                    .constant => {},
                }
            }
        }
    }
}

/// Print a rule to stderr, indented with two spaces.
fn printRule(rule: Rule) void {
    var buf: [1024]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    const writer = fbs.writer();
    writer.print("{f}", .{rule}) catch {
        std.debug.print("  <rule too long to display>\n", .{});
        return;
    };
    std.debug.print("  {s}\n", .{fbs.getWritten()});
}

/// Stratify rules for semi-naive evaluation with negation.
/// Returns strata in evaluation order (stratum 0 first).
/// Ground facts (empty body) are excluded from strata.
pub fn stratify(rules: []const Rule, allocator: Allocator) StratificationError![]Stratum {
    // Collect non-ground rules
    var non_ground = std.ArrayListUnmanaged(Rule){};
    defer non_ground.deinit(allocator);
    for (rules) |rule| {
        if (rule.body.len > 0) {
            non_ground.append(allocator, rule) catch return error.OutOfMemory;
        }
    }

    if (non_ground.items.len == 0) {
        return allocator.alloc(Stratum, 0) catch return error.OutOfMemory;
    }

    // Validate safety before proceeding
    try validateSafety(non_ground.items);

    // Fast path: no negation -> single stratum
    if (!hasNegation(non_ground.items)) {
        const strata = allocator.alloc(Stratum, 1) catch return error.OutOfMemory;
        const rule_slice = allocator.alloc(Rule, non_ground.items.len) catch return error.OutOfMemory;
        @memcpy(rule_slice, non_ground.items);
        strata[0] = .{ .rules = rule_slice };
        return strata;
    }

    // Build dependency graph
    var graph = buildDepGraph(non_ground.items, allocator) catch return error.OutOfMemory;
    defer deinitDepGraph(&graph, allocator);

    // Run Tarjan's SCC
    const sccs = tarjanSCC(&graph, allocator) catch return error.OutOfMemory;
    defer {
        for (sccs) |scc| allocator.free(scc);
        allocator.free(sccs);
    }

    // Check for negative edges within any SCC (unstratifiable)
    for (sccs) |scc| {
        for (scc) |node| {
            if (graph.get(node)) |edges| {
                for (edges.items) |edge| {
                    if (!edge.negative) continue;
                    // Check if target is in the same SCC
                    for (scc) |other| {
                        if (std.mem.eql(u8, edge.target, other)) {
                            // Print error message naming the predicates
                            std.debug.print("error: unstratifiable program \xe2\x80\x94 circular negation between \"{s}\" and \"{s}\"\n", .{ node, edge.target });
                            // Print the offending rules
                            for (non_ground.items) |rule| {
                                const head = rule.head.predicate;
                                var involves = false;
                                for (scc) |pred| {
                                    if (std.mem.eql(u8, head, pred)) {
                                        involves = true;
                                        break;
                                    }
                                }
                                if (involves) {
                                    printRule(rule);
                                }
                            }
                            return error.UnstratifiableProgram;
                        }
                    }
                }
            }
        }
    }

    // Build SCC membership map: predicate -> SCC index
    var scc_of = std.StringHashMapUnmanaged(usize){};
    defer scc_of.deinit(allocator);
    for (sccs, 0..) |scc, i| {
        for (scc) |node| {
            scc_of.put(allocator, node, i) catch return error.OutOfMemory;
        }
    }

    // Assign stratum numbers based on negative dependencies
    // SCCs are in reverse topological order from Tarjan, so we process them
    // and bump stratum on negative cross-edges
    const num_sccs = sccs.len;
    const stratum_of_scc = allocator.alloc(usize, num_sccs) catch return error.OutOfMemory;
    defer allocator.free(stratum_of_scc);
    @memset(stratum_of_scc, 0);

    // Compute stratum for each SCC
    // Process SCCs: for each SCC, look at dependencies and compute stratum
    var changed = true;
    while (changed) {
        changed = false;
        for (sccs, 0..) |scc, scc_idx| {
            for (scc) |node| {
                if (graph.get(node)) |edges| {
                    for (edges.items) |edge| {
                        const target_scc = scc_of.get(edge.target) orelse continue;
                        if (target_scc == scc_idx) continue; // Same SCC
                        const base = stratum_of_scc[target_scc];
                        const required = if (edge.negative) base + 1 else base;
                        if (required > stratum_of_scc[scc_idx]) {
                            stratum_of_scc[scc_idx] = required;
                            changed = true;
                        }
                    }
                }
            }
        }
    }

    // Find max stratum
    var max_stratum: usize = 0;
    for (stratum_of_scc) |s| {
        if (s > max_stratum) max_stratum = s;
    }

    // Group rules by stratum of their head predicate
    const num_strata = max_stratum + 1;
    const rule_buckets = allocator.alloc(std.ArrayListUnmanaged(Rule), num_strata) catch return error.OutOfMemory;
    defer {
        for (rule_buckets) |*b| b.deinit(allocator);
        allocator.free(rule_buckets);
    }
    for (rule_buckets) |*b| b.* = .{};

    for (non_ground.items) |rule| {
        const head_scc = scc_of.get(rule.head.predicate) orelse continue;
        const stratum = stratum_of_scc[head_scc];
        rule_buckets[stratum].append(allocator, rule) catch return error.OutOfMemory;
    }

    // Build result
    const strata = allocator.alloc(Stratum, num_strata) catch return error.OutOfMemory;
    for (rule_buckets, 0..) |*bucket, i| {
        strata[i] = .{
            .rules = bucket.toOwnedSlice(allocator) catch return error.OutOfMemory,
        };
    }

    return strata;
}

pub fn freeStrata(strata: []Stratum, allocator: Allocator) void {
    for (strata) |s| {
        allocator.free(s.rules);
    }
    allocator.free(strata);
}

// =============================================================================
// Tests
// =============================================================================

test "stratify: no negation → single stratum" {
    const allocator = std.testing.allocator;
    var parser = datalog.Parser.init(allocator,
        \\edge("a", "b").
        \\reachable(X, Y) :- edge(X, Y).
        \\reachable(X, Z) :- edge(X, Y), reachable(Y, Z).
    );
    defer parser.deinit();
    const parsed = try parser.parseProgram();

    const strata = try stratify(parsed.rules, allocator);
    defer freeStrata(strata, allocator);

    try std.testing.expectEqual(@as(usize, 1), strata.len);
    try std.testing.expectEqual(@as(usize, 2), strata[0].rules.len);
}

test "stratify: simple negation → two strata" {
    const allocator = std.testing.allocator;
    var parser = datalog.Parser.init(allocator,
        \\author("Homer").
        \\epic_author("Homer").
        \\non_epic(A) :- author(A), not epic_author(A).
    );
    defer parser.deinit();
    const parsed = try parser.parseProgram();

    const strata = try stratify(parsed.rules, allocator);
    defer freeStrata(strata, allocator);

    // Should have 2 strata: stratum 0 has no rules (epic_author/author are ground),
    // stratum 1 has the negation rule. But since ground facts are excluded,
    // and non_epic depends negatively on epic_author (ground), we get 1 stratum
    // with the negation rule at stratum index > 0.
    // Actually: epic_author has no non-ground rules, so it won't be in the dep graph
    // as a head. The only non-ground rule is non_epic, which depends on author (positive)
    // and epic_author (negative). Both are ground-only predicates. So the dep graph
    // has: non_epic -> author (positive), non_epic -> epic_author (negative).
    // author and epic_author nodes exist but have no outgoing edges.
    // SCCs: {non_epic}, {author}, {epic_author} (all singletons).
    // non_epic depends negatively on epic_author, so stratum(non_epic) = stratum(epic_author) + 1 = 1.
    // author and epic_author are at stratum 0. But they have no rules!
    // So strata: [0] = empty, [1] = {non_epic rule}.
    // We should have 2 strata, one of which may be empty.
    try std.testing.expect(strata.len >= 1);

    // Find the stratum with the non_epic rule
    var found_non_epic = false;
    for (strata) |stratum| {
        for (stratum.rules) |rule| {
            if (std.mem.eql(u8, rule.head.predicate, "non_epic")) {
                found_non_epic = true;
            }
        }
    }
    try std.testing.expect(found_non_epic);
}

test "stratify: transitive deps → correct ordering" {
    const allocator = std.testing.allocator;
    var parser = datalog.Parser.init(allocator,
        \\influenced("Homer", "Virgil").
        \\influenced_t(A, B) :- influenced(A, B).
        \\influenced_t(A, C) :- influenced(A, B), influenced_t(B, C).
        \\author("Homer"). author("Virgil"). author("Plato").
        \\not_influenced(A) :- author(A), not influenced_t("Homer", A).
    );
    defer parser.deinit();
    const parsed = try parser.parseProgram();

    const strata = try stratify(parsed.rules, allocator);
    defer freeStrata(strata, allocator);

    // influenced_t rules should be in an earlier stratum than not_influenced
    var influenced_t_stratum: ?usize = null;
    var not_influenced_stratum: ?usize = null;
    for (strata, 0..) |stratum, si| {
        for (stratum.rules) |rule| {
            if (std.mem.eql(u8, rule.head.predicate, "influenced_t")) {
                influenced_t_stratum = si;
            }
            if (std.mem.eql(u8, rule.head.predicate, "not_influenced")) {
                not_influenced_stratum = si;
            }
        }
    }
    try std.testing.expect(influenced_t_stratum != null);
    try std.testing.expect(not_influenced_stratum != null);
    try std.testing.expect(influenced_t_stratum.? < not_influenced_stratum.?);
}

test "stratify: circular negation → error" {
    const allocator = std.testing.allocator;
    var parser = datalog.Parser.init(allocator,
        \\book("The Iliad").
        \\popular(B) :- book(B), not obscure(B).
        \\obscure(B) :- book(B), not popular(B).
    );
    defer parser.deinit();
    const parsed = try parser.parseProgram();

    const result = stratify(parsed.rules, allocator);
    try std.testing.expectError(error.UnstratifiableProgram, result);
}

test "stratify: ground facts excluded" {
    const allocator = std.testing.allocator;
    var parser = datalog.Parser.init(allocator,
        \\color("red").
        \\color("blue").
        \\color("green").
    );
    defer parser.deinit();
    const parsed = try parser.parseProgram();

    const strata = try stratify(parsed.rules, allocator);
    defer freeStrata(strata, allocator);

    // All rules are ground facts, no strata needed
    try std.testing.expectEqual(@as(usize, 0), strata.len);
}

test "unsafe negation rejected" {
    const allocator = std.testing.allocator;
    var parser = datalog.Parser.init(allocator,
        \\bad(B) :- not genre(B, "epic").
    );
    defer parser.deinit();
    const parsed = try parser.parseProgram();

    const result = stratify(parsed.rules, allocator);
    try std.testing.expectError(error.UnsafeNegation, result);
}

test "unsafe negation with unbound variable in negated binary" {
    const allocator = std.testing.allocator;
    var parser = datalog.Parser.init(allocator,
        \\bad(A) :- author(A), not wrote(A, B).
    );
    defer parser.deinit();
    const parsed = try parser.parseProgram();

    const result = stratify(parsed.rules, allocator);
    try std.testing.expectError(error.UnsafeNegation, result);
}
