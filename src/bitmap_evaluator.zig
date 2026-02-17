const std = @import("std");
const rawr = @import("rawr");
const RoaringBitmap = rawr.RoaringBitmap;
const StringInterner = @import("string_interner.zig").StringInterner;
const relation_mod = @import("relation.zig");
const BinaryRelation = relation_mod.BinaryRelation;
const UnaryRelation = relation_mod.UnaryRelation;
const Relation = relation_mod.Relation;
const RelationMap = relation_mod.RelationMap;
const deinitRelations = relation_mod.deinitRelations;
const datalog = @import("datalog.zig");
const Rule = datalog.Rule;
const Atom = datalog.Atom;
const Term = datalog.Term;
const Binding = datalog.Binding;
const Mapping = datalog.Mapping;
const FactFetcher = @import("fact_fetcher.zig").FactFetcher;
const Fact = @import("fact.zig").Fact;
const Entity = @import("fact.zig").Entity;
const bitmap_ingest = @import("bitmap_ingest.zig");
const buildArgPositions = bitmap_ingest.buildArgPositions;

const VarRef = struct {
    atom_idx: u8,
    term_pos: u8,
};

const JoinAnalysis = struct {
    join_var: []const u8,
    left_pos: u8,
    right_pos: u8,
    head_map: [2]VarRef,
};

pub const BitmapEvaluator = struct {
    interner: StringInterner,
    relations: RelationMap,
    rules: []const Rule,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, rules: []const Rule) BitmapEvaluator {
        return .{
            .interner = StringInterner.init(allocator),
            .relations = .{},
            .rules = rules,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *BitmapEvaluator) void {
        deinitRelations(&self.relations, self.allocator);
        self.interner.deinit();
    }

    /// Insert ground facts (rules with empty body) into relations.
    pub fn addGroundFacts(self: *BitmapEvaluator, rules: []const Rule) !void {
        for (rules) |rule| {
            if (rule.body.len != 0) continue;
            const arity = rule.head.terms.len;
            if (arity == 0 or arity > 2) continue;

            switch (arity) {
                1 => {
                    const id0 = try self.internTerm(rule.head.terms[0]) orelse continue;
                    const rel = try self.ensureRelation(rule.head.predicate, 1);
                    try rel.unary.insert(id0);
                },
                2 => {
                    const id0 = try self.internTerm(rule.head.terms[0]) orelse continue;
                    const id1 = try self.internTerm(rule.head.terms[1]) orelse continue;
                    const rel = try self.ensureRelation(rule.head.predicate, 2);
                    try rel.binary.insert(id0, id1);
                },
                else => unreachable,
            }
        }
    }

    /// Load facts from an external store via @map directives.
    /// Interns entity IDs as bare strings (not type-prefixed).
    pub fn loadMappedFacts(
        self: *BitmapEvaluator,
        ff: FactFetcher,
        mappings: []const Mapping,
    ) !void {
        for (mappings) |mapping| {
            const arity = mapping.args.len;
            if (arity == 0 or arity > 2) continue;

            const arg_positions = try buildArgPositions(mapping, self.allocator);
            defer self.allocator.free(arg_positions);

            const facts = try ff.fetchFacts(mapping, self.allocator);
            defer {
                for (facts) |f| f.deinit(self.allocator);
                self.allocator.free(facts);
            }

            for (facts) |fact| {
                switch (arity) {
                    1 => {
                        const id = try self.interner.intern(
                            fact.entities[arg_positions[0]].id,
                        );
                        const rel = try self.ensureRelation(mapping.predicate, 1);
                        try rel.unary.insert(id);
                    },
                    2 => {
                        const a = try self.interner.intern(
                            fact.entities[arg_positions[0]].id,
                        );
                        const b = try self.interner.intern(
                            fact.entities[arg_positions[1]].id,
                        );
                        const rel = try self.ensureRelation(mapping.predicate, 2);
                        try rel.binary.insert(a, b);
                    },
                    else => unreachable,
                }
            }
        }
    }

    /// Run semi-naive fixpoint evaluation.
    pub fn evaluate(self: *BitmapEvaluator) !void {
        // Pre-create all head relations to prevent RelationMap resize during execution
        for (self.rules) |rule| {
            if (rule.body.len == 0) continue;
            _ = try self.ensureRelation(rule.head.predicate, rule.head.terms.len);
        }

        // Phase 1: Initial naive pass
        var deltas: RelationMap = .{};
        defer deinitRelations(&deltas, self.allocator);

        for (self.rules) |rule| {
            if (rule.body.len == 0) continue;
            if (rule.body.len == 1) {
                _ = try self.executeSingleAtomRule(rule, &self.relations, &deltas);
            } else if (rule.body.len == 2) {
                _ = try self.executeTwoAtomRule(rule, &self.relations, &self.relations, &deltas);
            }
        }

        // Phase 2: Semi-naive loop
        const max_iterations: usize = 10000;
        var iteration: usize = 0;

        while (iteration < max_iterations) {
            if (!hasNonEmptyRelation(&deltas)) break;
            iteration += 1;

            var prev_deltas = deltas;
            deltas = .{};
            defer deinitRelations(&prev_deltas, self.allocator);

            for (self.rules) |rule| {
                if (rule.body.len == 0) continue;

                if (rule.body.len == 1) {
                    if (prev_deltas.get(rule.body[0].atom.predicate) != null) {
                        _ = try self.executeSingleAtomRule(rule, &prev_deltas, &deltas);
                    }
                } else if (rule.body.len == 2) {
                    if (prev_deltas.get(rule.body[0].atom.predicate) != null) {
                        _ = try self.executeTwoAtomRule(rule, &prev_deltas, &self.relations, &deltas);
                    }
                    if (prev_deltas.get(rule.body[1].atom.predicate) != null) {
                        _ = try self.executeTwoAtomRule(rule, &self.relations, &prev_deltas, &deltas);
                    }
                }
            }
        }
    }

    /// Query a pattern against materialized relations.
    pub fn query(self: *BitmapEvaluator, pattern: Atom) ![]Binding {
        const rel_ptr = self.relations.getPtr(pattern.predicate) orelse
            return try self.allocEmptyResults();

        if (pattern.terms.len == 2) {
            return switch (rel_ptr.*) {
                .binary => |*b| self.queryBinary(pattern, b),
                .unary => self.allocEmptyResults(),
            };
        } else if (pattern.terms.len == 1) {
            return switch (rel_ptr.*) {
                .unary => |*u| self.queryUnary(pattern, u),
                .binary => self.allocEmptyResults(),
            };
        }
        return try self.allocEmptyResults();
    }

    /// Free query results returned by query().
    pub fn freeQueryResults(self: *BitmapEvaluator, results: []Binding) void {
        for (results) |*b| {
            b.deinit();
        }
        self.allocator.free(results);
    }

    // =========================================================================
    // Private helpers
    // =========================================================================

    fn internTerm(self: *BitmapEvaluator, term: Term) !?u32 {
        return switch (term) {
            .constant => |c| try self.interner.intern(c),
            .variable => null,
        };
    }

    fn ensureRelation(self: *BitmapEvaluator, predicate: []const u8, arity: usize) !*Relation {
        return self.ensureRelationIn(&self.relations, predicate, arity);
    }

    fn ensureRelationIn(
        self: *BitmapEvaluator,
        map: *RelationMap,
        predicate: []const u8,
        arity: usize,
    ) !*Relation {
        const gop = try map.getOrPut(self.allocator, predicate);
        if (!gop.found_existing) {
            const owned = try self.allocator.dupe(u8, predicate);
            gop.key_ptr.* = owned;
            gop.value_ptr.* = switch (arity) {
                1 => .{ .unary = try UnaryRelation.init(self.allocator) },
                2 => .{ .binary = try BinaryRelation.init(self.allocator) },
                else => unreachable,
            };
        }
        return gop.value_ptr;
    }

    fn allocEmptyResults(self: *BitmapEvaluator) ![]Binding {
        return try self.allocator.alloc(Binding, 0);
    }

    fn hasNonEmptyRelation(relations: *RelationMap) bool {
        var iter = relations.iterator();
        while (iter.next()) |entry| {
            switch (entry.value_ptr.*) {
                .binary => |b| {
                    if (!b.isEmpty()) return true;
                },
                .unary => |u| {
                    if (!u.isEmpty()) return true;
                },
            }
        }
        return false;
    }

    // =========================================================================
    // Single-atom rule execution
    // =========================================================================

    fn executeSingleAtomRule(
        self: *BitmapEvaluator,
        rule: Rule,
        source_rels: *RelationMap,
        deltas: *RelationMap,
    ) !bool {
        const body_atom = rule.body[0].atom;
        const head = rule.head;
        const head_arity = head.terms.len;

        const source_ptr = source_rels.getPtr(body_atom.predicate) orelse return false;

        // Resolve constants in body
        var body_bound: [2]?u32 = .{ null, null };
        for (body_atom.terms, 0..) |term, i| {
            if (i >= 2) break;
            switch (term) {
                .constant => |c| {
                    body_bound[i] = self.interner.lookup(c) orelse return false;
                },
                .variable => {},
            }
        }

        // Build head variable → body position mapping
        var head_from_body: [2]?u8 = .{ null, null };
        for (head.terms, 0..) |ht, hi| {
            if (hi >= 2) break;
            switch (ht) {
                .variable => |hv| {
                    for (body_atom.terms, 0..) |bt, bi| {
                        switch (bt) {
                            .variable => |bv| {
                                if (std.mem.eql(u8, hv, bv)) {
                                    head_from_body[hi] = @intCast(bi);
                                    break;
                                }
                            },
                            .constant => {},
                        }
                    }
                },
                .constant => {},
            }
        }

        // Same variable in both body positions (e.g., edge(X, X))
        const same_var = if (body_atom.terms.len == 2)
            switch (body_atom.terms[0]) {
                .variable => |v0| switch (body_atom.terms[1]) {
                    .variable => |v1| std.mem.eql(u8, v0, v1),
                    .constant => false,
                },
                .constant => false,
            }
        else
            false;

        // Buffer results to avoid pointer invalidation during insertion
        var new_tuples = std.ArrayListUnmanaged([2]u32){};
        defer new_tuples.deinit(self.allocator);

        switch (source_ptr.*) {
            .binary => |*source| {
                if (body_bound[0] != null and body_bound[1] != null) {
                    if (source.contains(body_bound[0].?, body_bound[1].?)) {
                        const hv = self.computeHeadVals(head, head_from_body, .{ body_bound[0].?, body_bound[1].? }) catch return false;
                        if (!self.targetContains(head.predicate, head_arity, hv)) {
                            try new_tuples.append(self.allocator, hv);
                        }
                    }
                } else if (body_bound[0]) |c0| {
                    const fwd = source.getForward(c0) orelse return false;
                    var iter = fwd.iterator();
                    while (iter.next()) |b_val| {
                        if (same_var and c0 != b_val) continue;
                        const hv = self.computeHeadVals(head, head_from_body, .{ c0, b_val }) catch continue;
                        if (!self.targetContains(head.predicate, head_arity, hv)) {
                            try new_tuples.append(self.allocator, hv);
                        }
                    }
                } else if (body_bound[1]) |c1| {
                    const rev = source.getReverse(c1) orelse return false;
                    var iter = rev.iterator();
                    while (iter.next()) |a_val| {
                        if (same_var and a_val != c1) continue;
                        const hv = self.computeHeadVals(head, head_from_body, .{ a_val, c1 }) catch continue;
                        if (!self.targetContains(head.predicate, head_arity, hv)) {
                            try new_tuples.append(self.allocator, hv);
                        }
                    }
                } else {
                    var domain_iter = source.domain.iterator();
                    while (domain_iter.next()) |a_val| {
                        const fwd = source.getForward(a_val) orelse continue;
                        var b_iter = fwd.iterator();
                        while (b_iter.next()) |b_val| {
                            if (same_var and a_val != b_val) continue;
                            const hv = self.computeHeadVals(head, head_from_body, .{ a_val, b_val }) catch continue;
                            if (!self.targetContains(head.predicate, head_arity, hv)) {
                                try new_tuples.append(self.allocator, hv);
                            }
                        }
                    }
                }
            },
            .unary => |*source| {
                switch (body_atom.terms[0]) {
                    .constant => return false,
                    .variable => {
                        var iter = source.members.iterator();
                        while (iter.next()) |val| {
                            const hv = self.computeHeadVals(head, head_from_body, .{ val, 0 }) catch continue;
                            if (!self.targetContains(head.predicate, head_arity, hv)) {
                                try new_tuples.append(self.allocator, hv);
                            }
                        }
                    },
                }
            },
        }

        var changed = false;
        for (new_tuples.items) |hv| {
            if (try self.insertDerived(head.predicate, head_arity, hv, deltas)) {
                changed = true;
            }
        }
        return changed;
    }

    // =========================================================================
    // Two-atom join rule execution
    // =========================================================================

    fn executeTwoAtomRule(
        self: *BitmapEvaluator,
        rule: Rule,
        left_source: *RelationMap,
        right_source: *RelationMap,
        deltas: *RelationMap,
    ) !bool {
        const analysis = analyzeJoin(rule) catch return false;

        const left_ptr = left_source.getPtr(rule.body[0].atom.predicate) orelse return false;
        const right_ptr = right_source.getPtr(rule.body[1].atom.predicate) orelse return false;

        var left = switch (left_ptr.*) {
            .binary => |*b| b,
            .unary => return false,
        };
        var right = switch (right_ptr.*) {
            .binary => |*b| b,
            .unary => return false,
        };

        // Resolve constants in body atoms
        const left_atom = rule.body[0].atom;
        const right_atom = rule.body[1].atom;
        var left_bound: [2]?u32 = .{ null, null };
        var right_bound: [2]?u32 = .{ null, null };
        for (left_atom.terms, 0..) |term, i| {
            if (i >= 2) break;
            switch (term) {
                .constant => |c| {
                    left_bound[i] = self.interner.lookup(c) orelse return false;
                },
                .variable => {},
            }
        }
        for (right_atom.terms, 0..) |term, i| {
            if (i >= 2) break;
            switch (term) {
                .constant => |c| {
                    right_bound[i] = self.interner.lookup(c) orelse return false;
                },
                .variable => {},
            }
        }

        // Get join candidate bitmaps
        const left_join_bm: *RoaringBitmap = switch (analysis.left_pos) {
            0 => &left.domain,
            1 => &left.range,
            else => unreachable,
        };
        const right_join_bm: *RoaringBitmap = switch (analysis.right_pos) {
            0 => &right.domain,
            1 => &right.range,
            else => unreachable,
        };

        // Compute candidates, handling constants at join position
        const left_join_const = left_bound[analysis.left_pos];
        const right_join_const = right_bound[analysis.right_pos];

        var candidates = if (left_join_const != null and right_join_const != null) blk: {
            if (left_join_const.? != right_join_const.?) return false;
            var bm = try RoaringBitmap.init(self.allocator);
            _ = try bm.add(left_join_const.?);
            break :blk bm;
        } else if (left_join_const) |lc| blk: {
            if (!right_join_bm.contains(lc)) return false;
            var bm = try RoaringBitmap.init(self.allocator);
            _ = try bm.add(lc);
            break :blk bm;
        } else if (right_join_const) |rc| blk: {
            if (!left_join_bm.contains(rc)) return false;
            var bm = try RoaringBitmap.init(self.allocator);
            _ = try bm.add(rc);
            break :blk bm;
        } else blk: {
            break :blk try left_join_bm.bitwiseAnd(self.allocator, right_join_bm);
        };
        defer candidates.deinit();

        // Non-join position constants for filtering
        const left_non_join_pos: u8 = 1 - analysis.left_pos;
        const right_non_join_pos: u8 = 1 - analysis.right_pos;
        const left_non_join_bound = left_bound[left_non_join_pos];
        const right_non_join_bound = right_bound[right_non_join_pos];

        const head_arity = rule.head.terms.len;

        // Buffer results to avoid pointer invalidation
        var new_tuples = std.ArrayListUnmanaged([2]u32){};
        defer new_tuples.deinit(self.allocator);

        var cand_iter = candidates.iterator();
        while (cand_iter.next()) |join_val| {
            const left_others = getOtherSide(left, analysis.left_pos, join_val) orelse continue;
            const right_others = getOtherSide(right, analysis.right_pos, join_val) orelse continue;

            var left_iter = left_others.iterator();
            while (left_iter.next()) |left_val| {
                if (left_non_join_bound) |lb| {
                    if (left_val != lb) continue;
                }

                var right_iter = right_others.iterator();
                while (right_iter.next()) |right_val| {
                    if (right_non_join_bound) |rb| {
                        if (right_val != rb) continue;
                    }

                    var head_vals: [2]u32 = .{ 0, 0 };
                    for (0..head_arity) |hi| {
                        head_vals[hi] = resolveHeadValue(
                            analysis.head_map[hi],
                            join_val,
                            left_val,
                            right_val,
                            analysis,
                        );
                    }

                    if (!self.targetContains(rule.head.predicate, head_arity, head_vals)) {
                        try new_tuples.append(self.allocator, head_vals);
                    }
                }
            }
        }

        var changed = false;
        for (new_tuples.items) |hv| {
            if (try self.insertDerived(rule.head.predicate, head_arity, hv, deltas)) {
                changed = true;
            }
        }
        return changed;
    }

    // =========================================================================
    // Shared helpers
    // =========================================================================

    fn computeHeadVals(
        self: *BitmapEvaluator,
        head: Atom,
        head_from_body: [2]?u8,
        body_vals: [2]u32,
    ) ![2]u32 {
        var result: [2]u32 = .{ 0, 0 };
        for (head.terms, 0..) |ht, hi| {
            if (hi >= 2) break;
            result[hi] = switch (ht) {
                .variable => body_vals[head_from_body[hi] orelse return error.HeadVariableNotInBody],
                .constant => |c| try self.interner.intern(c),
            };
        }
        return result;
    }

    fn targetContains(self: *BitmapEvaluator, predicate: []const u8, arity: usize, vals: [2]u32) bool {
        const target = self.relations.getPtr(predicate) orelse return false;
        return switch (arity) {
            1 => target.unary.contains(vals[0]),
            2 => target.binary.contains(vals[0], vals[1]),
            else => false,
        };
    }

    fn insertDerived(
        self: *BitmapEvaluator,
        predicate: []const u8,
        arity: usize,
        vals: [2]u32,
        deltas: *RelationMap,
    ) !bool {
        const target = try self.ensureRelation(predicate, arity);
        switch (arity) {
            1 => {
                if (!target.unary.contains(vals[0])) {
                    try target.unary.insert(vals[0]);
                    const delta = try self.ensureRelationIn(deltas, predicate, 1);
                    try delta.unary.insert(vals[0]);
                    return true;
                }
            },
            2 => {
                if (!target.binary.contains(vals[0], vals[1])) {
                    try target.binary.insert(vals[0], vals[1]);
                    const delta = try self.ensureRelationIn(deltas, predicate, 2);
                    try delta.binary.insert(vals[0], vals[1]);
                    return true;
                }
            },
            else => {},
        }
        return false;
    }

    fn analyzeJoin(rule: Rule) !JoinAnalysis {
        if (rule.body.len != 2) return error.UnsupportedRule;

        const left = rule.body[0].atom;
        const right = rule.body[1].atom;

        var join_var: ?[]const u8 = null;
        var left_pos: u8 = 0;
        var right_pos: u8 = 0;

        for (left.terms, 0..) |lt, li| {
            switch (lt) {
                .variable => |lv| {
                    for (right.terms, 0..) |rt, ri| {
                        switch (rt) {
                            .variable => |rv| {
                                if (std.mem.eql(u8, lv, rv)) {
                                    join_var = lv;
                                    left_pos = @intCast(li);
                                    right_pos = @intCast(ri);
                                }
                            },
                            .constant => {},
                        }
                        if (join_var != null) break;
                    }
                },
                .constant => {},
            }
            if (join_var != null) break;
        }

        const jv = join_var orelse return error.NoJoinVariable;

        var head_map: [2]VarRef = .{
            .{ .atom_idx = 0, .term_pos = 0 },
            .{ .atom_idx = 0, .term_pos = 0 },
        };

        for (rule.head.terms, 0..) |ht, hi| {
            if (hi >= 2) break;
            switch (ht) {
                .variable => |hv| {
                    var found = false;
                    for (left.terms, 0..) |lt, li| {
                        switch (lt) {
                            .variable => |lv| {
                                if (std.mem.eql(u8, hv, lv)) {
                                    head_map[hi] = .{ .atom_idx = 0, .term_pos = @intCast(li) };
                                    found = true;
                                    break;
                                }
                            },
                            .constant => {},
                        }
                    }
                    if (!found) {
                        for (right.terms, 0..) |rt, ri| {
                            switch (rt) {
                                .variable => |rv| {
                                    if (std.mem.eql(u8, hv, rv)) {
                                        head_map[hi] = .{ .atom_idx = 1, .term_pos = @intCast(ri) };
                                        found = true;
                                        break;
                                    }
                                },
                                .constant => {},
                            }
                        }
                    }
                    if (!found) return error.HeadVariableNotInBody;
                },
                .constant => {},
            }
        }

        return .{
            .join_var = jv,
            .left_pos = left_pos,
            .right_pos = right_pos,
            .head_map = head_map,
        };
    }

    fn getOtherSide(rel: *BinaryRelation, join_pos: u8, join_val: u32) ?*RoaringBitmap {
        return switch (join_pos) {
            0 => rel.getForward(join_val),
            1 => rel.getReverse(join_val),
            else => null,
        };
    }

    fn resolveHeadValue(
        head_ref: VarRef,
        join_val: u32,
        left_other: u32,
        right_other: u32,
        analysis: JoinAnalysis,
    ) u32 {
        if (head_ref.atom_idx == 0) {
            if (head_ref.term_pos == analysis.left_pos) return join_val;
            return left_other;
        } else {
            if (head_ref.term_pos == analysis.right_pos) return join_val;
            return right_other;
        }
    }

    // =========================================================================
    // Query resolution
    // =========================================================================

    fn queryBinary(self: *BitmapEvaluator, pattern: Atom, rel: *BinaryRelation) ![]Binding {
        var results = std.ArrayListUnmanaged(Binding){};
        errdefer {
            for (results.items) |*b| b.deinit();
            results.deinit(self.allocator);
        }

        const t0 = pattern.terms[0];
        const t1 = pattern.terms[1];

        const c0: ?u32 = switch (t0) {
            .constant => |c| self.interner.lookup(c),
            .variable => null,
        };
        const c1: ?u32 = switch (t1) {
            .constant => |c| self.interner.lookup(c),
            .variable => null,
        };

        // Constant not in interner means no results
        switch (t0) {
            .constant => if (c0 == null) return try self.allocEmptyResults(),
            .variable => {},
        }
        switch (t1) {
            .constant => if (c1 == null) return try self.allocEmptyResults(),
            .variable => {},
        }

        if (c0 != null and c1 != null) {
            if (rel.contains(c0.?, c1.?)) {
                const b = Binding.init(self.allocator);
                try results.append(self.allocator, b);
            }
        } else if (c0 != null) {
            const fwd = rel.getForward(c0.?) orelse return try self.allocEmptyResults();
            var iter = fwd.iterator();
            while (iter.next()) |val| {
                var b = Binding.init(self.allocator);
                try b.put(t1.variable, self.interner.resolve(val));
                try results.append(self.allocator, b);
            }
        } else if (c1 != null) {
            const rev = rel.getReverse(c1.?) orelse return try self.allocEmptyResults();
            var iter = rev.iterator();
            while (iter.next()) |val| {
                var b = Binding.init(self.allocator);
                try b.put(t0.variable, self.interner.resolve(val));
                try results.append(self.allocator, b);
            }
        } else {
            // Both variables
            var domain_iter = rel.domain.iterator();
            while (domain_iter.next()) |a| {
                const fwd = rel.getForward(a) orelse continue;
                const a_str = self.interner.resolve(a);
                var b_iter = fwd.iterator();
                while (b_iter.next()) |b| {
                    var binding = Binding.init(self.allocator);
                    try binding.put(t0.variable, a_str);
                    try binding.put(t1.variable, self.interner.resolve(b));
                    try results.append(self.allocator, binding);
                }
            }
        }

        return results.toOwnedSlice(self.allocator);
    }

    fn queryUnary(self: *BitmapEvaluator, pattern: Atom, rel: *UnaryRelation) ![]Binding {
        var results = std.ArrayListUnmanaged(Binding){};
        errdefer {
            for (results.items) |*b| b.deinit();
            results.deinit(self.allocator);
        }

        const t0 = pattern.terms[0];

        switch (t0) {
            .constant => |c| {
                const cid = self.interner.lookup(c) orelse return try self.allocEmptyResults();
                if (rel.contains(cid)) {
                    const b = Binding.init(self.allocator);
                    try results.append(self.allocator, b);
                }
            },
            .variable => |v| {
                var iter = rel.members.iterator();
                while (iter.next()) |val| {
                    var b = Binding.init(self.allocator);
                    try b.put(v, self.interner.resolve(val));
                    try results.append(self.allocator, b);
                }
            },
        }

        return results.toOwnedSlice(self.allocator);
    }
};

// =============================================================================
// Tests
// =============================================================================

test "bitmap eval: simple query" {
    const allocator = std.testing.allocator;

    var parser = datalog.Parser.init(allocator,
        \\parent("tom", "bob").
        \\parent("bob", "jim").
    );
    defer parser.deinit();
    const parsed = try parser.parseProgram();

    var eval = BitmapEvaluator.init(allocator, parsed.rules);
    defer eval.deinit();
    try eval.addGroundFacts(parsed.rules);

    var q_terms = [_]Term{ .{ .variable = "X" }, .{ .constant = "bob" } };
    const results = try eval.query(.{ .predicate = "parent", .terms = &q_terms });
    defer eval.freeQueryResults(results);

    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqualStrings("tom", results[0].get("X").?);
}

test "bitmap eval: transitive closure" {
    const allocator = std.testing.allocator;

    var parser = datalog.Parser.init(allocator,
        \\edge("a", "b").
        \\edge("b", "c").
        \\edge("c", "d").
        \\reachable(X, Y) :- edge(X, Y).
        \\reachable(X, Z) :- edge(X, Y), reachable(Y, Z).
    );
    defer parser.deinit();
    const parsed = try parser.parseProgram();

    var eval = BitmapEvaluator.init(allocator, parsed.rules);
    defer eval.deinit();
    try eval.addGroundFacts(parsed.rules);
    try eval.evaluate();

    var q_terms = [_]Term{ .{ .constant = "a" }, .{ .variable = "X" } };
    const results = try eval.query(.{ .predicate = "reachable", .terms = &q_terms });
    defer eval.freeQueryResults(results);

    try std.testing.expectEqual(@as(usize, 3), results.len);
}

test "bitmap eval: member_of transitive" {
    const allocator = std.testing.allocator;

    var parser = datalog.Parser.init(allocator,
        \\member_of("alice", "developers").
        \\member_of("developers", "employees").
        \\member_of("employees", "domain_users").
        \\member_of_t(X, G) :- member_of(X, G).
        \\member_of_t(X, G) :- member_of(X, M), member_of_t(M, G).
    );
    defer parser.deinit();
    const parsed = try parser.parseProgram();

    var eval = BitmapEvaluator.init(allocator, parsed.rules);
    defer eval.deinit();
    try eval.addGroundFacts(parsed.rules);
    try eval.evaluate();

    var q_terms = [_]Term{ .{ .constant = "alice" }, .{ .variable = "G" } };
    const results = try eval.query(.{ .predicate = "member_of_t", .terms = &q_terms });
    defer eval.freeQueryResults(results);

    try std.testing.expectEqual(@as(usize, 3), results.len);
}

test "bitmap eval: books and literary influence" {
    const allocator = std.testing.allocator;

    var parser = datalog.Parser.init(allocator,
        \\wrote("Homer", "The Odyssey").
        \\wrote("Homer", "The Iliad").
        \\wrote("Virgil", "The Aeneid").
        \\wrote("Dante", "Divine Comedy").
        \\wrote("Milton", "Paradise Lost").
        \\wrote("Plato", "The Republic").
        \\genre("The Odyssey", "epic").
        \\genre("The Iliad", "epic").
        \\genre("The Aeneid", "epic").
        \\genre("Divine Comedy", "epic").
        \\genre("Paradise Lost", "epic").
        \\genre("The Republic", "philosophy").
        \\influenced("Homer", "Virgil").
        \\influenced("Virgil", "Dante").
        \\influenced("Virgil", "Milton").
        \\influenced("Dante", "Milton").
        \\influenced_t(A, B) :- influenced(A, B).
        \\influenced_t(A, C) :- influenced(A, B), influenced_t(B, C).
        \\books_in_tradition(Book, Root) :-
        \\    influenced_t(Root, Author), wrote(Author, Book).
    );
    defer parser.deinit();
    const parsed = try parser.parseProgram();

    var eval = BitmapEvaluator.init(allocator, parsed.rules);
    defer eval.deinit();
    try eval.addGroundFacts(parsed.rules);
    try eval.evaluate();

    // Homer influenced Virgil, Dante, Milton (transitively)
    var q1_terms = [_]Term{ .{ .constant = "Homer" }, .{ .variable = "Who" } };
    const q1 = try eval.query(.{ .predicate = "influenced_t", .terms = &q1_terms });
    defer eval.freeQueryResults(q1);
    try std.testing.expectEqual(@as(usize, 3), q1.len);

    // Books in Homer's tradition: Aeneid, Divine Comedy, Paradise Lost
    var q2_terms = [_]Term{ .{ .variable = "Book" }, .{ .constant = "Homer" } };
    const q2 = try eval.query(.{ .predicate = "books_in_tradition", .terms = &q2_terms });
    defer eval.freeQueryResults(q2);
    try std.testing.expectEqual(@as(usize, 3), q2.len);

    // Who influenced Milton: Homer, Virgil, Dante
    var q3_terms = [_]Term{ .{ .variable = "Who" }, .{ .constant = "Milton" } };
    const q3 = try eval.query(.{ .predicate = "influenced_t", .terms = &q3_terms });
    defer eval.freeQueryResults(q3);
    try std.testing.expectEqual(@as(usize, 3), q3.len);
}

test "bitmap eval: ground facts only, no evaluation needed" {
    const allocator = std.testing.allocator;

    var parser = datalog.Parser.init(allocator,
        \\color("red").
        \\color("blue").
        \\color("green").
    );
    defer parser.deinit();
    const parsed = try parser.parseProgram();

    var eval = BitmapEvaluator.init(allocator, parsed.rules);
    defer eval.deinit();
    try eval.addGroundFacts(parsed.rules);

    var q_terms = [_]Term{.{ .variable = "C" }};
    const results = try eval.query(.{ .predicate = "color", .terms = &q_terms });
    defer eval.freeQueryResults(results);

    try std.testing.expectEqual(@as(usize, 3), results.len);
}

test "bitmap eval: query with no results" {
    const allocator = std.testing.allocator;

    var parser = datalog.Parser.init(allocator,
        \\edge("a", "b").
    );
    defer parser.deinit();
    const parsed = try parser.parseProgram();

    var eval = BitmapEvaluator.init(allocator, parsed.rules);
    defer eval.deinit();
    try eval.addGroundFacts(parsed.rules);

    var q_terms = [_]Term{ .{ .constant = "z" }, .{ .variable = "X" } };
    const results = try eval.query(.{ .predicate = "edge", .terms = &q_terms });
    defer eval.freeQueryResults(results);
    try std.testing.expectEqual(@as(usize, 0), results.len);

    var q2_terms = [_]Term{.{ .variable = "X" }};
    const results2 = try eval.query(.{ .predicate = "nonexistent", .terms = &q2_terms });
    defer eval.freeQueryResults(results2);
    try std.testing.expectEqual(@as(usize, 0), results2.len);
}

test "bitmap eval: loadMappedFacts from mock fetcher" {
    const allocator = std.testing.allocator;

    // MockFetcher inline for this test
    const MockFetcher = struct {
        facts_by_predicate: std.StringHashMapUnmanaged([]const Fact),
        alloc: std.mem.Allocator,

        fn init(a: std.mem.Allocator) @This() {
            return .{ .facts_by_predicate = .{}, .alloc = a };
        }
        fn deinit(self: *@This()) void {
            self.facts_by_predicate.deinit(self.alloc);
        }
        fn addFacts(self: *@This(), predicate: []const u8, facts: []const Fact) !void {
            try self.facts_by_predicate.put(self.alloc, predicate, facts);
        }
        fn fetcher(self: *@This()) FactFetcher {
            return .{ .ptr = self, .vtable = &.{ .fetchFacts = fetchImpl } };
        }
        fn fetchImpl(ptr: *anyopaque, mapping: Mapping, a: std.mem.Allocator) anyerror![]const Fact {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const stored = self.facts_by_predicate.get(mapping.predicate) orelse
                return &[_]Fact{};
            const result = try a.alloc(Fact, stored.len);
            for (stored, 0..) |fact, i| {
                result[i] = try fact.clone(a);
            }
            return result;
        }
    };

    // Set up mock: wrote(Author, Book) = [rel:wrote, author:$A, book:$B]
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

    // No rules needed -- just loading mapped facts
    var eval = BitmapEvaluator.init(allocator, &[_]Rule{});
    defer eval.deinit();
    try eval.loadMappedFacts(mock.fetcher(), &mappings);

    // Query: wrote("Homer", Book) should return Iliad
    // Key: bare "Homer" from entity.id matches bare "Homer" in query constant
    var q_terms = [_]Term{ .{ .constant = "Homer" }, .{ .variable = "Book" } };
    const results = try eval.query(.{ .predicate = "wrote", .terms = &q_terms });
    defer eval.freeQueryResults(results);

    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqualStrings("Iliad", results[0].get("Book").?);
}

test "bitmap eval: fixpoint converges" {
    const allocator = std.testing.allocator;

    var parser = datalog.Parser.init(allocator,
        \\edge("a", "b").
        \\edge("b", "c").
        \\edge("c", "a").
        \\reach(X, Y) :- edge(X, Y).
        \\reach(X, Z) :- edge(X, Y), reach(Y, Z).
    );
    defer parser.deinit();
    const parsed = try parser.parseProgram();

    var eval = BitmapEvaluator.init(allocator, parsed.rules);
    defer eval.deinit();
    try eval.addGroundFacts(parsed.rules);
    try eval.evaluate();

    var q_terms = [_]Term{ .{ .variable = "X" }, .{ .variable = "Y" } };
    const results = try eval.query(.{ .predicate = "reach", .terms = &q_terms });
    defer eval.freeQueryResults(results);

    try std.testing.expectEqual(@as(usize, 9), results.len);
}
