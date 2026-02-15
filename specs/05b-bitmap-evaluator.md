# Step 5b: Bitmap Evaluator

## Context

Steps 1–4 built the ingest pipeline: StringInterner, Relations, FactFetcher, bitmap ingest. Step 5a moved shared infrastructure to `relation.zig`. This step builds the evaluator that runs Datalog rules over bitmap relations.

The old evaluator (`datalog.zig` Evaluator) is tuple-at-a-time: every fact is a heap-allocated string Atom, joins are nested loops over Binding hashmaps, dedup hashes entire atoms. The new evaluator is set-at-a-time: entities are u32 IDs, relations are bitmaps, joins use bitmap intersection for candidate selection, dedup is inherent.

**Scope:** This step handles pure `.dl` ground facts and rules only (no @map, no LMDB ingest). Ground facts are interned as bare strings. The evaluator must produce identical results to the old evaluator on all existing test cases. CLI wiring and LMDB integration are step 6.

Depends on: Steps 01 (StringInterner), 02 (Relation), 05a (RelationMap in relation.zig).

## API

Create `src/bitmap_evaluator.zig`:

```zig
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

pub const BitmapEvaluator = struct {
    interner: StringInterner,
    relations: RelationMap,
    rules: []const Rule,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, rules: []const Rule) BitmapEvaluator;
    pub fn deinit(self: *BitmapEvaluator) void;

    /// Insert ground facts (rules with empty body) into relations.
    pub fn addGroundFacts(self: *BitmapEvaluator, rules: []const Rule) !void;

    /// Run semi-naive fixpoint evaluation.
    pub fn evaluate(self: *BitmapEvaluator) !void;

    /// Query a pattern against materialized relations.
    /// Returns Binding[] for compatibility with old evaluator tests.
    pub fn query(self: *BitmapEvaluator, pattern: Atom) ![]Binding;
};
```

## Initialization

```zig
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
```

The evaluator owns its interner and relations. For step 6 (CLI wiring), an alternate constructor will accept pre-loaded interner and relations from LMDB. For now, everything starts empty.

## Ground Fact Insertion

Ground facts are rules with empty body: `edge("a", "b").`

```zig
pub fn addGroundFacts(self: *BitmapEvaluator, rules: []const Rule) !void {
    for (rules) |rule| {
        if (rule.body.len != 0) continue; // skip rules with body

        const arity = rule.head.terms.len;
        if (arity == 0 or arity > 2) continue; // skip unsupported arities

        // Intern all constant terms
        // (ground facts should only have constants — skip if any variable)
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

/// Intern a term's constant value. Returns null for variables.
fn internTerm(self: *BitmapEvaluator, term: Term) !?u32 {
    return switch (term) {
        .constant => |c| try self.interner.intern(c),
        .variable => null,
    };
}

/// Get or create a relation for a predicate with the given arity.
fn ensureRelation(
    self: *BitmapEvaluator,
    predicate: []const u8,
    arity: usize,
) !*Relation {
    const gop = try self.relations.getOrPut(self.allocator, predicate);
    if (!gop.found_existing) {
        // Dupe predicate key for ownership
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
```

**Key detail:** `ensureRelation` dupes the predicate string into the evaluator's allocator for RelationMap key ownership. Same pattern as `bitmap_ingest.zig`. On getOrPut, if the key already existed, `gop.found_existing` is true and we skip the dupe.

**Subtlety:** The `getOrPut` call with `predicate` (which points into parser memory) will store that pointer as the key if the entry is new. We need to replace it with the duped copy. The code above does `gop.key_ptr.* = owned` which overwrites the key. This is correct — the hashmap already computed the hash, and the new key has the same content, so the hash is still valid.

## Rule Analysis

Before executing a rule, analyze its variable structure to determine how to execute the join.

### Variable positions

```zig
const VarInfo = struct {
    /// For each variable name, which (body_atom_index, term_position) pairs reference it
    var_positions: std.StringHashMapUnmanaged(std.ArrayListUnmanaged(VarRef)),
};

const VarRef = struct {
    atom_idx: u8,
    term_pos: u8,
};
```

For `head(X, Z) :- r(X, Y), s(Y, Z)`:
- X: body[0][0], head[0]
- Y: body[0][1], body[1][0]  ← join variable
- Z: body[1][1], head[1]

### JoinAnalysis for 2-atom rules

```zig
const JoinAnalysis = struct {
    /// The join variable name
    join_var: []const u8,
    /// Position of join var in body[0] (0 or 1)
    left_pos: u8,
    /// Position of join var in body[1] (0 or 1)
    right_pos: u8,
    /// For each head term: which body atom and position provides it
    /// head_map[i] = .{ .atom_idx, .term_pos }
    head_map: [2]VarRef,
};

/// Analyze a 2-body-atom rule to extract join information.
/// Returns error if rule has no shared variable or unsupported pattern.
fn analyzeJoin(rule: Rule) !JoinAnalysis {
    // Find variables that appear in both body atoms
    // For each variable in body[0], check if it also appears in body[1]
    // Exactly one shared variable expected (the join variable)

    // Build var → positions map for body[0]
    // Build var → positions map for body[1]
    // Find intersection → join variable(s)
    // Map head variables back to body positions
    ...
}
```

The full implementation iterates terms, builds position lists, finds the shared variable. Straightforward string matching on variable names.

### Handling constants in body atoms

A body atom like `wrote("Homer", B)` has a constant in position 0. This constrains the lookup:
- Instead of scanning the full relation, look up `forward[homer_id]` directly.
- The "constant position" and "interned ID" are passed to the join executor.

```zig
const AtomBinding = struct {
    /// For each term position: either a bound constant ID or null (variable)
    bound: [2]?u32,
};

fn resolveBodyAtomConstants(
    self: *BitmapEvaluator,
    atom: Atom,
) [2]?u32 {
    var bound: [2]?u32 = .{ null, null };
    for (atom.terms, 0..) |term, i| {
        if (i >= 2) break;
        switch (term) {
            .constant => |c| bound[i] = self.interner.lookup(c),
            .variable => {},
        }
    }
    return bound;
}
```

If a constant isn't in the interner, the atom can't match anything — short-circuit with empty result.

## Rule Execution

### Copy rules (1 body atom)

`head(X, Y) :- body(X, Y).` — or with rearrangement.

```zig
fn executeCopyRule(
    self: *BitmapEvaluator,
    rule: Rule,
    source: *BinaryRelation,  // or use delta for semi-naive
    target: *BinaryRelation,
    delta: *BinaryRelation,
) !bool {
    // Determine variable mapping: which body positions map to which head positions
    // For head(A, B) :- body(A, B) → direct copy
    // For head(B, A) :- body(A, B) → swap columns

    // Iterate source tuples, insert new ones into target and delta
    var changed = false;
    var domain_iter = source.domain.iterator();
    while (domain_iter.next()) |a| {
        const bs = source.getForward(a) orelse continue;
        var b_iter = bs.iterator();
        while (b_iter.next()) |b| {
            // Apply head variable mapping to determine (head_col0, head_col1)
            const h0 = ...; // mapped from body
            const h1 = ...; // mapped from body
            if (!target.contains(h0, h1)) {
                try target.insert(h0, h1);
                try delta.insert(h0, h1);
                changed = true;
            }
        }
    }
    return changed;
}
```

For copy rules with a constant in the body (e.g., `homer_books(B) :- wrote("Homer", B)`), the source iteration is constrained:
- Look up `source.forward[homer_id]` → iterate only those B values.
- The head is unary: `homer_books(B)` — insert into unary relation.

### Join rules (2 body atoms)

This is the core algorithm. For `head(X, Z) :- r(X, Y), s(Y, Z)`:

```zig
fn executeJoinRule(
    self: *BitmapEvaluator,
    analysis: JoinAnalysis,
    left: *BinaryRelation,   // body[0] relation (or delta)
    right: *BinaryRelation,  // body[1] relation (or delta)
    left_bound: [2]?u32,     // constants in body[0]
    right_bound: [2]?u32,    // constants in body[1]
    target: *BinaryRelation, // head relation
    delta: *BinaryRelation,  // delta for head
) !bool {
    var changed = false;

    // Step 1: Compute candidate join values
    // Join var at left_pos in left, right_pos in right
    // left_pos=1 means join var is in range → use left.range
    // left_pos=0 means join var is in domain → use left.domain
    // Same logic for right side

    // Get the bitmap of join var values from each side
    const left_join_bitmap = switch (analysis.left_pos) {
        0 => &left.domain,
        1 => &left.range,
        else => unreachable,
    };
    const right_join_bitmap = switch (analysis.right_pos) {
        0 => &right.domain,
        1 => &right.range,
        else => unreachable,
    };

    // Candidates = values present in both sides
    var candidates = try left_join_bitmap.bitwiseAnd(self.allocator, right_join_bitmap);
    defer candidates.deinit();

    // Step 2: For each candidate join value, compute result tuples
    var cand_iter = candidates.iterator();
    while (cand_iter.next()) |join_val| {
        // Get the "other side" values from each relation
        // If join var is at pos 0 in left, the other values are in left.forward[join_val]
        // If join var is at pos 1 in left, the other values are in left.reverse[join_val]
        const left_others = self.getOtherSide(left, analysis.left_pos, join_val) orelse continue;
        const right_others = self.getOtherSide(right, analysis.right_pos, join_val) orelse continue;

        // Apply constant filters if any
        // If left_bound has a constant on the non-join position, filter left_others
        // If right_bound has a constant on the non-join position, filter right_others

        // Emit tuples: each combination of (left_other, right_other) mapped to head positions
        var left_iter = left_others.iterator();
        while (left_iter.next()) |left_val| {
            var right_iter = right_others.iterator();
            while (right_iter.next()) |right_val| {
                // Map to head positions using analysis.head_map
                const h0 = self.resolveHeadValue(analysis.head_map[0], join_val, left_val, right_val);
                const h1 = self.resolveHeadValue(analysis.head_map[1], join_val, left_val, right_val);

                if (!target.contains(h0, h1)) {
                    try target.insert(h0, h1);
                    try delta.insert(h0, h1);
                    changed = true;
                }
            }
        }
    }
    return changed;
}

/// Given a relation and the position of the join variable, return the bitmap
/// of values on the OTHER side for a specific join value.
fn getOtherSide(
    self: *BitmapEvaluator,
    rel: *BinaryRelation,
    join_pos: u8,
    join_val: u32,
) ?*RoaringBitmap {
    return switch (join_pos) {
        0 => rel.getForward(join_val),   // join var at pos 0 → others are forward[val]
        1 => rel.getReverse(join_val),   // join var at pos 1 → others are reverse[val]
        else => null,
    };
}

/// Resolve a head position value. The head_map entry tells us which body atom
/// and position provides this value. We have three known values:
/// the join_val, the left_other, and the right_other.
fn resolveHeadValue(
    head_ref: VarRef,
    join_val: u32,
    left_other: u32,
    right_other: u32,
    analysis: JoinAnalysis,
) u32 {
    // head_ref.atom_idx tells us body[0] or body[1]
    // head_ref.term_pos tells us position in that body atom
    // If that position IS the join position → return join_val
    // If that position is the OTHER position → return left_other or right_other
    if (head_ref.atom_idx == 0) {
        if (head_ref.term_pos == analysis.left_pos) return join_val;
        return left_other;
    } else {
        if (head_ref.term_pos == analysis.right_pos) return join_val;
        return right_other;
    }
}
```

### Self-join

`reachable(X, Z) :- edge(X, Y), reachable(Y, Z).` where `reachable` appears in both head and body[1] — this is handled naturally. The `right` relation is the current state of `reachable` (or its delta). Same algorithm, same code path.

### Handling constants in body atoms (during join)

If body[0] is `wrote("Homer", Y)`, then `left_bound[0] = Some(homer_id)`. Instead of iterating all of left's domain, constrain to just homer_id:

```zig
// Before computing candidates, if left has a constant on the join position:
// candidates = just that one value (if it exists in right's join bitmap)
// If left has a constant on the non-join position:
// left_others is constrained to just that constant's bitmap contribution

// Simplest approach: if a body atom has a constant at the non-join position,
// check left_others.contains(constant_id) before emitting.
// If constant at join position, candidates = {constant_id} ∩ right_join_bitmap.
```

For step 5 MVP, handle constants by pre-filtering:
- Constant at join position: intersect with single-element bitmap (or just check `right_join_bitmap.contains(const_id)`)
- Constant at non-join position: filter the `left_others` / `right_others` iterator to only yield that constant

## Semi-Naive Fixpoint

The fixpoint loop inserts new facts into both `self.relations` and the delta simultaneously. No separate merge step.

```
1. addGroundFacts(rules)
2. Initial pass: for each rule, execute against full relations
   → new facts inserted into BOTH self.relations AND delta
3. Loop:
   a. All deltas empty → done
   b. prev_delta = delta
   c. delta = empty
   d. For each rule:
      For each delta position:
        Execute with delta at that position, full elsewhere
        New facts → insert into BOTH self.relations AND delta
   e. Goto (a)
```

```zig
pub fn evaluate(self: *BitmapEvaluator) !void {
    // Phase 1: Initial pass — evaluate all rules naively against full relations
    var deltas: RelationMap = .{};
    defer deinitRelations(&deltas, self.allocator);

    for (self.rules) |rule| {
        if (rule.body.len == 0) continue;
        _ = try self.executeRule(rule, &self.relations, &self.relations, &deltas);
    }

    // Phase 2: Semi-naive loop
    const max_iterations: usize = 10000;
    var iteration: usize = 0;

    while (iteration < max_iterations) {
        // Check if any deltas are non-empty
        if (!hasNonEmptyRelation(&deltas)) break;

        iteration += 1;

        // prev_delta = current deltas, new deltas = empty
        var prev_deltas = deltas;
        deltas = .{};

        // For each rule, try each body position as the delta source
        for (self.rules) |rule| {
            if (rule.body.len == 0) continue;

            if (rule.body.len == 1) {
                // Single body atom: use delta as source
                if (prev_deltas.get(rule.body[0].predicate) != null) {
                    _ = try self.executeRule(rule, &prev_deltas, &self.relations, &deltas);
                }
            } else if (rule.body.len == 2) {
                // Two body atoms: try delta at position 0, then at position 1
                // Position 0: left=delta, right=full
                if (prev_deltas.get(rule.body[0].predicate) != null) {
                    _ = try self.executeRule2(rule, &prev_deltas, &self.relations, &deltas, 0);
                }
                // Position 1: left=full, right=delta
                if (prev_deltas.get(rule.body[1].predicate) != null) {
                    _ = try self.executeRule2(rule, &self.relations, &prev_deltas, &deltas, 1);
                }
            }
        }

        deinitRelations(&prev_deltas, self.allocator);
    }
}
```

### executeRule dispatch

```zig
fn executeRule(
    self: *BitmapEvaluator,
    rule: Rule,
    source_relations: *RelationMap,  // where to read body atoms from
    full_relations: *RelationMap,    // full state (for multi-atom rules)
    deltas: *RelationMap,            // where to write new facts
) !bool {
    if (rule.body.len == 1) {
        return self.executeSingleAtomRule(rule, source_relations, deltas);
    } else if (rule.body.len == 2) {
        return self.executeTwoAtomRule(rule, source_relations, source_relations, deltas);
    }
    return false; // unsupported arity
}
```

For the semi-naive variant with delta at a specific position:

```zig
fn executeRule2(
    self: *BitmapEvaluator,
    rule: Rule,
    left_source: *RelationMap,   // source for body[0]
    right_source: *RelationMap,  // source for body[1]
    deltas: *RelationMap,        // where to write new facts
    delta_pos: usize,            // which position uses delta
) !bool {
    // Look up relations for body[0] and body[1] from their respective sources
    // Execute join
    ...
}
```

## Query Resolution

```zig
pub fn query(self: *BitmapEvaluator, pattern: Atom) ![]Binding {
    const alloc = self.allocator;

    // Get the relation for this predicate
    const rel_ptr = self.relations.getPtr(pattern.predicate) orelse return &[_]Binding{};

    if (pattern.terms.len == 2) {
        return self.queryBinary(pattern, &rel_ptr.binary, alloc);
    } else if (pattern.terms.len == 1) {
        return self.queryUnary(pattern, &rel_ptr.unary, alloc);
    }
    return &[_]Binding{};
}

fn queryBinary(
    self: *BitmapEvaluator,
    pattern: Atom,
    rel: *BinaryRelation,
    alloc: std.mem.Allocator,
) ![]Binding {
    var results: std.ArrayList(Binding) = .{};

    const t0 = pattern.terms[0];
    const t1 = pattern.terms[1];

    // Resolve constants
    const c0: ?u32 = switch (t0) { .constant => |c| self.interner.lookup(c), .variable => null };
    const c1: ?u32 = switch (t1) { .constant => |c| self.interner.lookup(c), .variable => null };

    // If a constant isn't in the interner, no results possible
    switch (t0) { .constant => { if (c0 == null) return &[_]Binding{}; }, .variable => {} }
    switch (t1) { .constant => { if (c1 == null) return &[_]Binding{}; }, .variable => {} }

    if (c0 != null and c1 != null) {
        // Both constants: just check containment
        if (rel.contains(c0.?, c1.?)) {
            var b = Binding.init(alloc);
            try results.append(alloc, b);
        }
    } else if (c0 != null) {
        // Constant in pos 0, variable in pos 1: iterate forward[c0]
        const fwd = rel.getForward(c0.?) orelse return &[_]Binding{};
        var iter = fwd.iterator();
        while (iter.next()) |val| {
            var b = Binding.init(alloc);
            try b.put(t1.variable, self.interner.resolve(val));
            try results.append(alloc, b);
        }
    } else if (c1 != null) {
        // Variable in pos 0, constant in pos 1: iterate reverse[c1]
        const rev = rel.getReverse(c1.?) orelse return &[_]Binding{};
        var iter = rev.iterator();
        while (iter.next()) |val| {
            var b = Binding.init(alloc);
            try b.put(t0.variable, self.interner.resolve(val));
            try results.append(alloc, b);
        }
    } else {
        // Both variables: iterate all tuples
        var domain_iter = rel.domain.iterator();
        while (domain_iter.next()) |a| {
            const fwd = rel.getForward(a) orelse continue;
            const a_str = self.interner.resolve(a);
            var b_iter = fwd.iterator();
            while (b_iter.next()) |b| {
                var binding = Binding.init(alloc);
                try binding.put(t0.variable, a_str);
                try binding.put(t1.variable, self.interner.resolve(b));
                try results.append(alloc, binding);
            }
        }
    }

    return results.toOwnedSlice(alloc);
}
```

**Note on Binding keys:** The old evaluator's Binding uses variable names from the query pattern as keys. The bitmap evaluator does the same — `b.put(t0.variable, resolved_string)`. The variable name strings point into parser memory (owned by the parser arena), which is valid for the lifetime of the test.

## Handling the head variable as join variable

Consider `influenced_t(A, C) :- influenced(A, B), influenced_t(B, C).`

Here:
- Join variable: B (body[0] pos 1, body[1] pos 0)
- Head pos 0 (A): from body[0] pos 0 (left other)
- Head pos 1 (C): from body[1] pos 1 (right other)

The `resolveHeadValue` function maps each head position to one of three values: `join_val`, `left_other`, `right_other`.

But what if the join variable IS a head variable? Example:
```
triangle(X, Y) :- edge(X, Y), edge(Y, X).
```
Here join var Y appears in the head. In that case, `head_map[1]` would point to a body position that IS the join position. `resolveHeadValue` returns `join_val` for that case.

The analysis code must handle this — a head variable can reference the join variable.

## Handling same variable in both positions

Consider `self_loop(X) :- edge(X, X).` — same variable in both positions of a body atom. This means we need to intersect: values that appear in BOTH domain and forward[x] contains x. I.e., find all x where `edge.contains(x, x)`.

For step 5 MVP: iterate domain, check `contains(x, x)` for each. This is a filter, not a join. Handle it as a special case in single-atom rule execution.

## Memory Management

| Data | Owner | Lifetime |
|---|---|---|
| StringInterner + strings | BitmapEvaluator | Until deinit |
| Relations (full) | BitmapEvaluator | Until deinit |
| Delta relations | Per-iteration, freed after use | One fixpoint loop |
| Query Bindings | Caller's allocator | Caller frees |
| JoinAnalysis | Stack | Per-rule execution |

Delta relations are full `BinaryRelation`s with their own forward/reverse hashmaps. They're small (new facts per iteration) but structurally identical to full relations. Create fresh each iteration, free after the iteration ends.

## Tests

All tests in `src/bitmap_evaluator.zig`. Each test mirrors an existing old evaluator test from `datalog.zig` to verify identical results.

### Test 1: Simple ground facts and query

```zig
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

    // Query: parent(X, "bob")
    var q_terms = [_]Term{ .{ .variable = "X" }, .{ .constant = "bob" } };
    const results = try eval.query(.{ .predicate = "parent", .terms = &q_terms });

    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqualStrings("tom", results[0].get("X").?);
}
```

### Test 2: Transitive closure

```zig
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

    // Query: reachable("a", X) → should reach b, c, d
    var q_terms = [_]Term{ .{ .constant = "a" }, .{ .variable = "X" } };
    const results = try eval.query(.{ .predicate = "reachable", .terms = &q_terms });

    try std.testing.expectEqual(@as(usize, 3), results.len);
}
```

### Test 3: member_of transitive

```zig
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

    // Query: member_of_t("alice", G) → developers, employees, domain_users
    var q_terms = [_]Term{ .{ .constant = "alice" }, .{ .variable = "G" } };
    const results = try eval.query(.{ .predicate = "member_of_t", .terms = &q_terms });

    try std.testing.expectEqual(@as(usize, 3), results.len);
}
```

### Test 4: Books and literary influence (multi-predicate joins)

```zig
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
    try std.testing.expectEqual(@as(usize, 3), q1.len);

    // Books in Homer's tradition: Aeneid, Divine Comedy, Paradise Lost
    var q2_terms = [_]Term{ .{ .variable = "Book" }, .{ .constant = "Homer" } };
    const q2 = try eval.query(.{ .predicate = "books_in_tradition", .terms = &q2_terms });
    try std.testing.expectEqual(@as(usize, 3), q2.len);

    // Who influenced Milton: Homer, Virgil, Dante
    var q3_terms = [_]Term{ .{ .variable = "Who" }, .{ .constant = "Milton" } };
    const q3 = try eval.query(.{ .predicate = "influenced_t", .terms = &q3_terms });
    try std.testing.expectEqual(@as(usize, 3), q3.len);
}
```

### Test 5: Ground facts only (no rules, no evaluation)

```zig
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

    // Query all colors
    var q_terms = [_]Term{.{ .variable = "C" }};
    const results = try eval.query(.{ .predicate = "color", .terms = &q_terms });
    try std.testing.expectEqual(@as(usize, 3), results.len);
}
```

### Test 6: Empty query result

```zig
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

    // Query with non-existent constant
    var q_terms = [_]Term{ .{ .constant = "z" }, .{ .variable = "X" } };
    const results = try eval.query(.{ .predicate = "edge", .terms = &q_terms });
    try std.testing.expectEqual(@as(usize, 0), results.len);

    // Query non-existent predicate
    var q2_terms = [_]Term{.{ .variable = "X" }};
    const results2 = try eval.query(.{ .predicate = "nonexistent", .terms = &q2_terms });
    try std.testing.expectEqual(@as(usize, 0), results2.len);
}
```

### Test 7: Fixpoint convergence

```zig
test "bitmap eval: fixpoint converges" {
    const allocator = std.testing.allocator;

    // Cycle: a→b→c→a. Transitive closure should have 9 tuples (3×3).
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

    // Every node reaches every node (including itself via cycle)
    var q_terms = [_]Term{ .{ .variable = "X" }, .{ .variable = "Y" } };
    const results = try eval.query(.{ .predicate = "reach", .terms = &q_terms });
    try std.testing.expectEqual(@as(usize, 9), results.len);
}
```

## Wire into the build

Add to `src/lib.zig`:

```zig
pub const bitmap_evaluator = @import("bitmap_evaluator.zig");
```

And in the test block:

```zig
test {
    // ... existing ...
    _ = @import("bitmap_evaluator.zig");
}
```

## What NOT to do

- Don't modify the old evaluator in `datalog.zig`. It stays functional.
- Don't wire into the CLI (`main.zig`). That's step 6.
- Don't load relations from LMDB. This step uses only ground facts from .dl.
- Don't handle ternary+ relations. Binary and unary only.
- Don't handle 3+ body atom rules. Two body atoms max. Error or skip if more.
- Don't optimize with bulk bitmap ops (bitwiseOrInPlace for join results). Per-tuple insert via `BinaryRelation.insert()` is correct and simple. Optimize later.
- Don't add profiling yet. Get correctness first.

## Checklist

- [ ] `src/bitmap_evaluator.zig` created with `BitmapEvaluator` struct
- [ ] `init`, `deinit` manage interner and relations
- [ ] `addGroundFacts` interns constants and inserts into relations
- [ ] `ensureRelation` creates relations on demand with key ownership
- [ ] `analyzeJoin` extracts join variable and head mapping for 2-atom rules
- [ ] Single-atom rule execution (copy with variable mapping)
- [ ] Two-atom join execution with bitmap candidate selection
- [ ] Semi-naive fixpoint loop with delta tracking
- [ ] `query` returns `[]Binding` with resolved strings
- [ ] Handles constants in body atoms and queries
- [ ] Tests 1-7 passing (matching old evaluator results)
- [ ] Wired into `lib.zig` and `zig build test` passes
- [ ] No changes to existing files except `lib.zig`
