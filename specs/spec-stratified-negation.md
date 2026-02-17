# spec-stratified-negation: BodyElement Refactor + Stratified Negation

Introduces `not` (negation-as-failure) to kb's Datalog, with the prerequisite
`BodyElement` refactor to support heterogeneous body elements. Adapted for the
bitmap evaluator (not the deleted hash-map evaluator).

---

## 0. Motivation

Negation unlocks an entire class of patterns that are impossible with positive-only
Datalog:

```prolog
% Authors who never wrote an epic
non_epic_author(A) :- author(A), not wrote_genre(A, "epic").

% Books with no known influence chain from Homer
independent_of_homer(B) :- book(B), wrote(Author, B), not influenced_t("Homer", Author).

% Works not yet categorized
uncategorized(B) :- book(B), not has_genre(B).
```

Without negation, expressing "everything except X" requires external
post-processing. With stratified negation, it's a single declarative rule.

---

## 1. Scope

This spec covers two tightly coupled changes:

1. **BodyElement refactor** — change `Rule.body` from `[]Atom` to `[]BodyElement`.
   Mechanical prerequisite. No behavioral change.

2. **Stratified negation** — `not` keyword in rule bodies, stratification
   algorithm (Tarjan's SCC), stratum-ordered evaluation in the bitmap evaluator.

**Out of scope:** comparisons (`X > "10"`), aggregation, wildcards (`_`),
query ordering/limits. These are future specs that build on the BodyElement
foundation laid here.

---

## 2. Syntax

`not` before an atom in a rule body:

```prolog
non_epic(B) :- book(B), not genre(B, "epic").
uninfluenced(A) :- author(A), not influenced(_, A).
solo_author(A) :- author(A), not coauthored(A, _).
```

`not` is a **contextual keyword** — it is only recognized before atoms in rule
bodies. A predicate literally named `not` would be ambiguous, but that's an
acceptable restriction.

Negation is **not** allowed in queries (`?-` lines) in this spec. Queries
continue to be `[]Atom`. Extending queries to `[]BodyElement` is trivial
follow-up work but is not needed for the core feature.

---

## 3. AST Changes

### 3.1 New type: `BodyElement`

In `src/datalog.zig`, add after the `Rule` struct:

```zig
pub const BodyElement = union(enum) {
    atom: Atom,
    negated_atom: Atom,
    // Future: comparison, wildcard, builtin

    pub fn getAtom(self: BodyElement) Atom {
        return switch (self) {
            .atom => |a| a,
            .negated_atom => |a| a,
        };
    }

    pub fn isNegated(self: BodyElement) bool {
        return self == .negated_atom;
    }

    pub fn format(self: BodyElement, comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
        switch (self) {
            .atom => |a| try writer.print("{}", .{a}),
            .negated_atom => |a| try writer.print("not {}", .{a}),
        }
    }

    pub fn dupe(self: BodyElement, allocator: Allocator) !BodyElement {
        return switch (self) {
            .atom => |a| .{ .atom = try a.dupe(allocator) },
            .negated_atom => |a| .{ .negated_atom = try a.dupe(allocator) },
        };
    }

    pub fn free(self: BodyElement, allocator: Allocator) void {
        switch (self) {
            .atom => |a| a.free(allocator),
            .negated_atom => |a| a.free(allocator),
        }
    }
};
```

### 3.2 Rule.body type change

```zig
pub const Rule = struct {
    head: Atom,
    body: []BodyElement,  // was []Atom
    // ...
};
```

Update `Rule.format`, `Rule.dupe`, `Rule.free` to use `BodyElement` methods.

### 3.3 ParseResult — no change to queries

`ParseResult.queries` stays as `[][]Atom`. Queries don't need BodyElement yet.

---

## 4. Parser Changes

### 4.1 New function: `parseBodyElement`

Replace the current `parseBody` (which returns `[]Atom`) with one that returns
`[]BodyElement`:

```zig
fn parseBody(self: *Parser) ParseError![]BodyElement {
    var elements: std.ArrayList(BodyElement) = .{};

    const first = try self.parseBodyElement();
    try elements.append(self.allocator(), first);

    while (self.current.type == .comma) {
        self.advance();
        const elem = try self.parseBodyElement();
        try elements.append(self.allocator(), elem);
    }

    return elements.toOwnedSlice(self.allocator());
}

fn parseBodyElement(self: *Parser) ParseError!BodyElement {
    // Check for "not" keyword (contextual)
    if (self.current.type == .identifier and
        std.mem.eql(u8, self.current.text, "not"))
    {
        self.advance();
        const atom = try self.parseAtom();
        return .{ .negated_atom = atom };
    }

    // Default: positive atom
    const atom = try self.parseAtom();
    return .{ .atom = atom };
}
```

### 4.2 parseRule update

`parseRule` calls `parseBody` which now returns `[]BodyElement`. The fact case
(empty body) needs an empty `[]BodyElement` slice:

```zig
if (self.current.type == .dot) {
    self.advance();
    return .{ .head = head, .body = &.{} };  // []BodyElement, zero-length — works
}
```

### 4.3 Query parsing — unchanged

`parseProgram` for `?-` lines continues to call a separate path that produces
`[]Atom` (the existing `parseBody` logic, extracted or kept as `parseQueryBody`).
Simplest approach: rename the old `parseBody` to `parseQueryBody` and keep it
returning `[]Atom`, used only for `?-` lines.

---

## 5. Stratification

### 5.1 Why stratification is needed

Consider this program:

```prolog
wrote("Homer", "The Iliad").
wrote("Virgil", "The Aeneid").
genre("The Iliad", "epic").
non_epic(B) :- wrote(_, B), not genre(B, "epic").
```

The rule for `non_epic` negates `genre`. For the negation to be meaningful,
`genre` must be **complete** — we need to know all genres before we can say
"this book has no genre entry for epic." If `genre` were itself derived from
rules still producing new facts, the negation result would be wrong.

Stratification solves this by partitioning rules into layers (strata). A rule
that negates predicate P is placed in a higher stratum than the rules that
define P. Each stratum runs to fixpoint before the next one begins, guaranteeing
that negated predicates are complete when checked.

### 5.2 Strongly connected components

A **strongly connected component (SCC)** is a maximal group of nodes in a
directed graph where every node can reach every other node through directed
edges. In our predicate dependency graph, predicates in the same SCC are
mutually recursive and must be evaluated together.

Example: the rules `reachable(X, Y) :- edge(X, Y)` and
`reachable(X, Z) :- edge(X, Y), reachable(Y, Z)` make `reachable` depend on
itself — it forms an SCC of size 1 (a self-loop). If `influenced_t` depended
on `wrote` and `wrote` depended back on `influenced_t`, they'd form an SCC of
size 2.

The key insight for stratification: **a negative edge within an SCC means the
program is unstratifiable.** If predicates A and B are mutually recursive (same
SCC) and A negates B, there's no valid evaluation order — you'd need B complete
to evaluate A, but B needs A, creating a cycle.

### 5.3 Tarjan's algorithm

Tarjan's algorithm finds all SCCs in a single depth-first traversal, running in
O(V + E) time where V is the number of predicates and E is the number of
dependency edges.

**How it works:**

1. DFS through the graph. Each node gets two numbers when first visited:
   - `index`: discovery order (0, 1, 2, ...)
   - `lowlink`: initially equal to `index`

2. Visited nodes are pushed onto a stack.

3. When DFS returns from a neighbor back to a node, update the node's `lowlink`
   to `min(self.lowlink, neighbor.lowlink)`. This propagates "I can reach
   something discovered earlier" information back up the DFS tree.

4. After processing all neighbors of a node: if `lowlink == index`, this node
   is the **root** of an SCC. Pop everything off the stack down to and including
   this node — that's one complete SCC.

**Why `lowlink` works:** if a node's lowlink is less than its index, it means
some descendant in the DFS can reach back to an ancestor — they're all in the
same SCC. When lowlink equals index, no descendant can reach further back, so
this node is the earliest entry point of its component.

**Worked example** with our literary predicates:

```
Dependency graph:
  influenced_t → influenced    (positive)
  influenced_t → influenced_t  (positive, self-loop)
  not_in_tradition → author    (positive)
  not_in_tradition → influenced_t (NEGATIVE)
```

Tarjan's DFS might visit: `influenced` (index=0), `influenced_t` (index=1,
self-loop so lowlink stays 1, pops as SCC {influenced_t}), then `author`
(index=2, SCC {author}), then `not_in_tradition` (index=3, SCC
{not_in_tradition}). `influenced` has no dependencies, SCC {influenced}.

SCCs in reverse topological order: {not_in_tradition}, {author},
{influenced_t}, {influenced}. Reverse that for evaluation order:
{influenced}, {influenced_t}, {author}, {not_in_tradition}.

The negative edge from `not_in_tradition` to `influenced_t` crosses SCC
boundaries (good — not within an SCC), so the program is stratifiable.
Strata: 0 = {influenced, author, influenced_t}, 1 = {not_in_tradition}.

### 5.4 Stratification steps

1. **Build predicate dependency graph.** For each rule:
   - `H.predicate → B.predicate` (positive edge) for each positive body atom B
   - `H.predicate → B.predicate` (negative edge) for each negated body atom B

2. **Run Tarjan's** to find SCCs.

3. **Check for negative cycles.** If any SCC contains a negative edge, the
   program is **unstratifiable**:
   ```prolog
   % These form an SCC with a negative edge — unstratifiable
   popular(B) :- book(B), not obscure(B).
   obscure(B) :- book(B), not popular(B).
   ```

4. **Assign strata.** SCCs come out of Tarjan's in reverse topological order.
   Walk them forward, assigning stratum numbers. If SCC_a has a negative edge
   to SCC_b, SCC_a gets a strictly higher stratum number than SCC_b.

5. **Group rules by stratum** of their head predicate.

### 5.5 Data structures

```zig
const DepEdge = struct {
    target: []const u8,
    negative: bool,
};

const DepGraph = std.StringHashMapUnmanaged(std.ArrayListUnmanaged(DepEdge));

const TarjanState = struct {
    index: u32,
    lowlink: u32,
    on_stack: bool,
};
```

### 5.6 Implementation location

New file: `src/stratify.zig`

```zig
pub const StratificationError = error{
    UnstratifiableProgram,
    OutOfMemory,
};

pub const Stratum = struct {
    rules: []const Rule,
};

/// Partition rules into strata. Returns strata in evaluation order
/// (stratum 0 first). Ground facts (empty body) are excluded — they
/// are loaded before evaluation, not part of any stratum.
pub fn stratify(
    rules: []const Rule,
    allocator: Allocator,
) StratificationError![]Stratum {
    // 1. Filter to non-ground rules
    // 2. Build dependency graph
    // 3. Tarjan's SCC
    // 4. Check negative cycles
    // 5. Assign strata, group rules
    // ...
}
```

### 5.7 Trivial case optimization

If no rules contain negated atoms, `stratify()` returns a single stratum
containing all non-ground rules. This preserves current behavior with zero
overhead for programs without negation.

### 5.8 Error reporting

Unstratifiable programs are detected at compile time (before evaluation).
The error should name the involved predicates:

```
error: unstratifiable program — circular negation between "popular" and "obscure"
  popular(B) :- book(B), not obscure(B).
  obscure(B) :- book(B), not popular(B).
```

---

## 6. Bitmap Evaluator Changes

### 6.1 Rule analysis: positive vs negative atoms

Each rule's body is split into **positive atoms** (drive enumeration) and
**negated atoms** (filter candidates). The positive atoms use the existing
join machinery. Negated atoms are applied as post-filters.

```zig
const RuleAnalysis = struct {
    positive_atoms: []const Atom,   // from BodyElement.atom
    negated_atoms: []const Atom,    // from BodyElement.negated_atom
};

fn analyzeRule(rule: Rule, allocator: Allocator) !RuleAnalysis {
    var pos = std.ArrayListUnmanaged(Atom){};
    var neg = std.ArrayListUnmanaged(Atom){};
    for (rule.body) |elem| {
        switch (elem) {
            .atom => |a| try pos.append(allocator, a),
            .negated_atom => |a| try neg.append(allocator, a),
        }
    }
    return .{
        .positive_atoms = pos.items,
        .negated_atoms = neg.items,
    };
}
```

### 6.2 Negation check: `checkNegation`

Given a candidate binding (a set of variable→ID mappings derived from positive
atom enumeration), check whether a negated atom matches any facts. If it does,
the binding is **discarded**.

```zig
/// Returns true if the negation PASSES (i.e., the negated atom does NOT match
/// any facts, so the binding should be KEPT).
/// Returns false if the negated atom matches existing facts (discard binding).
fn checkNegation(
    self: *BitmapEvaluator,
    negated: Atom,
    var_bindings: *const std.StringHashMapUnmanaged(u32),
) bool {
    const rel_ptr = self.relations.getPtr(negated.predicate) orelse
        return true;  // Relation doesn't exist → negation succeeds

    switch (rel_ptr.*) {
        .unary => |*u| {
            const id0 = self.resolveTermId(negated.terms[0], var_bindings) orelse
                return true;
            return !u.contains(id0);
        },
        .binary => |*b| {
            const id0 = self.resolveTermId(negated.terms[0], var_bindings) orelse
                return true;
            const id1 = self.resolveTermId(negated.terms[1], var_bindings) orelse
                return true;
            return !b.contains(id0, id1);
        },
    }
}

fn resolveTermId(
    self: *BitmapEvaluator,
    term: Term,
    var_bindings: *const std.StringHashMapUnmanaged(u32),
) ?u32 {
    return switch (term) {
        .constant => |c| self.interner.lookup(c),
        .variable => |v| var_bindings.get(v),
    };
}
```

### 6.3 Integration into rule execution

The existing `executeSingleAtomRule` and `executeTwoAtomRule` currently work
directly with `rule.body[0]` and `rule.body[1]` as `Atom`. With the BodyElement
change, they need to:

1. Extract the positive atoms from `rule.body`
2. Run the existing join/enumeration logic on positive atoms
3. For each candidate tuple, check all negated atoms
4. Only emit the tuple if all negation checks pass

**Pattern for single positive atom + negations:**

```zig
fn executeSingleAtomRuleWithNegation(
    self: *BitmapEvaluator,
    rule: Rule,
    analysis: RuleAnalysis,
    source_rels: *RelationMap,
    deltas: *RelationMap,
) !bool {
    // Use analysis.positive_atoms[0] as the driving atom
    // (same logic as current executeSingleAtomRule)
    //
    // At each candidate tuple, before inserting:
    //   var bindings = build var bindings from current position
    //   for (analysis.negated_atoms) |neg| {
    //       if (!self.checkNegation(neg, &bindings))
    //           continue :outer;  // skip this candidate
    //   }
    //   // ... insert into deltas
}
```

**Pattern for two positive atoms + negations:**

Same as above — the two-atom join produces candidates, negated atoms filter them.

### 6.4 Variable binding tracking

The current evaluator works with raw `u32` IDs and positional mappings
(`head_from_body`, `VarRef`). For negation checks, we need to know "what is
variable X bound to?" given the current enumeration position.

Add a lightweight variable→ID map built during enumeration:

```zig
/// Build variable bindings from the current iteration state.
/// Used for negation checks — maps variable names to their current u32 IDs.
fn buildVarBindings(
    self: *BitmapEvaluator,
    body_atoms: []const Atom,
    body_vals: []const [2]u32,
    out: *std.StringHashMapUnmanaged(u32),
) void {
    out.clearRetainingCapacity();
    for (body_atoms, 0..) |atom, ai| {
        for (atom.terms, 0..) |term, ti| {
            switch (term) {
                .variable => |v| {
                    out.put(self.allocator, v, body_vals[ai][ti]) catch {};
                },
                .constant => {},
            }
        }
    }
}
```

This is called once per candidate tuple, only when the rule has negated atoms.
Rules without negation skip this entirely (zero overhead for existing programs).

### 6.5 Stratum-ordered evaluation

Replace the current `evaluate()` method:

```zig
pub fn evaluate(self: *BitmapEvaluator) !void {
    const strat = @import("stratify.zig");

    // Pre-create all head relations
    for (self.rules) |rule| {
        if (rule.body.len == 0) continue;
        _ = try self.ensureRelation(rule.head.predicate, rule.head.terms.len);
    }

    // Stratify rules (excludes ground facts)
    const strata = try strat.stratify(self.rules, self.allocator);
    defer {
        for (strata) |s| self.allocator.free(s.rules);
        self.allocator.free(strata);
    }

    // Evaluate each stratum to fixpoint before moving to the next
    for (strata) |stratum| {
        try self.evaluateStratum(stratum.rules);
    }
}

/// Semi-naive fixpoint on a subset of rules (one stratum).
/// This is essentially the current evaluate() body, operating on
/// stratum.rules instead of self.rules.
fn evaluateStratum(self: *BitmapEvaluator, rules: []const Rule) !void {
    // Phase 1: Initial naive pass on these rules
    // Phase 2: Semi-naive loop on these rules
    // (Same structure as current evaluate(), just scoped to this subset)
}
```

**Key invariant:** When evaluating stratum N, all predicates from strata 0..N-1
are **complete** (fully materialized). Negation in stratum N only references
predicates from lower strata, so the negation check sees the final, complete
relation.

### 6.6 Performance characteristics

**Zero overhead for programs without negation.** `stratify()` detects no
negated atoms and returns a single stratum. `evaluateStratum` runs the same
semi-naive loop as the current `evaluate()`. The only added cost is the
stratify call itself, which is O(rules) with no allocation in the fast path.

**Negation check is O(1) per candidate.** `checkNegation` calls
`bitmap.contains(id)` — a single bit test in a roaring bitmap. No iteration,
no materialization at check time. The negated relation was already built during
a lower stratum's fixpoint.

**Early filtering within a rule.** When a rule has both positive atoms and
negated atoms, the negation check runs on each candidate **before** insertion.
If the check fails, we skip that candidate immediately — no wasted work
computing head values or checking for duplicates. For a rule like:

```prolog
% Find books by authors NOT influenced by Homer
original_work(B) :- wrote(A, B), author(A), not influenced_t("Homer", A).
```

The positive atoms `wrote(A, B), author(A)` enumerate candidates via bitmap
joins. For each (A, B) pair, we check `influenced_t("Homer", A)` — a single
`contains()` call. If Homer influenced that author, we skip immediately without
touching the head relation.

**Semi-naive within a stratum skips redundant work.** The delta-based iteration
only processes new tuples each round. Negation doesn't change this — negated
atoms reference completed lower-stratum relations, which are static during the
current stratum's evaluation.

**Full materialization of lower strata is required.** This is the fundamental
cost of stratified negation: you must compute all of `influenced_t` before you
can negate against it. There's no way around this with standard stratified
evaluation. Techniques like demand-driven evaluation (magic sets) can reduce
this by only computing the subset of a relation that's actually queried, but
that's a significant complexity increase and not in scope here.

---

## 7. Call Site Updates

### 7.1 `src/datalog.zig`

- Add `BodyElement` type (§3.1)
- Change `Rule.body` to `[]BodyElement` (§3.2)
- Update `Rule.format`, `Rule.dupe`, `Rule.free`
- Add `parseBodyElement` function (§4.1)
- Split `parseBody` → `parseBody` (returns `[]BodyElement`) + `parseQueryBody`
  (returns `[]Atom`, for `?-` lines)

### 7.2 `src/bitmap_evaluator.zig`

- Import `BodyElement`
- `addGroundFacts`: check `rule.body.len == 0` — unchanged, BodyElement slice
  of length 0 works the same
- `evaluate()`: add stratification call, delegate to `evaluateStratum`
- `executeSingleAtomRule` / `executeTwoAtomRule`: extract positive atoms from
  BodyElement slice, add negation filtering
- Add `checkNegation`, `resolveTermId`, `buildVarBindings` helpers
- Add `RuleAnalysis` and `analyzeRule`

### 7.3 `src/main.zig`

- `AccumulatedParse.queries` stays `ArrayList([]kb.datalog.Atom)` — no change
- `cmdDatalog` query iteration: no change (queries are still `[]Atom`)

### 7.4 `src/lib.zig`

- Add `pub const stratify = @import("stratify.zig");` for module access

### 7.5 `tests/integration.zig`

- Add negation test cases

---

## 8. New file: `src/stratify.zig`

Standalone module. ~150-200 lines. Contains:

- `DepGraph` construction from rules
- Tarjan's SCC algorithm (~30 lines for the DFS visit function)
- Negative cycle detection
- Stratum assignment
- `stratify()` public entry point

Unit tests within the file covering:

```zig
test "stratify: no negation → single stratum" { ... }
test "stratify: simple negation → two strata" { ... }
test "stratify: transitive deps → correct ordering" { ... }
test "stratify: circular negation → error" { ... }
test "stratify: ground facts excluded" { ... }
```

---

## 9. Test Plan

### 9.1 Unit tests (zig build test)

**Parser tests:**
```prolog
% Parses negated atom
non_epic(B) :- book(B), not genre(B, "epic").
% → body[0] = .atom("book", [B]), body[1] = .negated_atom("genre", [B, "epic"])

% Parses multiple negations
standalone(A) :- author(A), not influenced(_, A), not coauthored(A, _).
```

**Stratification tests:** (see §8)

**Evaluator tests:**
```prolog
% Basic negation — filter against base facts
author("Homer"). author("Virgil"). author("Plato").
epic_author("Homer"). epic_author("Virgil").
non_epic_author(A) :- author(A), not epic_author(A).
?- non_epic_author(X).
% Expected: X=Plato

% Negation against empty relation — all pass
book("The Iliad"). book("The Odyssey").
available(B) :- book(B), not checked_out(B).
?- available(X).
% Expected: X=The Iliad, X=The Odyssey

% Multi-stratum — negation depends on derived predicate
influenced("Homer", "Virgil").
influenced("Virgil", "Dante").
influenced("Virgil", "Milton").
influenced("Dante", "Milton").
influenced_t(A, B) :- influenced(A, B).
influenced_t(A, C) :- influenced(A, B), influenced_t(B, C).
author("Homer"). author("Virgil"). author("Dante").
author("Milton"). author("Plato").
not_in_homeric_tradition(A) :- author(A), not influenced_t("Homer", A).
?- not_in_homeric_tradition(X).
% Expected: X=Homer, X=Plato
% (Homer can't influence himself; Plato has no influence edge from Homer)

% Negation with binary relation in join
wrote("Homer", "The Iliad"). wrote("Homer", "The Odyssey").
wrote("Virgil", "The Aeneid"). wrote("Plato", "The Republic").
genre("The Iliad", "epic"). genre("The Odyssey", "epic").
genre("The Aeneid", "epic"). genre("The Republic", "philosophy").
non_epic_work(A, B) :- wrote(A, B), not genre(B, "epic").
?- non_epic_work(X, Y).
% Expected: X=Plato, Y=The Republic

% Unstratifiable — circular negation → compile error
popular(B) :- book(B), not obscure(B).
obscure(B) :- book(B), not popular(B).
% Expected: error before evaluation
```

### 9.2 Integration tests (tests/e2e.sh)

Add test `.dl` files in `tests/fixtures/`:

**`tests/fixtures/negation_basic.dl`:**
```prolog
author("Homer"). author("Virgil"). author("Plato").
epic_author("Homer"). epic_author("Virgil").
non_epic_author(A) :- author(A), not epic_author(A).
?- non_epic_author(X).
```
Expected output: 1 result (Plato).

**`tests/fixtures/negation_multi_stratum.dl`:**
```prolog
influenced("Homer", "Virgil").
influenced("Virgil", "Dante").
influenced("Virgil", "Milton").
influenced_t(A, B) :- influenced(A, B).
influenced_t(A, C) :- influenced(A, B), influenced_t(B, C).
author("Homer"). author("Virgil"). author("Dante").
author("Milton"). author("Plato").
not_in_homeric_tradition(A) :- author(A), not influenced_t("Homer", A).
?- not_in_homeric_tradition(X).
```
Expected: 2 results (Homer and Plato).

**`tests/fixtures/negation_unstratifiable.dl`:**
```prolog
popular(B) :- book(B), not obscure(B).
obscure(B) :- book(B), not popular(B).
book("The Iliad").
?- popular(X).
```
Expected: error message, non-zero exit code.

### 9.3 Regression

All existing tests must pass unchanged. The BodyElement refactor wraps every
existing `Atom` in `.atom`, preserving semantics.

---

## 10. Implementation Order

### Step A: BodyElement refactor (mechanical, no behavior change)

1. Add `BodyElement` type to `datalog.zig`
2. Change `Rule.body` to `[]BodyElement`
3. Update `Rule.format`, `Rule.dupe`, `Rule.free`
4. Split `parseBody` → `parseBody` (returns `[]BodyElement`) + `parseQueryBody`
   (returns `[]Atom`)
5. Update `parseRule` to use new `parseBody`
6. Update `bitmap_evaluator.zig`: everywhere that accesses `rule.body[i]` or
   iterates `rule.body`, extract the atom via `elem.atom` or switch on the tag.
   For now, `negated_atom` cases are `unreachable`.
7. Update any test code that constructs `Rule` literals
8. `zig build test` — all existing tests pass, zero behavior change

### Step B: Parser support for `not`

1. Add `parseBodyElement` that checks for `not` keyword
2. Add parser tests for negation syntax
3. `zig build test` — new parser tests pass, existing tests unchanged

### Step C: Stratification

1. Create `src/stratify.zig` with Tarjan's SCC + stratification
2. Add unit tests for stratification
3. Wire into `bitmap_evaluator.zig`: `evaluate()` calls `stratify()` and
   iterates strata
4. Programs without negation: single stratum, identical behavior to before
5. `zig build test` — all existing tests pass (single-stratum path)

### Step D: Negation in evaluator

1. Add `checkNegation`, `resolveTermId`, `buildVarBindings` to evaluator
2. Update `executeSingleAtomRule` / `executeTwoAtomRule` to split positive
   and negated atoms, apply negation filtering
3. Add evaluator tests with negation
4. Add e2e test fixtures
5. `zig build test && zig build test-integration` — all tests pass

### Step E: Error handling for unstratifiable programs

1. Wire `StratificationError.UnstratifiableProgram` through to `cmdDatalog`
2. Print meaningful error message naming the involved predicates
3. Add e2e test for unstratifiable program (expect non-zero exit)

---

## 11. Safety Condition

All variables in a negated atom **must** be bound by positive atoms in the same
rule body. This is the standard Datalog safety requirement.

**Safe:**
```prolog
non_epic(B) :- book(B), not genre(B, "epic").      % B bound by book(B)
original(A, B) :- wrote(A, B), not influenced(_, A). % A bound by wrote(A, B)
```

**Unsafe (reject at compile time):**
```prolog
bad(B) :- not genre(B, "epic").           % B only appears in negation
bad2(A) :- author(A), not wrote(A, B).    % B not bound by any positive atom
```

Implement as a validation pass — either in `stratify()` or as a separate
`validateSafety()` function called before evaluation. Error message:

```
error: unsafe rule — variable "B" appears only in negation
  bad2(A) :- author(A), not wrote(A, B).
```

---

## 12. Restrictions and Future Work

### Current restrictions

- **Negation only in rule bodies**, not in queries
- **All negated variables must be bound** (safety condition, §11)
- **Arity ≤ 2** for negated relations (same limit as bitmap evaluator)

### Future extensions building on BodyElement

- **Comparisons** — add `.comparison` variant to BodyElement
- **Wildcards** (`_`) — desugar in parser to fresh variables
- **Aggregation** — separate head-side change, stratification interaction
- **Graph traversal built-ins** — `.builtin_reachable`, `.builtin_shortest_path`

---

## 13. Compatibility

All existing `.dl` files work unchanged. The `BodyElement` refactor wraps every
existing body atom in `.atom` — purely internal. `not` is a new contextual
keyword; no existing program uses it since it was previously a parse error.
