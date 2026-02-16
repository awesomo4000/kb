# Spec: BodyElement Refactor + Stratified Negation

## Context

The bitmap evaluator is the sole evaluation path (step 7 removed the old evaluator).
To support `not P(X)` in rule bodies, two things are needed:

1. The AST must support body elements that aren't plain atoms (mechanical refactor).
2. The evaluator must partition rules into strata and handle negation during execution.

This spec combines Phase 1 (BodyElement refactor) and Phase 3 (stratified negation)
from the datalog-extensions doc, adapted for the bitmap evaluator.

## Syntax

`not` keyword before an atom in a rule body:

```prolog
% Items not in the excluded set
filtered(X) :- base(X), not excluded(X).

% Accounts with no group membership
orphan_account(U) :- user(U), not member_of(U, _).

% Scan actions not yet completed
actionable(Tool, Host, Port) :-
    scan_needed(Tool, Host, Port),
    not completed(Tool, Host, Port).
```

Unstratifiable programs (circular negation) are a compile-time error:

```prolog
% ERROR: circular negation
p(X) :- q(X), not r(X).
r(X) :- not p(X).
```

## AST changes

### New types in `src/datalog.zig`

Add `BodyElement` union after the existing `Rule` struct:

```zig
pub const BodyElement = union(enum) {
    atom: Atom,
    negated_atom: Atom,
};
```

### Change `Rule.body` type

```zig
pub const Rule = struct {
    head: Atom,
    body: []BodyElement,  // was []Atom
    // ...
};
```

Update `Rule.format`, `Rule.dupe`, `Rule.free` to handle `BodyElement` instead
of `Atom`. The format for a negated atom prints `not pred(...)`.

### Change `ParseResult.queries` type

Queries can also contain negated atoms in their body:

```zig
pub const ParseResult = struct {
    rules: []Rule,
    queries: [][]BodyElement,  // was [][]Atom
    mappings: []Mapping,
    includes: [][]const u8,
};
```

## Parser changes

### `parseBody` returns `[]BodyElement`

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
```

### New `parseBodyElement`

```zig
fn parseBodyElement(self: *Parser) ParseError!BodyElement {
    // Check for "not" keyword (contextual - only before an atom in body position)
    if (self.current.type == .identifier and
        std.mem.eql(u8, self.current.text, "not"))
    {
        self.advance();
        const atom = try self.parseAtom();
        return .{ .negated_atom = atom };
    }

    const atom = try self.parseAtom();
    return .{ .atom = atom };
}
```

`not` is a contextual keyword - not reserved. Nobody names their predicates `not`.

### Wildcard `_` desugaring in `parseTerm`

When `_` appears as a term, generate a unique anonymous variable name
(e.g., `_anon_0`, `_anon_1`) so each wildcard binds independently:

```zig
fn parseTerm(self: *Parser) ParseError!Term {
    if (self.current.type == .identifier) {
        const text = self.current.text;
        if (std.mem.eql(u8, text, "_")) {
            // Wildcard: generate unique anonymous variable
            const name = try std.fmt.allocPrint(self.allocator(), "_anon_{}", .{self.wildcard_counter});
            self.wildcard_counter += 1;
            self.advance();
            return .{ .variable = name };
        }
        // ... rest of existing parseTerm ...
    }
    // ...
}
```

Add `wildcard_counter: u32 = 0` field to `Parser`.

## Stratification

### Algorithm

Runs once before evaluation, at the AST level (evaluator-independent).

1. Build a predicate dependency graph from the rules:
   - `H :- ..., P(...), ...` -> positive edge `H.predicate -> P.predicate`
   - `H :- ..., not P(...), ...` -> **negative** edge `H.predicate -> P.predicate`

2. Compute SCCs using Tarjan's algorithm.

3. If any SCC contains a negative edge -> error: `UnstratifiableProgram`.
   Report which predicates form the cycle.

4. Topologically sort SCCs. Assign stratum numbers. A predicate with a
   negative dependency on stratum N gets stratum >= N+1.

5. Group rules by their head predicate's stratum. Return `[][]const Rule`.

### Where it lives

New function on `BitmapEvaluator`:

```zig
fn stratify(self: *BitmapEvaluator) ![][]const Rule {
    // Build dependency graph from self.rules
    // Run Tarjan's SCC
    // Check for negative cycles
    // Assign strata
    // Return rules grouped by stratum
}
```

Alternatively, this could be a standalone function in `datalog.zig` since it
only operates on the AST. Either location is fine - the evaluator needs to call
it, and it has no dependency on bitmap types.

### Ground facts and strata

Ground facts (rules with empty body) have no dependencies. They belong to
stratum 0 implicitly and are loaded before any evaluation via `addGroundFacts`.
The stratifier should skip them (they're handled separately).

Predicates that only appear as ground facts (never as a rule head with a body)
are in stratum 0. Rules that negate such predicates go to stratum 1+.

## Evaluator changes

### `evaluate()` iterates strata

```zig
pub fn evaluate(self: *BitmapEvaluator) !void {
    const strata = try self.stratify();
    defer self.allocator.free(strata);
    // Each stratum entry's rules are slices into self.rules, no freeing needed

    for (strata) |stratum_rules| {
        try self.evaluateStratum(stratum_rules);
    }
}
```

`evaluateStratum` is the current `evaluate()` method (naive pass + semi-naive
loop), operating on a subset of rules. After a stratum completes, its predicates
are frozen - they will not change in future strata.

### Handling negated atoms in rule execution

Negated atoms are **filters**, not **generators**. They never produce new
bindings - they only discard existing ones. A negated atom must have all its
variables bound by preceding positive atoms in the same rule body.

**Safety check:** At stratification time (or at rule analysis time), verify
that every variable in a `negated_atom` appears in at least one positive atom
in the same rule body. If not, emit an error - unbounded negation is unsafe.

### Single-atom rules with negation

A single-atom rule cannot be *just* a negated atom (that would be unsafe - no
positive atom to generate bindings from). So `executeSingleAtomRule` does not
need to handle negation. If `rule.body.len == 1`, it must be a positive atom.

### Two-atom rules with negation

For a rule like `filtered(X) :- base(X), not excluded(X)`:
- `body[0]` is positive atom `base(X)` - generates candidates
- `body[1]` is `negated_atom` `excluded(X)` - filters candidates

The execution flow:

1. Iterate the positive atom's relation as normal to get candidate tuples.
2. For each candidate, check if the negated atom's pattern matches in its
   relation. If it matches -> **discard** (negation fails). If no match ->
   **keep** (negation succeeds).

### General approach for mixed bodies

Rather than special-casing every combination, the implementation should:

1. Separate the body into positive atoms and negated atoms.
2. Execute the positive atoms as joins (existing `executeSingleAtomRule` /
   `executeTwoAtomRule` logic).
3. For each candidate tuple produced by the positive join, check all negated
   atoms. If any negated atom matches, discard the candidate.

This means the existing join machinery doesn't change - negation is a
post-filter on join results before they're inserted as derived facts.

### Implementation strategy

The cleanest approach for the bitmap evaluator:

1. Extract positive atoms from `rule.body` -> run existing join logic to get
   candidate tuples (list of `[2]u32` values).
2. Before inserting each candidate into the target relation and deltas, check
   all negated atoms. If any negated atom is satisfied by the candidate values,
   skip that candidate.

This keeps the join code untouched and adds negation as a filter layer.

```zig
// After computing new_tuples from positive atoms...
for (new_tuples.items) |hv| {
    if (self.negationSatisfied(rule, hv)) continue; // any negation matches -> skip
    if (try self.insertDerived(head.predicate, head_arity, hv, deltas)) {
        changed = true;
    }
}
```

## Call site updates

### `src/datalog.zig`

- `Rule.body` type: `[]Atom` -> `[]BodyElement`
- `Rule.format`: iterate `BodyElement`, print `not` prefix for `.negated_atom`
- `Rule.dupe`: dupe each `BodyElement`
- `Rule.free`: free each `BodyElement`
- `parseBody`: return `[]BodyElement`, call `parseBodyElement`
- `parseTerm`: handle `_` wildcard
- `ParseResult.queries`: `[][]Atom` -> `[][]BodyElement`
- Add `BodyElement` type with format/dupe/free methods
- Add `wildcard_counter` to Parser

### `src/bitmap_evaluator.zig`

- `evaluate()`: call `stratify()`, iterate strata
- `evaluateStratum()`: new method (current evaluate logic)
- `stratify()`: new method - dependency graph, Tarjan's, strata assignment
- `executeSingleAtomRule`: extract `.atom` from `BodyElement` (body[0] is always positive)
- `executeTwoAtomRule`: extract `.atom` from body elements, handle mixed pos/neg
- Add `negationSatisfied` / `checkNegatedAtom` helpers
- `addGroundFacts`: `rule.body.len == 0` still works (ground facts have empty body)
- `analyzeJoin`: takes `Atom` args - callers extract atoms from `BodyElement`

### `src/main.zig`

- `AccumulatedParse.queries`: `ArrayList([]kb.datalog.Atom)` -> `ArrayList([]kb.datalog.BodyElement)`
- `cmdDatalog` query iteration: extract atoms from BodyElement for display and query
- Ground fact detection: `if (rule.body.len == 0)` - unchanged

### `src/bitmap_ingest.zig`

- Not affected - ingest doesn't look at rule bodies.

### `src/lib.zig`

- No changes needed (re-exports `datalog` module which includes new types).

## Test plan

### New parser tests

```prolog
% Parse negated atom
test "parser negated body element"
filtered(X) :- base(X), not excluded(X).
% Verify: body[0] is .atom, body[1] is .negated_atom

% Parse wildcard
test "parser wildcard desugaring"
has_member(G) :- member_of(_, G).
% Verify: body[0].atom.terms[0] is .variable with _anon_* name

% Parse negation in query
test "parser negated query element"
?- user(X), not admin(X).
% Verify: query body has .atom and .negated_atom
```

### New evaluator tests

```prolog
% Basic negation
test "bitmap eval: basic negation"
base("a"). base("b"). base("c").
excluded("b").
filtered(X) :- base(X), not excluded(X).
?- filtered(X).
% Expected: a, c (2 results)

% Negation with binary relation
test "bitmap eval: negation binary"
user("alice"). user("bob"). user("charlie").
admin("alice").
non_admin(X) :- user(X), not admin(X).
?- non_admin(X).
% Expected: bob, charlie (2 results)

% Stratification: negation depends on derived predicate
test "bitmap eval: stratified negation"
edge("a", "b"). edge("b", "c"). edge("c", "d").
reachable(X, Y) :- edge(X, Y).
reachable(X, Z) :- edge(X, Y), reachable(Y, Z).
not_reachable_from_a(X) :- edge(_, X), not reachable("a", X).
% Expected: 0 results (all nodes reachable from a)

% Unstratifiable program
test "bitmap eval: circular negation error"
p(X) :- q(X), not r(X).
r(X) :- not p(X).
% Expected: error.UnstratifiableProgram at stratification time

% Wildcard in negation
test "bitmap eval: wildcard negation"
user("alice"). user("bob"). user("charlie").
member_of("alice", "admins"). member_of("bob", "users").
orphan(X) :- user(X), not member_of(X, _).
?- orphan(X).
% Expected: charlie (1 result)
```

### E2E test

Add Test 5 to `tests/e2e.sh`:

```bash
# Test 5: Negation
echo ""
echo "--- Test: Negation ---"
NEG_DL=$(mktemp /tmp/kb-test-XXXXXX.dl)
cat > "$NEG_DL" << 'EOF'
user("alice"). user("bob"). user("charlie").
admin("alice").
non_admin(X) :- user(X), not admin(X).
?- non_admin(X).
EOF
OUTPUT=$(cd /tmp && "$KB" datalog "$NEG_DL" 2>&1)
assert_contains "$OUTPUT" "bob" "non_admin bob"
assert_contains "$OUTPUT" "charlie" "non_admin charlie"
assert_contains "$OUTPUT" "(2 results)" "2 non-admin results"
rm -f "$NEG_DL"
```

### Existing tests

All existing tests must pass unchanged. Existing `.dl` files contain no `not`
keywords, so all body elements parse as `.atom` - behavior is identical.

## Checklist

- [ ] `BodyElement` union added to `datalog.zig`
- [ ] `Rule.body` changed from `[]Atom` to `[]BodyElement`
- [ ] `Rule.format/dupe/free` updated for `BodyElement`
- [ ] `ParseResult.queries` changed from `[][]Atom` to `[][]BodyElement`
- [ ] `parseBody` returns `[]BodyElement`, calls `parseBodyElement`
- [ ] `parseBodyElement` handles `not` prefix
- [ ] `parseTerm` handles `_` wildcard with counter
- [ ] `wildcard_counter` field added to `Parser`
- [ ] Parser tests for negation, wildcards
- [ ] `stratify()` implemented: dependency graph, Tarjan's SCC, negative cycle check
- [ ] `evaluate()` calls `stratify()`, iterates strata
- [ ] `evaluateStratum()` extracted from current `evaluate()` logic
- [ ] Negation filter in rule execution (post-join check)
- [ ] Safety check: negated atom variables must appear in positive atoms
- [ ] `main.zig` updated for `BodyElement` in queries
- [ ] Evaluator tests: basic negation, binary negation, stratified, circular error, wildcard
- [ ] E2E test 5 added for negation
- [ ] All existing tests pass unchanged
- [ ] `zig build` succeeds
- [ ] `zig build test` passes
- [ ] `zig build test-all` passes
- [ ] `tests/e2e.sh` passes (all 5 tests)
