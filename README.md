# kb

A hypergraph fact store with Datalog queries, backed by LMDB and roaring bitmaps.

## Core Idea

Everything is stored as **facts** — hyperedges connecting typed entities:

```json
{"edges": [["player", "Alice"], ["team", "Rockets"], ["rel", "plays_for"]], "source": "league"}
```

Query from any angle:
```bash
kb get player/Alice          # What team is Alice on?
kb get team/Rockets          # Who plays for the Rockets?
kb get rel/plays_for         # All player-team relationships
```

## Install

```bash
zig build   # Requires Zig 0.15.2
```

## Usage

```bash
kb ingest data.jsonl          # Load facts from JSONL
kb get team/                  # Browse entities
kb datalog rules.dl           # Run Datalog rules and queries
```

## Fact Format

Each fact connects multiple typed entities as a hyperedge. The store indexes every entity for fast lookups from any direction.

```json
{"edges": [["team", "Wolves"], ["team", "Rockets"], ["rel", "won"]], "source": "league"}
```

Use `@map` directives in `.dl` files to bridge hypergraph facts into Datalog predicates:
```prolog
@map plays_for(P, T) = [rel:plays_for, player:P, team:T].
@map won(W, L) = [rel:won, team:W, team:L].
```

## Language Features

kb uses Datalog — a declarative language where you define rules that derive new facts from existing ones. The engine applies rules repeatedly until no new facts are derived (fixpoint).

### Rules and recursion

```prolog
won("Wolves", "Rockets").
won("Bears", "Wolves").

% Direct rule
dominates(A, B) :- won(A, B).

% Recursive — finds the full chain no matter how deep
dominates(A, C) :- won(A, B), dominates(B, C).

?- dominates("Bears", X).   % X = Wolves, X = Rockets
```

### Wildcards

Use `_` to ignore a position. Each `_` is independent.

```prolog
has_roster(T) :- plays_for(_, T).   % any team with at least one player
```

### Stratified negation

`not` filters out bindings where a fact exists. Variables in negated atoms must appear in a positive atom in the same rule (safety requirement). Rules with negation are automatically stratified.

```prolog
loser(T) :- won(_, T).
unbeaten(T) :- team(T), not loser(T).
```

### Comparison operators

`=`, `!=`, `<`, `>`, `<=`, `>=` filter bindings without generating new facts. Both sides must be bound by a positive atom. Numeric when both values parse as integers, lexicographic otherwise.

```prolog
high_scorer(P) :- points(P, Pts), Pts >= "20".
mid_range(P)   :- points(P, Pts), Pts >= "10", Pts < "20".
rivals(A, B)   :- won(A, B), A != B.
```

### Putting it together

A complete example combining all features — see `tests/fixtures/demo.dl`:

```prolog
% Facts
plays_for("Alice", "Rockets").  plays_for("Carol", "Wolves").
points("Alice", "28").          points("Carol", "35").
won("Wolves", "Rockets").       won("Bears", "Wolves").

% Recursive dominance
dominates(A, B) :- won(A, B).
dominates(A, C) :- won(A, B), dominates(B, C).

% Negation: teams with no losses
loser(T) :- won(_, T).
unbeaten(T) :- team(T), not loser(T).

% Comparisons + recursion: high scorers on dominant teams
high_on_team(P, T) :- points(P, Pts), Pts >= "20", plays_for(P, T).
top_threat(P) :- high_on_team(P, T), dominates(T, "Rockets").

?- top_threat(P).   % Carol (Wolves dominate Rockets, Carol scored 35)
```

## Evaluation

Rules are evaluated using a bitmap-based semi-naive engine. Relations are stored as roaring bitmap sets, and joins are computed via bitmap intersection. Stratification handles negation by partitioning rules into layers evaluated in dependency order.

## Storage

LMDB provides memory-mapped, concurrent-read storage. Data persists in the `.kb/` directory.

## License

MIT
