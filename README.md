# kb

A hypergraph-based fact store with Datalog queries.

## Core Idea

Everything is stored as **facts** — hyperedges connecting multiple typed entities.

```json
{"edges": [["author", "alice"], ["book", "1984"], ["rel", "wrote"]], "source": "library"}
{"edges": [["author", "alice"], ["book", "animal_farm"], ["rel", "wrote"]], "source": "library"}
```

Query from any angle:
```bash
kb get author/alice          # What did alice write?
kb get book/1984             # Who wrote 1984?
kb get rel/wrote             # All author-book relationships
```

## Install

```bash
zig build   # Requires Zig 0.15.2
```

## Usage

```bash
# Ingest facts from JSONL
kb ingest data.jsonl

# Query entities
kb get author/
kb get author/alice
kb get author/alice/book/

# Run Datalog rules
kb datalog rules.dl
```

## Datalog

Define rules over the hypergraph using `@map` directives:

```prolog
% Map hypergraph facts to predicates
@map wrote(A, B) = [rel:wrote, author:A, book:B].

% Define derived relationships
influenced_by(A, C) :- wrote(A, B), references(B, C).

% Query
?- influenced_by(A, C).
```

## Fact Format

```json
{
  "edges": [["type", "id"], ["type", "id"], ...],
  "source": "tool_name"
}
```

Each fact connects multiple typed entities. The hypergraph indexes by entity for fast lookups from any direction.

## Storage

Uses LMDB for memory-mapped, concurrent-read storage. Data persists in `.kb/` directory.

## How Datalog Works

Datalog is a declarative query language where you define **rules** that derive new facts from existing ones.

**Facts** are things you know:
```datalog
parent("alice", "bob").    % alice is bob's parent
parent("bob", "charlie").  % bob is charlie's parent
```

**Rules** derive new facts:
```datalog
grandparent(X, Z) :- parent(X, Y), parent(Y, Z).
```

This reads: "X is grandparent of Z **if** X is parent of Y **and** Y is parent of Z."

**Evaluation** repeatedly applies rules until no new facts are derived:
```
Start:    parent(alice,bob), parent(bob,charlie)
Apply:    grandparent(alice,charlie)  ← new fact derived!
Apply:    (no more new facts)
Done.
```

**Queries** ask what's true:
```datalog
?- grandparent(X, "charlie").   % Who are charlie's grandparents?
   X = alice
```

The power is in **recursive rules** — finding transitive relationships:
```datalog
ancestor(X, Y) :- parent(X, Y).
ancestor(X, Z) :- parent(X, Y), ancestor(Y, Z).
```

This finds all ancestors, no matter how many generations back. The engine keeps applying rules until it reaches a fixpoint (no new facts). You describe *what* you want, not *how* to compute it.

## License

MIT
