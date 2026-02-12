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

## License

MIT
