# kb Development Workflow

## Roles

- **Aaron** — project owner, architect
- **Architect** — Claude in claude.ai, designs architecture, writes specs, reviews code
- **Coder** — Claude Code instance, implements specs, pushes code

## How a task flows

```
1. Aaron + Architect design the feature in claude.ai
2. Architect writes a spec → committed to specs/NN-name.md
3. Architect creates a GitHub issue linking the spec
4. Coder picks up the issue, implements on a branch
5. Coder opens a PR referencing the issue ("Closes #N")
6. Aaron + Architect review the PR, leave comments
7. Coder addresses feedback, pushes fixes
8. PR merged, spec moved to specs/done/, issue auto-closes
```

## For the Coder: step-by-step

### 1. Pick up the issue

Read the GitHub issue assigned to you. It will link to a spec file in
`specs/` — that's your primary reference. Read the spec thoroughly
before writing any code.

### 2. Create a branch

Branch from `main`. Name it after the spec number:

```
git checkout -b step-04-bitmap-ingest
```

### 3. Implement

Follow the spec. Key rules:

- **Do what the spec says.** If the spec specifies an API, file name,
  test case, or approach — follow it. If you think something should be
  different, say so in a comment on the issue before diverging.
- **Don't touch files the spec says not to touch.** Specs have a
  "What NOT to do" section. Respect it.
- **Write all specified tests.** The spec lists test cases with expected
  behavior. Implement all of them. Add more if you find edge cases.
- **Keep commits focused.** One logical change per commit. The spec's
  checklist is a good guide for commit boundaries.

### 4. Verify before pushing

```bash
zig build test              # Unit tests (inline in src/)
zig build test-integration  # Integration tests (tests/)
zig build test-all          # Both
```

All tests must pass — both the new ones you wrote and all existing ones.
Zero regressions.

### 5. Open a PR

- Title: `Step NN: Short description`
- Body: Reference the issue (`Closes #N`) and the spec
- Include a summary of what you implemented and any decisions you made
  that weren't explicitly in the spec
- If you deviated from the spec, explain why

### 6. Address review feedback

Aaron and Architect will review and may leave comments on the PR.
Fix issues and push to the same branch. Respond to comments explaining
what you changed.

### 7. After merge

Once the PR is merged:

```bash
git mv specs/NN-name.md specs/done/NN-name.md
git commit -m "Move spec NN to done"
git push
```

## Spec format

Specs live in `specs/` and follow this structure (see `specs/done/` for
examples):

- **Context** — why this step exists, what it depends on
- **API** — exact types, function signatures, file names
- **Implementation** — how it should work, with code sketches
- **Memory ownership** — who allocates, who frees
- **Tests** — numbered test cases with setup and expected results
- **What NOT to do** — explicit scope boundaries
- **Checklist** — checkboxes for all deliverables

## Conventions

- Zig 0.15.2
- LMDB for persistence
- rawr for roaring bitmaps (kb dependency)
- Test data uses books/authors/literary influence (Homer, Virgil, etc.)
- kb is a general-purpose library — no application-specific references
- Old and new evaluator code coexist until the explicit deletion step
- Wire new modules into `src/lib.zig` for test discovery

## Repository structure

```
src/
  main.zig                 # CLI
  datalog.zig              # Parser, AST, old evaluator (don't modify yet)
  fact_store.zig           # LMDB persistence
  fact.zig                 # Entity/Fact types
  hypergraph_source.zig    # Old FactSource (don't modify yet)
  lib.zig                  # Module root, test imports
  string_interner.zig      # Step 01: string ↔ u32 mapping
  relation.zig             # Step 02: bitmap-backed relations
  fact_fetcher.zig         # Step 03: raw fact fetching interface
specs/
  done/                    # Completed specs
tests/
  integration.zig          # Integration tests
  bench.zig                # Benchmarks
  fixtures/                # Test data files
```

## Build reference

```bash
zig build                          # Build kb binary
zig build test                     # Unit tests
zig build test-integration         # Integration tests
zig build test-all                 # Both
zig build run -- datalog rules.dl  # Run Datalog
```
