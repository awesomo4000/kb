#!/bin/bash
# End-to-end test for kb
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
KB="$ROOT_DIR/zig-out/bin/kb"
FIXTURES="$SCRIPT_DIR/fixtures"
TEST_DB="$SCRIPT_DIR/.kb-test"

FAILED=0

assert_contains() {
    local output="$1"
    local expected="$2"
    local test_name="$3"
    if echo "$output" | grep -q "$expected"; then
        echo "  OK: $test_name"
    else
        echo "  FAIL: $test_name"
        echo "    Expected to contain: $expected"
        echo "    Got: $output"
        FAILED=1
    fi
}

assert_line_count() {
    local output="$1"
    local expected="$2"
    local test_name="$3"
    local actual=$(echo "$output" | grep -c "^" || true)
    if [ "$actual" -ge "$expected" ]; then
        echo "  OK: $test_name ($actual lines)"
    else
        echo "  FAIL: $test_name"
        echo "    Expected at least $expected lines, got $actual"
        FAILED=1
    fi
}

echo "=== kb end-to-end test ==="

# Clean up
rm -rf "$TEST_DB"

# Build if needed
if [ ! -f "$KB" ]; then
    echo "Building kb..."
    (cd "$ROOT_DIR" && zig build)
fi

# Test 1: Ingest
echo ""
echo "--- Test: Ingest ---"
OUTPUT=$(KB_PATH="$TEST_DB" "$KB" ingest "$FIXTURES/sample_facts.jsonl" 2>&1)
assert_contains "$OUTPUT" "Ingested 24 facts" "ingested 24 facts"

# Test 2: Entity queries
echo ""
echo "--- Test: Entity queries ---"
OUTPUT=$(KB_PATH="$TEST_DB" "$KB" get author/)
assert_contains "$OUTPUT" "Homer" "found Homer"
assert_contains "$OUTPUT" "Plato" "found Plato"
assert_line_count "$OUTPUT" 5 "at least 5 authors"

OUTPUT=$(KB_PATH="$TEST_DB" "$KB" get book/The\ Odyssey)
assert_contains "$OUTPUT" "Homer" "Odyssey by Homer"
assert_contains "$OUTPUT" "epic" "Odyssey is epic"

# Test 3: Datalog - transitive queries
echo ""
echo "--- Test: Datalog ---"
OUTPUT=$(KB_PATH="$TEST_DB" "$KB" datalog "$FIXTURES/sample_rules.dl" 2>&1)

# Direct influence: only Virgil was directly influenced by Homer
assert_contains "$OUTPUT" "influenced_by(Who, \"Homer\")" "query ran"
assert_contains "$OUTPUT" "Who = Virgil" "Virgil influenced by Homer"

# Transitive: Virgil, Dante, Milton all trace back to Homer
assert_contains "$OUTPUT" "influenced_t(Who, \"Homer\")" "transitive query ran"
assert_contains "$OUTPUT" "Dante Alighieri" "Dante in transitive chain"
assert_contains "$OUTPUT" "John Milton" "Milton in transitive chain"
assert_contains "$OUTPUT" "(3 results)" "3 authors influenced by Homer"

# Contemporaries
assert_contains "$OUTPUT" "Plato" "found Plato"
assert_contains "$OUTPUT" "Aristotle" "found Aristotle"

# Test 4: Datalog - pure .dl (no @map, no LMDB store)
echo ""
echo "--- Test: Pure Datalog (no store) ---"
PURE_DL=$(mktemp /tmp/kb-test-XXXXXX.dl)
cat > "$PURE_DL" << 'EOF'
edge("a", "b").
edge("b", "c").
reachable(X, Y) :- edge(X, Y).
reachable(X, Z) :- edge(X, Y), reachable(Y, Z).
?- reachable("a", X).
EOF
OUTPUT=$(cd /tmp && "$KB" datalog "$PURE_DL" 2>&1)
assert_contains "$OUTPUT" "b" "reachable a->b"
assert_contains "$OUTPUT" "c" "reachable a->c"
assert_contains "$OUTPUT" "(2 results)" "2 reachable results"
rm -f "$PURE_DL"

# Clean up
rm -rf "$TEST_DB"

echo ""
if [ "$FAILED" -eq 0 ]; then
    echo "=== All tests passed ==="
    exit 0
else
    echo "=== Some tests failed ==="
    exit 1
fi
