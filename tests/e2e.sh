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

# Test 5: Negation basic
echo ""
echo "--- Test: Negation basic ---"
OUTPUT=$(cd /tmp && "$KB" datalog "$FIXTURES/negation_basic.dl" 2>&1)
assert_contains "$OUTPUT" "Plato" "non-epic author is Plato"
assert_contains "$OUTPUT" "(1 results)" "1 non-epic author"

# Test 6: Negation multi-stratum
echo ""
echo "--- Test: Negation multi-stratum ---"
OUTPUT=$(cd /tmp && "$KB" datalog "$FIXTURES/negation_multi_stratum.dl" 2>&1)
assert_contains "$OUTPUT" "Homer" "Homer not in own tradition"
assert_contains "$OUTPUT" "Plato" "Plato not in Homeric tradition"
assert_contains "$OUTPUT" "(2 results)" "2 authors not in Homeric tradition"

# Test 7: Unstratifiable program
echo ""
echo "--- Test: Unstratifiable program ---"
if (cd /tmp && "$KB" datalog "$FIXTURES/negation_unstratifiable.dl" 2>&1); then
    echo "  FAIL: expected non-zero exit code"
    FAILED=1
else
    echo "  OK: unstratifiable program rejected"
fi

# Test 8: Wildcards
echo ""
echo "--- Test: Wildcards ---"
WC_DL=$(mktemp /tmp/kb-test-XXXXXX.dl)
cat > "$WC_DL" << 'EOF'
edge("a", "b"). edge("b", "c"). edge("c", "d").
has_outgoing(X) :- edge(X, _).
has_incoming(X) :- edge(_, X).
?- has_outgoing(X).
?- has_incoming(X).
EOF
OUTPUT=$(cd /tmp && "$KB" datalog "$WC_DL" 2>&1)
assert_contains "$OUTPUT" "has_outgoing(X)" "wildcard query ran"
assert_contains "$OUTPUT" "X = a" "a has outgoing"
assert_contains "$OUTPUT" "X = b" "b has outgoing"
assert_contains "$OUTPUT" "X = c" "c has outgoing"
assert_contains "$OUTPUT" "X = d" "d has incoming"
rm -f "$WC_DL"

# Test 9: Comparisons
echo ""
echo "--- Test: Comparisons ---"
CMP_DL=$(mktemp /tmp/kb-test-XXXXXX.dl)
cat > "$CMP_DL" << 'EOF'
score("alice", "90"). score("bob", "40"). score("carol", "75"). score("dave", "10").
high(X) :- score(X, S), S > "50".
low(X) :- score(X, S), S <= "20".
exact(X) :- score(X, S), S = "75".
not_bob(X) :- score(X, _), X != "bob".
mid(X) :- score(X, S), S >= "40", S < "80".
?- high(X).
?- low(X).
?- exact(X).
?- not_bob(X).
?- mid(X).
EOF
OUTPUT=$(cd /tmp && "$KB" datalog "$CMP_DL" 2>&1)
assert_contains "$OUTPUT" "high(X)" "gt query ran"
assert_contains "$OUTPUT" "X = alice" "alice is high scorer"
assert_contains "$OUTPUT" "X = carol" "carol is high scorer"
assert_contains "$OUTPUT" "low(X)" "le query ran"
assert_contains "$OUTPUT" "X = dave" "dave is low scorer"
assert_contains "$OUTPUT" "exact(X)" "eq query ran"
assert_contains "$OUTPUT" "X = carol" "carol exact 75"
assert_contains "$OUTPUT" "not_bob(X)" "neq query ran"
assert_contains "$OUTPUT" "mid(X)" "range query ran"
assert_contains "$OUTPUT" "X = bob" "bob in mid range"
rm -f "$CMP_DL"

# Test 10: Combined demo (recursion + wildcards + negation + comparisons)
echo ""
echo "--- Test: Combined demo ---"
OUTPUT=$(cd /tmp && "$KB" datalog "$FIXTURES/demo.dl" 2>&1)
# High scorers: 20+ points
assert_contains "$OUTPUT" "high_scorer(P)" "high_scorer query ran"
assert_contains "$OUTPUT" "P = Alice" "Alice is high scorer"
assert_contains "$OUTPUT" "P = Grace" "Grace is high scorer"
assert_contains "$OUTPUT" "(4 results)" "4 high scorers"
# Mid scorers: 10-19 range
assert_contains "$OUTPUT" "mid_scorer(P)" "mid_scorer query ran"
assert_contains "$OUTPUT" "P = Bob" "Bob is mid scorer"
assert_contains "$OUTPUT" "P = Hank" "Hank is mid scorer"
# Dominance (recursion)
assert_contains "$OUTPUT" "dominates" "dominates query ran"
assert_contains "$OUTPUT" "X = Rockets" "Wolves dominate Rockets"
assert_contains "$OUTPUT" "(1 results)" "1 dominance result for Wolves"
# Negation: unbeaten teams
assert_contains "$OUTPUT" "unbeaten(X)" "unbeaten query ran"
assert_contains "$OUTPUT" "X = Falcons" "Falcons are unbeaten"
# Combined: top threats (high scorers on teams dominating Rockets)
assert_contains "$OUTPUT" "top_threat(P)" "top_threat query ran"
assert_contains "$OUTPUT" "P = Carol" "Carol is a top threat"
assert_contains "$OUTPUT" "P = Eve" "Eve is a top threat"
assert_contains "$OUTPUT" "(3 results)" "3 top threats"

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
