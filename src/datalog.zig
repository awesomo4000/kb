const std = @import("std");
const Allocator = std.mem.Allocator;

// =============================================================================
// AST Types
// =============================================================================

pub const Term = union(enum) {
    variable: []const u8, // Starts with uppercase
    constant: []const u8, // Starts with lowercase or is quoted

    pub fn eql(self: Term, other: Term) bool {
        return switch (self) {
            .variable => |v| switch (other) {
                .variable => |ov| std.mem.eql(u8, v, ov),
                .constant => false,
            },
            .constant => |c| switch (other) {
                .constant => |oc| std.mem.eql(u8, c, oc),
                .variable => false,
            },
        };
    }

    pub fn format(self: Term, comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
        switch (self) {
            .variable => |v| try writer.print("{s}", .{v}),
            .constant => |c| try writer.print("\"{s}\"", .{c}),
        }
    }

    pub fn dupe(self: Term, allocator: Allocator) !Term {
        return switch (self) {
            .variable => |v| .{ .variable = try allocator.dupe(u8, v) },
            .constant => |c| .{ .constant = try allocator.dupe(u8, c) },
        };
    }

    pub fn free(self: Term, allocator: Allocator) void {
        switch (self) {
            .variable => |v| allocator.free(v),
            .constant => |c| allocator.free(c),
        }
    }
};

pub const Atom = struct {
    predicate: []const u8,
    terms: []Term,

    pub fn eql(self: Atom, other: Atom) bool {
        if (!std.mem.eql(u8, self.predicate, other.predicate)) return false;
        if (self.terms.len != other.terms.len) return false;
        for (self.terms, other.terms) |a, b| {
            if (!a.eql(b)) return false;
        }
        return true;
    }

    pub fn format(self: Atom, comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
        try writer.print("{s}(", .{self.predicate});
        for (self.terms, 0..) |term, i| {
            if (i > 0) try writer.writeAll(", ");
            try writer.print("{}", .{term});
        }
        try writer.writeAll(")");
    }

    pub fn dupe(self: Atom, allocator: Allocator) !Atom {
        const terms = try allocator.alloc(Term, self.terms.len);
        for (self.terms, 0..) |t, i| {
            terms[i] = try t.dupe(allocator);
        }
        return .{
            .predicate = try allocator.dupe(u8, self.predicate),
            .terms = terms,
        };
    }

    pub fn free(self: Atom, allocator: Allocator) void {
        for (self.terms) |t| t.free(allocator);
        allocator.free(self.terms);
        allocator.free(self.predicate);
    }
};

pub const Rule = struct {
    head: Atom,
    body: []Atom,

    pub fn format(self: Rule, comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
        try writer.print("{}", .{self.head});
        if (self.body.len > 0) {
            try writer.writeAll(" :- ");
            for (self.body, 0..) |atom, i| {
                if (i > 0) try writer.writeAll(", ");
                try writer.print("{}", .{atom});
            }
        }
        try writer.writeAll(".");
    }

    pub fn dupe(self: Rule, allocator: Allocator) !Rule {
        const body = try allocator.alloc(Atom, self.body.len);
        for (self.body, 0..) |a, i| {
            body[i] = try a.dupe(allocator);
        }
        return .{
            .head = try self.head.dupe(allocator),
            .body = body,
        };
    }

    pub fn free(self: Rule, allocator: Allocator) void {
        self.head.free(allocator);
        for (self.body) |a| a.free(allocator);
        allocator.free(self.body);
    }
};

// =============================================================================
// Lexer
// =============================================================================

pub const TokenType = enum {
    identifier, // foo, Bar, _x
    string, // "hello"
    lparen, // (
    rparen, // )
    comma, // ,
    dot, // .
    turnstile, // :-
    query, // ?-
    eof,
    err,
};

pub const Token = struct {
    type: TokenType,
    text: []const u8,
    line: usize,
    col: usize,
};

pub const Lexer = struct {
    source: []const u8,
    pos: usize = 0,
    line: usize = 1,
    col: usize = 1,

    pub fn init(source: []const u8) Lexer {
        return .{ .source = source };
    }

    pub fn next(self: *Lexer) Token {
        self.skipWhitespaceAndComments();

        if (self.pos >= self.source.len) {
            return self.makeToken(.eof, "");
        }

        const start = self.pos;
        const start_col = self.col;
        const c = self.source[self.pos];

        // Single character tokens
        if (c == '(') return self.advance(.lparen);
        if (c == ')') return self.advance(.rparen);
        if (c == ',') return self.advance(.comma);
        if (c == '.') return self.advance(.dot);

        // :- turnstile
        if (c == ':' and self.peek(1) == '-') {
            self.pos += 2;
            self.col += 2;
            return .{ .type = .turnstile, .text = ":-", .line = self.line, .col = start_col };
        }

        // ?- query
        if (c == '?' and self.peek(1) == '-') {
            self.pos += 2;
            self.col += 2;
            return .{ .type = .query, .text = "?-", .line = self.line, .col = start_col };
        }

        // String literal
        if (c == '"') {
            return self.readString();
        }

        // Identifier (including variables)
        if (isIdentStart(c)) {
            return self.readIdentifier(start, start_col);
        }

        // Unknown character
        self.pos += 1;
        self.col += 1;
        return .{ .type = .err, .text = self.source[start..self.pos], .line = self.line, .col = start_col };
    }

    fn advance(self: *Lexer, token_type: TokenType) Token {
        const tok = Token{
            .type = token_type,
            .text = self.source[self.pos .. self.pos + 1],
            .line = self.line,
            .col = self.col,
        };
        self.pos += 1;
        self.col += 1;
        return tok;
    }

    fn makeToken(self: *Lexer, token_type: TokenType, text: []const u8) Token {
        return .{ .type = token_type, .text = text, .line = self.line, .col = self.col };
    }

    fn peek(self: *Lexer, offset: usize) u8 {
        const idx = self.pos + offset;
        if (idx >= self.source.len) return 0;
        return self.source[idx];
    }

    fn readString(self: *Lexer) Token {
        const start_col = self.col;
        const start = self.pos;
        self.pos += 1; // skip opening quote
        self.col += 1;

        while (self.pos < self.source.len and self.source[self.pos] != '"') {
            if (self.source[self.pos] == '\n') {
                self.line += 1;
                self.col = 1;
            } else {
                self.col += 1;
            }
            self.pos += 1;
        }

        if (self.pos >= self.source.len) {
            return .{ .type = .err, .text = "unterminated string", .line = self.line, .col = start_col };
        }

        self.pos += 1; // skip closing quote
        self.col += 1;

        // Return text without quotes
        return .{ .type = .string, .text = self.source[start + 1 .. self.pos - 1], .line = self.line, .col = start_col };
    }

    fn readIdentifier(self: *Lexer, start: usize, start_col: usize) Token {
        while (self.pos < self.source.len and isIdentChar(self.source[self.pos])) {
            self.pos += 1;
            self.col += 1;
        }
        return .{ .type = .identifier, .text = self.source[start..self.pos], .line = self.line, .col = start_col };
    }

    fn skipWhitespaceAndComments(self: *Lexer) void {
        while (self.pos < self.source.len) {
            const c = self.source[self.pos];
            if (c == ' ' or c == '\t' or c == '\r') {
                self.pos += 1;
                self.col += 1;
            } else if (c == '\n') {
                self.pos += 1;
                self.line += 1;
                self.col = 1;
            } else if (c == '%') {
                // Line comment
                while (self.pos < self.source.len and self.source[self.pos] != '\n') {
                    self.pos += 1;
                }
            } else {
                break;
            }
        }
    }

    fn isIdentStart(c: u8) bool {
        return (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or c == '_';
    }

    fn isIdentChar(c: u8) bool {
        return isIdentStart(c) or (c >= '0' and c <= '9');
    }
};

// =============================================================================
// Parser
// =============================================================================

pub const ParseError = error{
    UnexpectedToken,
    ExpectedIdentifier,
    ExpectedLParen,
    ExpectedRParen,
    ExpectedCommaOrRParen,
    ExpectedDotOrTurnstile,
    OutOfMemory,
};

pub const Parser = struct {
    lexer: Lexer,
    current: Token,
    allocator: Allocator,

    pub fn init(allocator: Allocator, source: []const u8) Parser {
        var lexer = Lexer.init(source);
        const first_token = lexer.next();
        return .{
            .lexer = lexer,
            .current = first_token,
            .allocator = allocator,
        };
    }

    pub fn parseProgram(self: *Parser) !struct { rules: []Rule, queries: [][]Atom } {
        var rules: std.ArrayList(Rule) = .{};
        var queries: std.ArrayList([]Atom) = .{};

        while (self.current.type != .eof) {
            if (self.current.type == .query) {
                // Query: ?- body.
                self.advance();
                const body = try self.parseBody();
                try self.expect(.dot);
                try queries.append(self.allocator, body);
            } else {
                // Fact or rule
                const rule = try self.parseRule();
                try rules.append(self.allocator, rule);
            }
        }

        return .{
            .rules = try rules.toOwnedSlice(self.allocator),
            .queries = try queries.toOwnedSlice(self.allocator),
        };
    }

    pub fn parseRule(self: *Parser) ParseError!Rule {
        const head = try self.parseAtom();

        if (self.current.type == .dot) {
            // Fact (rule with empty body)
            self.advance();
            return .{ .head = head, .body = &.{} };
        }

        if (self.current.type == .turnstile) {
            self.advance();
            const body = try self.parseBody();
            try self.expect(.dot);
            return .{ .head = head, .body = body };
        }

        return error.ExpectedDotOrTurnstile;
    }

    fn parseBody(self: *Parser) ParseError![]Atom {
        var atoms: std.ArrayList(Atom) = .{};

        const first = try self.parseAtom();
        try atoms.append(self.allocator, first);

        while (self.current.type == .comma) {
            self.advance();
            const atom = try self.parseAtom();
            try atoms.append(self.allocator, atom);
        }

        return atoms.toOwnedSlice(self.allocator);
    }

    fn parseAtom(self: *Parser) ParseError!Atom {
        if (self.current.type != .identifier) {
            return error.ExpectedIdentifier;
        }

        const predicate = try self.allocator.dupe(u8, self.current.text);
        self.advance();

        try self.expect(.lparen);

        var terms: std.ArrayList(Term) = .{};

        if (self.current.type != .rparen) {
            const first = try self.parseTerm();
            try terms.append(self.allocator, first);

            while (self.current.type == .comma) {
                self.advance();
                const term = try self.parseTerm();
                try terms.append(self.allocator, term);
            }
        }

        try self.expect(.rparen);

        return .{
            .predicate = predicate,
            .terms = try terms.toOwnedSlice(self.allocator),
        };
    }

    fn parseTerm(self: *Parser) ParseError!Term {
        if (self.current.type == .string) {
            const text = try self.allocator.dupe(u8, self.current.text);
            self.advance();
            return .{ .constant = text };
        }

        if (self.current.type == .identifier) {
            const text = try self.allocator.dupe(u8, self.current.text);
            self.advance();

            // Variables start with uppercase
            if (text.len > 0 and text[0] >= 'A' and text[0] <= 'Z') {
                return .{ .variable = text };
            }
            // Constants start with lowercase
            return .{ .constant = text };
        }

        return error.UnexpectedToken;
    }

    fn expect(self: *Parser, expected: TokenType) ParseError!void {
        if (self.current.type != expected) {
            return switch (expected) {
                .lparen => error.ExpectedLParen,
                .rparen => error.ExpectedRParen,
                else => error.UnexpectedToken,
            };
        }
        self.advance();
    }

    fn advance(self: *Parser) void {
        self.current = self.lexer.next();
    }
};

// =============================================================================
// Evaluator (Bottom-up / Naive) - Uses arena for all allocations
// =============================================================================

pub const Binding = std.StringHashMap([]const u8);

pub const Evaluator = struct {
    arena: std.heap.ArenaAllocator,
    facts: std.ArrayList(Atom),
    rules: []Rule,

    pub fn init(backing_allocator: Allocator, rules: []Rule) Evaluator {
        return .{
            .arena = std.heap.ArenaAllocator.init(backing_allocator),
            .facts = .{},
            .rules = rules,
        };
    }

    pub fn deinit(self: *Evaluator) void {
        self.arena.deinit(); // Frees everything at once
    }

    fn allocator(self: *Evaluator) Allocator {
        return self.arena.allocator();
    }

    pub fn addFact(self: *Evaluator, atom: Atom) !void {
        // Check if already exists
        for (self.facts.items) |existing| {
            if (existing.eql(atom)) return;
        }
        // Dupe into arena
        const duped = try atom.dupe(self.allocator());
        try self.facts.append(self.allocator(), duped);
    }

    /// Run fixed-point evaluation until no new facts are derived
    pub fn evaluate(self: *Evaluator) !void {
        var changed = true;
        var iteration: usize = 0;
        const max_iterations = 10000; // Safety limit

        while (changed and iteration < max_iterations) {
            changed = false;
            iteration += 1;

            for (self.rules) |rule| {
                if (rule.body.len == 0) continue; // Skip facts

                // Find all bindings that satisfy the body
                const bindings = try self.matchBody(rule.body);

                // For each binding, instantiate the head
                for (bindings) |binding| {
                    const new_fact = try self.substitute(rule.head, binding);
                    // Check if exists, add if not
                    var exists = false;
                    for (self.facts.items) |existing| {
                        if (existing.eql(new_fact)) {
                            exists = true;
                            break;
                        }
                    }
                    if (!exists) {
                        try self.facts.append(self.allocator(), new_fact);
                        changed = true;
                    }
                    // No need to free - arena owns it
                }
            }
        }
    }

    /// Query the fact set with a pattern. Results valid until Evaluator.deinit()
    pub fn query(self: *Evaluator, pattern: Atom) ![]Binding {
        return self.matchAtom(pattern);
    }

    fn matchAtom(self: *Evaluator, pattern: Atom) ![]Binding {
        const alloc = self.allocator();
        var results: std.ArrayList(Binding) = .{};

        for (self.facts.items) |fact| {
            if (!std.mem.eql(u8, fact.predicate, pattern.predicate)) continue;
            if (fact.terms.len != pattern.terms.len) continue;

            var binding = Binding.init(alloc);
            var matched = true;

            for (fact.terms, pattern.terms) |fact_term, pattern_term| {
                switch (pattern_term) {
                    .constant => |c| {
                        switch (fact_term) {
                            .constant => |fc| {
                                if (!std.mem.eql(u8, c, fc)) {
                                    matched = false;
                                    break;
                                }
                            },
                            .variable => {
                                matched = false;
                                break;
                            },
                        }
                    },
                    .variable => |v| {
                        const fact_val = switch (fact_term) {
                            .constant => |c| c,
                            .variable => {
                                matched = false;
                                break;
                            },
                        };

                        if (binding.get(v)) |existing| {
                            if (!std.mem.eql(u8, existing, fact_val)) {
                                matched = false;
                                break;
                            }
                        } else {
                            // Just store - arena owns everything
                            try binding.put(v, fact_val);
                        }
                    },
                }
            }

            if (matched) {
                try results.append(alloc, binding);
            }
            // No cleanup needed on mismatch - arena
        }

        return results.toOwnedSlice(alloc);
    }

    fn matchBody(self: *Evaluator, body: []Atom) ![]Binding {
        const alloc = self.allocator();

        if (body.len == 0) {
            var result: std.ArrayList(Binding) = .{};
            try result.append(alloc, Binding.init(alloc));
            return result.toOwnedSlice(alloc);
        }

        // Start with first atom
        var bindings = try self.matchAtom(body[0]);

        // Join with remaining atoms
        for (body[1..]) |atom| {
            var new_bindings: std.ArrayList(Binding) = .{};

            for (bindings) |binding| {
                const substituted = try self.substituteAtom(atom, binding);
                const matches = try self.matchAtom(substituted);

                for (matches) |match| {
                    if (try self.mergeBindings(binding, match)) |m| {
                        try new_bindings.append(alloc, m);
                    }
                }
            }
            // No cleanup - arena
            bindings = try new_bindings.toOwnedSlice(alloc);
        }

        return bindings;
    }

    fn substituteAtom(self: *Evaluator, atom: Atom, binding: Binding) !Atom {
        const alloc = self.allocator();
        const terms = try alloc.alloc(Term, atom.terms.len);
        for (atom.terms, 0..) |term, i| {
            terms[i] = switch (term) {
                .variable => |v| if (binding.get(v)) |val|
                    .{ .constant = try alloc.dupe(u8, val) }
                else
                    .{ .variable = try alloc.dupe(u8, v) },
                .constant => |c| .{ .constant = try alloc.dupe(u8, c) },
            };
        }
        return .{
            .predicate = try alloc.dupe(u8, atom.predicate),
            .terms = terms,
        };
    }

    fn substitute(self: *Evaluator, atom: Atom, binding: Binding) !Atom {
        const alloc = self.allocator();
        const terms = try alloc.alloc(Term, atom.terms.len);
        for (atom.terms, 0..) |term, i| {
            terms[i] = switch (term) {
                .variable => |v| blk: {
                    if (binding.get(v)) |val| {
                        break :blk .{ .constant = try alloc.dupe(u8, val) };
                    }
                    break :blk .{ .variable = try alloc.dupe(u8, v) };
                },
                .constant => |c| .{ .constant = try alloc.dupe(u8, c) },
            };
        }
        return .{
            .predicate = try alloc.dupe(u8, atom.predicate),
            .terms = terms,
        };
    }

    fn mergeBindings(self: *Evaluator, a: Binding, b: Binding) !?Binding {
        const alloc = self.allocator();
        var result = Binding.init(alloc);

        // Copy from a - just copy pointers, arena owns the strings
        var iter_a = a.iterator();
        while (iter_a.next()) |entry| {
            try result.put(entry.key_ptr.*, entry.value_ptr.*);
        }

        // Merge from b, checking consistency
        var iter_b = b.iterator();
        while (iter_b.next()) |entry| {
            if (result.get(entry.key_ptr.*)) |existing| {
                if (!std.mem.eql(u8, existing, entry.value_ptr.*)) {
                    return null; // Inconsistent - no cleanup needed, arena
                }
            } else {
                try result.put(entry.key_ptr.*, entry.value_ptr.*);
            }
        }

        return result;
    }
};

// =============================================================================
// Tests
// =============================================================================

test "lexer basic tokens" {
    var lexer = Lexer.init("foo(X, Y).");

    try std.testing.expectEqual(TokenType.identifier, lexer.next().type);
    try std.testing.expectEqual(TokenType.lparen, lexer.next().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.next().type);
    try std.testing.expectEqual(TokenType.comma, lexer.next().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.next().type);
    try std.testing.expectEqual(TokenType.rparen, lexer.next().type);
    try std.testing.expectEqual(TokenType.dot, lexer.next().type);
    try std.testing.expectEqual(TokenType.eof, lexer.next().type);
}

test "lexer strings" {
    var lexer = Lexer.init("foo(\"hello world\").");

    const id = lexer.next();
    try std.testing.expectEqual(TokenType.identifier, id.type);
    try std.testing.expectEqualStrings("foo", id.text);

    _ = lexer.next(); // lparen

    const str = lexer.next();
    try std.testing.expectEqual(TokenType.string, str.type);
    try std.testing.expectEqualStrings("hello world", str.text);
}

test "lexer turnstile and query" {
    var lexer = Lexer.init("foo(X) :- bar(X). ?- foo(Y).");

    _ = lexer.next(); // foo
    _ = lexer.next(); // (
    _ = lexer.next(); // X
    _ = lexer.next(); // )
    try std.testing.expectEqual(TokenType.turnstile, lexer.next().type);
    _ = lexer.next(); // bar
    _ = lexer.next(); // (
    _ = lexer.next(); // X
    _ = lexer.next(); // )
    _ = lexer.next(); // .
    try std.testing.expectEqual(TokenType.query, lexer.next().type);
}

test "lexer comments" {
    var lexer = Lexer.init(
        \\% This is a comment
        \\foo(X).
    );

    const tok = lexer.next();
    try std.testing.expectEqual(TokenType.identifier, tok.type);
    try std.testing.expectEqualStrings("foo", tok.text);
}

test "parser simple fact" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator, "parent(tom, bob).");

    const result = try parser.parseProgram();
    defer {
        for (result.rules) |r| r.free(allocator);
        allocator.free(result.rules);
        allocator.free(result.queries);
    }

    try std.testing.expectEqual(@as(usize, 1), result.rules.len);
    try std.testing.expectEqualStrings("parent", result.rules[0].head.predicate);
    try std.testing.expectEqual(@as(usize, 2), result.rules[0].head.terms.len);
    try std.testing.expectEqual(@as(usize, 0), result.rules[0].body.len);
}

test "parser rule with body" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator, "grandparent(X, Z) :- parent(X, Y), parent(Y, Z).");

    const result = try parser.parseProgram();
    defer {
        for (result.rules) |r| r.free(allocator);
        allocator.free(result.rules);
        allocator.free(result.queries);
    }

    try std.testing.expectEqual(@as(usize, 1), result.rules.len);
    const rule = result.rules[0];
    try std.testing.expectEqualStrings("grandparent", rule.head.predicate);
    try std.testing.expectEqual(@as(usize, 2), rule.body.len);
    try std.testing.expectEqualStrings("parent", rule.body[0].predicate);
    try std.testing.expectEqualStrings("parent", rule.body[1].predicate);
}

test "parser query" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator, "?- foo(X).");

    const result = try parser.parseProgram();
    defer {
        for (result.queries) |q| {
            for (q) |a| a.free(allocator);
            allocator.free(q);
        }
        allocator.free(result.rules);
        allocator.free(result.queries);
    }

    try std.testing.expectEqual(@as(usize, 0), result.rules.len);
    try std.testing.expectEqual(@as(usize, 1), result.queries.len);
    try std.testing.expectEqualStrings("foo", result.queries[0][0].predicate);
}

test "evaluator simple query" {
    const allocator = std.testing.allocator;

    var parser = Parser.init(allocator,
        \\parent("tom", "bob").
        \\parent("bob", "jim").
    );

    const result = try parser.parseProgram();
    defer {
        for (result.rules) |r| r.free(allocator);
        allocator.free(result.rules);
        allocator.free(result.queries);
    }

    var eval = Evaluator.init(allocator, result.rules);
    defer eval.deinit();

    // Add facts
    for (result.rules) |rule| {
        if (rule.body.len == 0) {
            try eval.addFact(rule.head);
        }
    }

    // Query
    var query_terms = [_]Term{ .{ .variable = "X" }, .{ .constant = "bob" } };
    const query_atom = Atom{
        .predicate = "parent",
        .terms = &query_terms,
    };

    const results = try eval.query(query_atom);
    // No manual cleanup - arena handles it via eval.deinit()

    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqualStrings("tom", results[0].get("X").?);
}

test "evaluator transitive closure" {
    const allocator = std.testing.allocator;

    var parser = Parser.init(allocator,
        \\edge("a", "b").
        \\edge("b", "c").
        \\edge("c", "d").
        \\reachable(X, Y) :- edge(X, Y).
        \\reachable(X, Z) :- edge(X, Y), reachable(Y, Z).
    );

    const result = try parser.parseProgram();
    defer {
        for (result.rules) |r| r.free(allocator);
        allocator.free(result.rules);
        allocator.free(result.queries);
    }

    var eval = Evaluator.init(allocator, result.rules);
    defer eval.deinit();

    // Add facts
    for (result.rules) |rule| {
        if (rule.body.len == 0) {
            try eval.addFact(rule.head);
        }
    }

    // Run fixed-point evaluation
    try eval.evaluate();

    // Query: reachable("a", X)
    var query_terms = [_]Term{ .{ .constant = "a" }, .{ .variable = "X" } };
    const query_atom = Atom{
        .predicate = "reachable",
        .terms = &query_terms,
    };

    const results = try eval.query(query_atom);
    // No manual cleanup - arena handles it

    // Should reach b, c, d
    try std.testing.expectEqual(@as(usize, 3), results.len);
}

test "evaluator member_of transitive" {
    const allocator = std.testing.allocator;

    var parser = Parser.init(allocator,
        \\member_of("alice", "developers").
        \\member_of("developers", "employees").
        \\member_of("employees", "domain_users").
        \\member_of_t(X, G) :- member_of(X, G).
        \\member_of_t(X, G) :- member_of(X, M), member_of_t(M, G).
    );

    const result = try parser.parseProgram();
    defer {
        for (result.rules) |r| r.free(allocator);
        allocator.free(result.rules);
        allocator.free(result.queries);
    }

    var eval = Evaluator.init(allocator, result.rules);
    defer eval.deinit();

    for (result.rules) |rule| {
        if (rule.body.len == 0) {
            try eval.addFact(rule.head);
        }
    }

    try eval.evaluate();

    // Query: member_of_t("alice", G)
    var query_terms = [_]Term{ .{ .constant = "alice" }, .{ .variable = "G" } };
    const query_atom = Atom{
        .predicate = "member_of_t",
        .terms = &query_terms,
    };

    const results = try eval.query(query_atom);
    // No manual cleanup - arena handles it

    // Alice should be in developers, employees, domain_users
    try std.testing.expectEqual(@as(usize, 3), results.len);
}

test "books and literary influence" {
    const allocator = std.testing.allocator;

    // Facts about books and authors
    var parser = Parser.init(allocator,
        \\% Books and their authors
        \\wrote("Homer", "The Odyssey").
        \\wrote("Homer", "The Iliad").
        \\wrote("Virgil", "The Aeneid").
        \\wrote("Dante", "Divine Comedy").
        \\wrote("Milton", "Paradise Lost").
        \\wrote("Plato", "The Republic").
        \\
        \\% Genre classifications
        \\genre("The Odyssey", "epic").
        \\genre("The Iliad", "epic").
        \\genre("The Aeneid", "epic").
        \\genre("Divine Comedy", "epic").
        \\genre("Paradise Lost", "epic").
        \\genre("The Republic", "philosophy").
        \\
        \\% Direct influence relationships
        \\influenced("Homer", "Virgil").
        \\influenced("Virgil", "Dante").
        \\influenced("Virgil", "Milton").
        \\influenced("Dante", "Milton").
        \\
        \\% Transitive influence - who shaped whom across generations
        \\influenced_t(A, B) :- influenced(A, B).
        \\influenced_t(A, C) :- influenced(A, B), influenced_t(B, C).
        \\
        \\% All books by authors who were influenced (directly or indirectly) by X
        \\books_in_tradition(Book, Root) :-
        \\    influenced_t(Root, Author), wrote(Author, Book).
    );

    const result = try parser.parseProgram();
    defer {
        for (result.rules) |r| r.free(allocator);
        allocator.free(result.rules);
        allocator.free(result.queries);
    }

    var eval = Evaluator.init(allocator, result.rules);
    defer eval.deinit();

    // Add facts
    for (result.rules) |rule| {
        if (rule.body.len == 0) {
            try eval.addFact(rule.head);
        }
    }

    try eval.evaluate();

    // Query: Who did Homer influence (transitively)?
    std.debug.print("\n?- influenced_t(\"Homer\", Who).\n", .{});
    var q1_terms = [_]Term{ .{ .constant = "Homer" }, .{ .variable = "Who" } };
    const q1 = try eval.query(.{ .predicate = "influenced_t", .terms = &q1_terms });
    for (q1) |b| std.debug.print("   Who = {s}\n", .{b.get("Who").?});

    // Homer -> Virgil -> Dante, Milton. So Homer influenced Virgil, Dante, Milton
    try std.testing.expectEqual(@as(usize, 3), q1.len);

    // Query: What books are in Homer's literary tradition?
    std.debug.print("\n?- books_in_tradition(Book, \"Homer\").\n", .{});
    var q2_terms = [_]Term{ .{ .variable = "Book" }, .{ .constant = "Homer" } };
    const q2 = try eval.query(.{ .predicate = "books_in_tradition", .terms = &q2_terms });
    for (q2) |b| std.debug.print("   Book = {s}\n", .{b.get("Book").?});

    // Virgil wrote Aeneid, Dante wrote Divine Comedy, Milton wrote Paradise Lost
    try std.testing.expectEqual(@as(usize, 3), q2.len);

    // Query: Who influenced Milton (reverse lookup)?
    std.debug.print("\n?- influenced_t(Who, \"Milton\").\n", .{});
    var q3_terms = [_]Term{ .{ .variable = "Who" }, .{ .constant = "Milton" } };
    const q3 = try eval.query(.{ .predicate = "influenced_t", .terms = &q3_terms });
    for (q3) |b| std.debug.print("   Who = {s}\n", .{b.get("Who").?});

    // Virgil -> Milton, Dante -> Milton, Homer -> Virgil -> Milton
    try std.testing.expectEqual(@as(usize, 3), q3.len);
}
