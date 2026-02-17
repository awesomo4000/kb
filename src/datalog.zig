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

    pub fn format(self: Term, writer: anytype) !void {
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

    /// Hash function for use in hash sets/maps
    pub fn hash(self: Atom) u64 {
        var h = std.hash.Wyhash.init(0);
        h.update(self.predicate);
        h.update(&[_]u8{0}); // separator
        for (self.terms) |term| {
            switch (term) {
                .variable => |v| {
                    h.update(&[_]u8{'v'});
                    h.update(v);
                },
                .constant => |c| {
                    h.update(&[_]u8{'c'});
                    h.update(c);
                },
            }
            h.update(&[_]u8{0}); // separator
        }
        return h.final();
    }

    pub fn format(self: Atom, writer: anytype) !void {
        try writer.print("{s}(", .{self.predicate});
        for (self.terms, 0..) |term, i| {
            if (i > 0) try writer.writeAll(", ");
            try writer.print("{f}", .{term});
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

pub const BodyElement = union(enum) {
    atom: Atom,
    negated_atom: Atom,

    pub fn getAtom(self: BodyElement) Atom {
        return switch (self) {
            .atom => |a| a,
            .negated_atom => |a| a,
        };
    }

    pub fn isNegated(self: BodyElement) bool {
        return self == .negated_atom;
    }

    pub fn format(self: BodyElement, writer: anytype) !void {
        switch (self) {
            .atom => |a| try writer.print("{f}", .{a}),
            .negated_atom => |a| {
                try writer.writeAll("not ");
                try writer.print("{f}", .{a});
            },
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

pub const Rule = struct {
    head: Atom,
    body: []BodyElement,

    pub fn format(self: Rule, writer: anytype) !void {
        try writer.print("{f}", .{self.head});
        if (self.body.len > 0) {
            try writer.writeAll(" :- ");
            for (self.body, 0..) |elem, i| {
                if (i > 0) try writer.writeAll(", ");
                try writer.print("{f}", .{elem});
            }
        }
        try writer.writeAll(".");
    }

    pub fn dupe(self: Rule, allocator: Allocator) !Rule {
        const body = try allocator.alloc(BodyElement, self.body.len);
        for (self.body, 0..) |elem, i| {
            body[i] = try elem.dupe(allocator);
        }
        return .{
            .head = try self.head.dupe(allocator),
            .body = body,
        };
    }

    pub fn free(self: Rule, allocator: Allocator) void {
        self.head.free(allocator);
        for (self.body) |elem| elem.free(allocator);
        allocator.free(self.body);
    }
};

/// Mapping from Datalog predicate to hypergraph pattern
/// @map influenced(A, B) = [rel:influenced-by, author:A, author:B].
pub const Mapping = struct {
    predicate: []const u8,
    args: []const []const u8, // Variable names in order
    pattern: []const PatternElement,

    pub const PatternElement = struct {
        entity_type: []const u8, // e.g., "rel", "author"
        value: union(enum) {
            constant: []const u8, // e.g., "influenced-by"
            variable: []const u8, // e.g., "A" - references an arg
        },
    };

    pub fn free(self: Mapping, allocator: Allocator) void {
        allocator.free(self.predicate);
        for (self.args) |a| allocator.free(a);
        allocator.free(self.args);
        for (self.pattern) |p| {
            allocator.free(p.entity_type);
            switch (p.value) {
                .constant => |c| allocator.free(c),
                .variable => |v| allocator.free(v),
            }
        }
        allocator.free(self.pattern);
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
    lbracket, // [
    rbracket, // ]
    comma, // ,
    dot, // .
    colon, // :
    equals, // =
    at, // @
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
        if (c == '[') return self.advance(.lbracket);
        if (c == ']') return self.advance(.rbracket);
        if (c == ',') return self.advance(.comma);
        if (c == '.') return self.advance(.dot);
        if (c == '=') return self.advance(.equals);
        if (c == '@') return self.advance(.at);

        // :- turnstile or just :
        if (c == ':') {
            if (self.peek(1) == '-') {
                self.pos += 2;
                self.col += 2;
                return .{ .type = .turnstile, .text = ":-", .line = self.line, .col = start_col };
            }
            return self.advance(.colon);
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
    ExpectedLBracket,
    ExpectedRBracket,
    ExpectedColon,
    ExpectedEquals,
    ExpectedCommaOrRParen,
    ExpectedDotOrTurnstile,
    OutOfMemory,
};

pub const Parser = struct {
    lexer: Lexer,
    current: Token,
    arena: std.heap.ArenaAllocator,
    source: []const u8,

    // Error context - populated when parse fails
    error_line: usize = 0,
    error_col: usize = 0,
    error_msg: []const u8 = "",

    pub fn init(backing_allocator: Allocator, source: []const u8) Parser {
        var lexer = Lexer.init(source);
        const first_token = lexer.next();
        return .{
            .lexer = lexer,
            .current = first_token,
            .arena = std.heap.ArenaAllocator.init(backing_allocator),
            .source = source,
        };
    }

    /// Free all memory allocated during parsing.
    /// ParseResult is invalid after this call.
    pub fn deinit(self: *Parser) void {
        self.arena.deinit();
    }

    fn allocator(self: *Parser) Allocator {
        return self.arena.allocator();
    }

    /// Get a nice error message with source context
    pub fn formatError(self: *const Parser, writer: anytype) !void {
        if (self.error_line == 0) return;

        try writer.print("error: {s} at line {}, col {}\n", .{
            self.error_msg, self.error_line, self.error_col
        });

        // Find the line in source
        var line_start: usize = 0;
        var current_line: usize = 1;
        for (self.source, 0..) |c, i| {
            if (current_line == self.error_line) {
                line_start = i;
                break;
            }
            if (c == '\n') current_line += 1;
        }

        // Print the line
        var line_end = line_start;
        while (line_end < self.source.len and self.source[line_end] != '\n') {
            line_end += 1;
        }
        try writer.print("  {s}\n", .{self.source[line_start..line_end]});

        // Print caret (spaces + ^)
        for (0..self.error_col + 1) |_| {
            try writer.writeByte(' ');
        }
        try writer.writeAll("^\n");
    }

    fn setError(self: *Parser, msg: []const u8) void {
        self.error_line = self.current.line;
        self.error_col = self.current.col;
        self.error_msg = msg;
    }

    /// Get error info as a struct (for programmatic access)
    pub const ErrorInfo = struct {
        line: usize,
        col: usize,
        msg: []const u8,
    };

    pub fn getLastError(self: *const Parser) ?ErrorInfo {
        if (self.error_line == 0) return null;
        return .{
            .line = self.error_line,
            .col = self.error_col,
            .msg = self.error_msg,
        };
    }

    pub const ParseResult = struct {
        rules: []Rule,
        queries: [][]Atom,
        mappings: []Mapping,
        includes: [][]const u8,
    };

    pub fn parseProgram(self: *Parser) !ParseResult {
        var rules: std.ArrayList(Rule) = .{};
        var queries: std.ArrayList([]Atom) = .{};
        var mappings: std.ArrayList(Mapping) = .{};
        var includes: std.ArrayList([]const u8) = .{};

        while (self.current.type != .eof) {
            if (self.current.type == .at) {
                // Directive: @map or @include
                self.advance();
                if (self.current.type != .identifier) {
                    self.setError("expected directive name after @");
                    return error.ExpectedIdentifier;
                }
                if (std.mem.eql(u8, self.current.text, "map")) {
                    self.advance();
                    const mapping = try self.parseMapping();
                    try mappings.append(self.allocator(), mapping);
                } else if (std.mem.eql(u8, self.current.text, "include")) {
                    self.advance();
                    if (self.current.type != .string) {
                        self.setError("expected string path after @include");
                        return error.ExpectedString;
                    }
                    const path = try self.allocator().dupe(u8, self.current.text);
                    try includes.append(self.allocator(), path);
                    self.advance();
                    try self.expect(.dot);
                } else {
                    self.setError("unknown directive (expected 'map' or 'include')");
                    return error.UnexpectedToken;
                }
            } else if (self.current.type == .query) {
                // Query: ?- body.
                self.advance();
                const body = try self.parseQueryBody();
                try self.expect(.dot);
                try queries.append(self.allocator(), body);
            } else {
                // Fact or rule
                const rule = try self.parseRule();
                try rules.append(self.allocator(), rule);
            }
        }

        return .{
            .rules = try rules.toOwnedSlice(self.allocator()),
            .queries = try queries.toOwnedSlice(self.allocator()),
            .mappings = try mappings.toOwnedSlice(self.allocator()),
            .includes = try includes.toOwnedSlice(self.allocator()),
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

        self.setError("expected '.' or ':-' after atom");
        return error.ExpectedDotOrTurnstile;
    }

    /// Parse a rule body as []BodyElement (supports `not` for negation).
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

    /// Parse a single body element: either `not atom(...)` or `atom(...)`.
    fn parseBodyElement(self: *Parser) ParseError!BodyElement {
        // Check for `not` contextual keyword
        if (self.current.type == .identifier and std.mem.eql(u8, self.current.text, "not")) {
            self.advance();
            const atom = try self.parseAtom();
            return .{ .negated_atom = atom };
        }
        const atom = try self.parseAtom();
        return .{ .atom = atom };
    }

    /// Parse a query body as []Atom (no negation support, used for `?-` lines).
    fn parseQueryBody(self: *Parser) ParseError![]Atom {
        var atoms: std.ArrayList(Atom) = .{};

        const first = try self.parseAtom();
        try atoms.append(self.allocator(), first);

        while (self.current.type == .comma) {
            self.advance();
            const atom = try self.parseAtom();
            try atoms.append(self.allocator(), atom);
        }

        return atoms.toOwnedSlice(self.allocator());
    }

    fn parseAtom(self: *Parser) ParseError!Atom {
        if (self.current.type != .identifier) {
            self.setError("expected predicate name");
            return error.ExpectedIdentifier;
        }

        const predicate = try self.allocator().dupe(u8, self.current.text);
        self.advance();

        try self.expect(.lparen);

        var terms: std.ArrayList(Term) = .{};

        if (self.current.type != .rparen) {
            const first = try self.parseTerm();
            try terms.append(self.allocator(), first);

            while (self.current.type == .comma) {
                self.advance();
                const term = try self.parseTerm();
                try terms.append(self.allocator(), term);
            }
        }

        try self.expect(.rparen);

        return .{
            .predicate = predicate,
            .terms = try terms.toOwnedSlice(self.allocator()),
        };
    }

    fn parseTerm(self: *Parser) ParseError!Term {
        if (self.current.type == .string) {
            const text = try self.allocator().dupe(u8, self.current.text);
            self.advance();
            return .{ .constant = text };
        }

        if (self.current.type == .identifier) {
            const text = try self.allocator().dupe(u8, self.current.text);
            self.advance();

            // Variables start with uppercase
            if (text.len > 0 and text[0] >= 'A' and text[0] <= 'Z') {
                return .{ .variable = text };
            }
            // Constants start with lowercase
            return .{ .constant = text };
        }

        self.setError("expected variable, constant, or string");
        return error.UnexpectedToken;
    }

    /// Parse: predicate(A, B) = [type:val, type:Var].
    fn parseMapping(self: *Parser) ParseError!Mapping {
        // Parse predicate name
        if (self.current.type != .identifier) {
            self.setError("expected predicate name in @map");
            return error.ExpectedIdentifier;
        }
        const predicate = try self.allocator().dupe(u8, self.current.text);
        self.advance();

        // Parse argument list
        try self.expect(.lparen);
        var args: std.ArrayList([]const u8) = .{};

        if (self.current.type != .rparen) {
            // First argument (must be variable - uppercase)
            if (self.current.type != .identifier) {
                self.setError("expected argument name in @map");
                return error.ExpectedIdentifier;
            }
            try args.append(self.allocator(), try self.allocator().dupe(u8, self.current.text));
            self.advance();

            // Additional arguments
            while (self.current.type == .comma) {
                self.advance();
                if (self.current.type != .identifier) {
                    self.setError("expected argument name after ','");
                    return error.ExpectedIdentifier;
                }
                try args.append(self.allocator(), try self.allocator().dupe(u8, self.current.text));
                self.advance();
            }
        }
        try self.expect(.rparen);

        // Parse =
        if (self.current.type != .equals) {
            self.setError("expected '=' after @map predicate");
            return error.ExpectedEquals;
        }
        self.advance();

        // Parse pattern: [type:val, type:Var, ...]
        if (self.current.type != .lbracket) {
            self.setError("expected '[' to start pattern");
            return error.ExpectedLBracket;
        }
        self.advance();

        var pattern: std.ArrayList(Mapping.PatternElement) = .{};

        if (self.current.type != .rbracket) {
            // First element
            const elem = try self.parsePatternElement();
            try pattern.append(self.allocator(), elem);

            // Additional elements
            while (self.current.type == .comma) {
                self.advance();
                const next_elem = try self.parsePatternElement();
                try pattern.append(self.allocator(), next_elem);
            }
        }

        if (self.current.type != .rbracket) {
            self.setError("expected ']' to close pattern");
            return error.ExpectedRBracket;
        }
        self.advance();

        // Expect trailing dot
        try self.expect(.dot);

        return .{
            .predicate = predicate,
            .args = try args.toOwnedSlice(self.allocator()),
            .pattern = try pattern.toOwnedSlice(self.allocator()),
        };
    }

    /// Parse: type:value or type:Variable
    fn parsePatternElement(self: *Parser) ParseError!Mapping.PatternElement {
        // Entity type
        if (self.current.type != .identifier) {
            self.setError("expected entity type in pattern");
            return error.ExpectedIdentifier;
        }
        const entity_type = try self.allocator().dupe(u8, self.current.text);
        self.advance();

        // Colon separator
        if (self.current.type != .colon) {
            self.setError("expected ':' after entity type");
            return error.ExpectedColon;
        }
        self.advance();

        // Value: either identifier or string
        if (self.current.type == .string) {
            // Quoted string -> constant
            const val = try self.allocator().dupe(u8, self.current.text);
            self.advance();
            return .{
                .entity_type = entity_type,
                .value = .{ .constant = val },
            };
        } else if (self.current.type == .identifier) {
            const text = self.current.text;
            const duped = try self.allocator().dupe(u8, text);
            self.advance();

            // Uppercase -> variable reference, lowercase -> constant
            if (text.len > 0 and text[0] >= 'A' and text[0] <= 'Z') {
                return .{
                    .entity_type = entity_type,
                    .value = .{ .variable = duped },
                };
            } else {
                return .{
                    .entity_type = entity_type,
                    .value = .{ .constant = duped },
                };
            }
        }

        return error.UnexpectedToken;
    }

    fn expect(self: *Parser, expected: TokenType) ParseError!void {
        if (self.current.type != expected) {
            const msg = switch (expected) {
                .lparen => "expected '('",
                .rparen => "expected ')'",
                .lbracket => "expected '['",
                .rbracket => "expected ']'",
                .dot => "expected '.'",
                .comma => "expected ','",
                .colon => "expected ':'",
                .equals => "expected '='",
                else => "unexpected token",
            };
            self.setError(msg);
            return switch (expected) {
                .lparen => error.ExpectedLParen,
                .rparen => error.ExpectedRParen,
                .lbracket => error.ExpectedLBracket,
                .rbracket => error.ExpectedRBracket,
                else => error.UnexpectedToken,
            };
        }
        self.advance();
    }

    fn advance(self: *Parser) void {
        self.current = self.lexer.next();
    }
};

/// Variable bindings from query results: variable name -> string value
pub const Binding = std.StringHashMap([]const u8);

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

test "lexer @map tokens" {
    var lexer = Lexer.init("@map foo(A) = [rel:bar, x:A].");

    try std.testing.expectEqual(TokenType.at, lexer.next().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.next().type); // map
    try std.testing.expectEqual(TokenType.identifier, lexer.next().type); // foo
    try std.testing.expectEqual(TokenType.lparen, lexer.next().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.next().type); // A
    try std.testing.expectEqual(TokenType.rparen, lexer.next().type);
    try std.testing.expectEqual(TokenType.equals, lexer.next().type);
    try std.testing.expectEqual(TokenType.lbracket, lexer.next().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.next().type); // rel
    try std.testing.expectEqual(TokenType.colon, lexer.next().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.next().type); // bar
    try std.testing.expectEqual(TokenType.comma, lexer.next().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.next().type); // x
    try std.testing.expectEqual(TokenType.colon, lexer.next().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.next().type); // A
    try std.testing.expectEqual(TokenType.rbracket, lexer.next().type);
    try std.testing.expectEqual(TokenType.dot, lexer.next().type);
    try std.testing.expectEqual(TokenType.eof, lexer.next().type);
}

test "parser simple fact" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator, "parent(tom, bob).");
    defer parser.deinit();

    const result = try parser.parseProgram();

    try std.testing.expectEqual(@as(usize, 1), result.rules.len);
    try std.testing.expectEqualStrings("parent", result.rules[0].head.predicate);
    try std.testing.expectEqual(@as(usize, 2), result.rules[0].head.terms.len);
    try std.testing.expectEqual(@as(usize, 0), result.rules[0].body.len);
}

test "parser rule with body" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator, "grandparent(X, Z) :- parent(X, Y), parent(Y, Z).");
    defer parser.deinit();

    const result = try parser.parseProgram();

    try std.testing.expectEqual(@as(usize, 1), result.rules.len);
    const rule = result.rules[0];
    try std.testing.expectEqualStrings("grandparent", rule.head.predicate);
    try std.testing.expectEqual(@as(usize, 2), rule.body.len);
    try std.testing.expectEqualStrings("parent", rule.body[0].atom.predicate);
    try std.testing.expectEqualStrings("parent", rule.body[1].atom.predicate);
}

test "parser query" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator, "?- foo(X).");
    defer parser.deinit();

    const result = try parser.parseProgram();

    try std.testing.expectEqual(@as(usize, 0), result.rules.len);
    try std.testing.expectEqual(@as(usize, 1), result.queries.len);
    try std.testing.expectEqualStrings("foo", result.queries[0][0].predicate);
}

test "parser @map directive" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator,
        \\% Map Datalog predicates to hypergraph patterns
        \\@map influenced(A, B) = [rel:influenced, author:A, author:B].
        \\@map wrote(Author, Book) = [rel:wrote, author:Author, book:Book].
        \\
        \\% Rules using the mapped predicates
        \\influenced_t(A, B) :- influenced(A, B).
    );
    defer parser.deinit();

    const result = try parser.parseProgram();

    // Should have 2 mappings and 1 rule
    try std.testing.expectEqual(@as(usize, 2), result.mappings.len);
    try std.testing.expectEqual(@as(usize, 1), result.rules.len);

    // Check first mapping: influenced(A, B) = [rel:influenced, author:A, author:B]
    const m1 = result.mappings[0];
    try std.testing.expectEqualStrings("influenced", m1.predicate);
    try std.testing.expectEqual(@as(usize, 2), m1.args.len);
    try std.testing.expectEqualStrings("A", m1.args[0]);
    try std.testing.expectEqualStrings("B", m1.args[1]);
    try std.testing.expectEqual(@as(usize, 3), m1.pattern.len);

    // Check pattern elements
    try std.testing.expectEqualStrings("rel", m1.pattern[0].entity_type);
    try std.testing.expectEqualStrings("influenced", m1.pattern[0].value.constant);

    try std.testing.expectEqualStrings("author", m1.pattern[1].entity_type);
    try std.testing.expectEqualStrings("A", m1.pattern[1].value.variable);

    try std.testing.expectEqualStrings("author", m1.pattern[2].entity_type);
    try std.testing.expectEqualStrings("B", m1.pattern[2].value.variable);

    // Check second mapping
    const m2 = result.mappings[1];
    try std.testing.expectEqualStrings("wrote", m2.predicate);
    try std.testing.expectEqual(@as(usize, 2), m2.args.len);
}

test "parser error formatting" {
    const allocator = std.testing.allocator;

    // Invalid syntax - missing closing paren
    var parser = Parser.init(allocator,
        \\foo(X, Y.
        \\bar(Z).
    );
    defer parser.deinit(); // Arena cleans up partial allocations on error

    const result = parser.parseProgram();
    try std.testing.expectError(error.ExpectedRParen, result);

    // Check error info
    const err_info = parser.getLastError().?;
    try std.testing.expectEqual(@as(usize, 1), err_info.line);
    try std.testing.expectEqualStrings("expected ')'", err_info.msg);

    // Print the formatted error to a buffer
    std.debug.print("\n--- Error formatting demo ---\n", .{});
    var buf: [512]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    try parser.formatError(fbs.writer());
    std.debug.print("{s}", .{fbs.getWritten()});
    std.debug.print("-----------------------------\n", .{});
}

test "parser negation in rule body" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator, "non_epic(A) :- author(A), not epic_author(A).");
    defer parser.deinit();

    const result = try parser.parseProgram();

    try std.testing.expectEqual(@as(usize, 1), result.rules.len);
    const rule = result.rules[0];
    try std.testing.expectEqualStrings("non_epic", rule.head.predicate);
    try std.testing.expectEqual(@as(usize, 2), rule.body.len);

    // First body element: positive atom
    try std.testing.expect(rule.body[0] == .atom);
    try std.testing.expectEqualStrings("author", rule.body[0].atom.predicate);

    // Second body element: negated atom
    try std.testing.expect(rule.body[1] == .negated_atom);
    try std.testing.expectEqualStrings("epic_author", rule.body[1].negated_atom.predicate);
    try std.testing.expect(rule.body[1].isNegated());
    try std.testing.expect(!rule.body[0].isNegated());
}

test "parser multiple negations in rule body" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator, "special(A) :- author(A), not epic(A), not modern(A).");
    defer parser.deinit();

    const result = try parser.parseProgram();

    try std.testing.expectEqual(@as(usize, 1), result.rules.len);
    const rule = result.rules[0];
    try std.testing.expectEqual(@as(usize, 3), rule.body.len);

    try std.testing.expect(rule.body[0] == .atom);
    try std.testing.expectEqualStrings("author", rule.body[0].atom.predicate);

    try std.testing.expect(rule.body[1] == .negated_atom);
    try std.testing.expectEqualStrings("epic", rule.body[1].negated_atom.predicate);

    try std.testing.expect(rule.body[2] == .negated_atom);
    try std.testing.expectEqualStrings("modern", rule.body[2].negated_atom.predicate);
}
