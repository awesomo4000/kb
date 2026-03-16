const std = @import("std");
const Allocator = std.mem.Allocator;
const ast = @import("ast.zig");

// Re-export AST types for backward compatibility
pub const Term = ast.Term;
pub const Atom = ast.Atom;
pub const BodyElement = ast.BodyElement;
pub const Rule = ast.Rule;
pub const Mapping = ast.Mapping;
pub const Binding = ast.Binding;
pub const CompOp = ast.CompOp;
pub const Comparison = ast.Comparison;

// =============================================================================
// Lexer
// =============================================================================

pub const TokenType = enum {
    identifier, // foo, Bar, _x
    string, // "hello"
    number, // 42, 443, 8080
    lparen, // (
    rparen, // )
    lbracket, // [
    rbracket, // ]
    comma, // ,
    dot, // .
    colon, // :
    equals, // =
    not_equal, // !=
    less_than, // <
    greater_than, // >
    less_eq, // <=
    greater_eq, // >=
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

        // Comparison operators: !=, <, <=, >, >=
        if (c == '!') {
            if (self.peek(1) == '=') {
                self.pos += 2;
                self.col += 2;
                return .{ .type = .not_equal, .text = "!=", .line = self.line, .col = start_col };
            }
            self.pos += 1;
            self.col += 1;
            return .{ .type = .err, .text = self.source[start..self.pos], .line = self.line, .col = start_col };
        }
        if (c == '<') {
            if (self.peek(1) == '=') {
                self.pos += 2;
                self.col += 2;
                return .{ .type = .less_eq, .text = "<=", .line = self.line, .col = start_col };
            }
            return self.advance(.less_than);
        }
        if (c == '>') {
            if (self.peek(1) == '=') {
                self.pos += 2;
                self.col += 2;
                return .{ .type = .greater_eq, .text = ">=", .line = self.line, .col = start_col };
            }
            return self.advance(.greater_than);
        }

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

        // Numeric literal
        if (c >= '0' and c <= '9') {
            return self.readNumber(start, start_col);
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

    fn readNumber(self: *Lexer, start: usize, start_col: usize) Token {
        while (self.pos < self.source.len and self.source[self.pos] >= '0' and self.source[self.pos] <= '9') {
            self.pos += 1;
            self.col += 1;
        }
        return .{ .type = .number, .text = self.source[start..self.pos], .line = self.line, .col = start_col };
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

    // Wildcard counter — reset per rule/query for fresh anonymous variable names
    anon_counter: u32 = 0,

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
                self.anon_counter = 0;
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
        self.anon_counter = 0;
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

    /// Parse a single body element: `not atom(...)`, `atom(...)`, or `term op term`.
    fn parseBodyElement(self: *Parser) ParseError!BodyElement {
        // Check for `not` contextual keyword
        if (self.current.type == .identifier and std.mem.eql(u8, self.current.text, "not")) {
            self.advance();
            const atom = try self.parseAtom();
            return .{ .negated_atom = atom };
        }

        // Check for comparison: term op term
        // A comparison starts with an identifier, string, or number, followed by a comparison operator.
        if (self.current.type == .identifier or self.current.type == .string or self.current.type == .number) {
            const peek_type = self.peekNextType();
            if (isComparisonOp(peek_type)) {
                const left = try self.parseTerm();
                const op = self.parseCompOp();
                const right = try self.parseTerm();
                return .{ .comparison = .{ .left = left, .op = op, .right = right } };
            }
        }

        const atom = try self.parseAtom();
        return .{ .atom = atom };
    }

    /// Peek at the next token type without consuming it.
    fn peekNextType(self: *Parser) TokenType {
        // Save lexer state
        const saved_pos = self.lexer.pos;
        const saved_line = self.lexer.line;
        const saved_col = self.lexer.col;

        // Advance lexer to get next token
        const next_tok = self.lexer.next();
        const result = next_tok.type;

        // Restore lexer state
        self.lexer.pos = saved_pos;
        self.lexer.line = saved_line;
        self.lexer.col = saved_col;

        return result;
    }

    fn isComparisonOp(tt: TokenType) bool {
        return switch (tt) {
            .equals, .not_equal, .less_than, .greater_than, .less_eq, .greater_eq => true,
            else => false,
        };
    }

    fn parseCompOp(self: *Parser) CompOp {
        const op: CompOp = switch (self.current.type) {
            .equals => .eq,
            .not_equal => .neq,
            .less_than => .lt,
            .greater_than => .gt,
            .less_eq => .le,
            .greater_eq => .ge,
            else => unreachable,
        };
        self.advance();
        return op;
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

        // Bare number literal → constant with the digit text
        if (self.current.type == .number) {
            const text = try self.allocator().dupe(u8, self.current.text);
            self.advance();
            return .{ .constant = text };
        }

        if (self.current.type == .identifier) {
            const text = self.current.text;

            // Wildcard: bare "_" becomes a fresh anonymous variable
            if (text.len == 1 and text[0] == '_') {
                const name = std.fmt.allocPrint(self.allocator(), "_#{}", .{self.anon_counter}) catch return error.OutOfMemory;
                self.anon_counter += 1;
                self.advance();
                return .{ .variable = name };
            }

            const duped = try self.allocator().dupe(u8, text);
            self.advance();

            // Variables start with uppercase
            if (duped.len > 0 and duped[0] >= 'A' and duped[0] <= 'Z') {
                return .{ .variable = duped };
            }
            // Constants start with lowercase
            return .{ .constant = duped };
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

test "parser wildcard desugaring" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator, "is_member(U) :- member_of(U, _).");
    defer parser.deinit();

    const result = try parser.parseProgram();

    try std.testing.expectEqual(@as(usize, 1), result.rules.len);
    const rule = result.rules[0];
    const body_atom = rule.body[0].atom;

    // Second term should be a variable (not constant), with generated name
    try std.testing.expect(body_atom.terms[1] == .variable);
    // Name contains # so it cannot collide with user variables
    try std.testing.expect(std.mem.indexOfScalar(u8, body_atom.terms[1].variable, '#') != null);
}

test "parser multiple wildcards are independent" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator, "cross(U) :- member_of(U, _), member_of(_, U).");
    defer parser.deinit();

    const result = try parser.parseProgram();

    const rule = result.rules[0];
    const wc1 = rule.body[0].atom.terms[1]; // first _
    const wc2 = rule.body[1].atom.terms[0]; // second _

    // Both are variables
    try std.testing.expect(wc1 == .variable);
    try std.testing.expect(wc2 == .variable);
    // They have different names
    try std.testing.expect(!std.mem.eql(u8, wc1.variable, wc2.variable));
}

test "parser wildcard counter resets per rule" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator,
        \\a(X) :- b(X, _).
        \\c(X) :- d(X, _).
    );
    defer parser.deinit();

    const result = try parser.parseProgram();

    // Both rules should have _#0 as their wildcard (counter resets)
    const wc1 = result.rules[0].body[0].atom.terms[1];
    const wc2 = result.rules[1].body[0].atom.terms[1];
    try std.testing.expectEqualStrings(wc1.variable, wc2.variable);
}

test "parser underscore prefix identifiers are not wildcards" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator, "foo(_bar).");
    defer parser.deinit();

    const result = try parser.parseProgram();

    // _bar starts with lowercase, so it is a constant -- not a wildcard
    const term = result.rules[0].head.terms[0];
    try std.testing.expect(term == .constant);
    try std.testing.expectEqualStrings("_bar", term.constant);
}

// =============================================================================
// Comparison operator tests
// =============================================================================

test "lexer comparison operators" {
    // Test all 6 operators
    var lexer = Lexer.init("= != < > <= >=");

    try std.testing.expectEqual(TokenType.equals, lexer.next().type);
    try std.testing.expectEqual(TokenType.not_equal, lexer.next().type);
    try std.testing.expectEqual(TokenType.less_than, lexer.next().type);
    try std.testing.expectEqual(TokenType.greater_than, lexer.next().type);
    try std.testing.expectEqual(TokenType.less_eq, lexer.next().type);
    try std.testing.expectEqual(TokenType.greater_eq, lexer.next().type);
    try std.testing.expectEqual(TokenType.eof, lexer.next().type);
}

test "lexer not_equal text" {
    var lexer = Lexer.init("!=");
    const tok = lexer.next();
    try std.testing.expectEqual(TokenType.not_equal, tok.type);
    try std.testing.expectEqualStrings("!=", tok.text);
}

test "lexer bare bang is error" {
    var lexer = Lexer.init("! ");
    const tok = lexer.next();
    try std.testing.expectEqual(TokenType.err, tok.type);
}

test "parser comparison in rule body" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator, "old(X) :- age(X, A), A > \"50\".");
    defer parser.deinit();

    const result = try parser.parseProgram();

    try std.testing.expectEqual(@as(usize, 1), result.rules.len);
    const rule = result.rules[0];
    try std.testing.expectEqual(@as(usize, 2), rule.body.len);

    // First body element: positive atom
    try std.testing.expect(rule.body[0] == .atom);
    try std.testing.expectEqualStrings("age", rule.body[0].atom.predicate);

    // Second body element: comparison
    try std.testing.expect(rule.body[1] == .comparison);
    const cmp = rule.body[1].comparison;
    try std.testing.expect(cmp.left == .variable);
    try std.testing.expectEqualStrings("A", cmp.left.variable);
    try std.testing.expectEqual(CompOp.gt, cmp.op);
    try std.testing.expect(cmp.right == .constant);
    try std.testing.expectEqualStrings("50", cmp.right.constant);
}

test "parser chained comparisons" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator, "range(X) :- val(X, V), V >= \"10\", V < \"20\".");
    defer parser.deinit();

    const result = try parser.parseProgram();

    try std.testing.expectEqual(@as(usize, 1), result.rules.len);
    const rule = result.rules[0];
    try std.testing.expectEqual(@as(usize, 3), rule.body.len);

    try std.testing.expect(rule.body[0] == .atom);
    try std.testing.expect(rule.body[1] == .comparison);
    try std.testing.expect(rule.body[2] == .comparison);

    try std.testing.expectEqual(CompOp.ge, rule.body[1].comparison.op);
    try std.testing.expectEqual(CompOp.lt, rule.body[2].comparison.op);
}

test "parser equality disambiguation" {
    // X = "foo" should parse as comparison, not as @map equals
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator, "match(X) :- val(X, V), V = \"hello\".");
    defer parser.deinit();

    const result = try parser.parseProgram();

    try std.testing.expectEqual(@as(usize, 1), result.rules.len);
    const rule = result.rules[0];
    try std.testing.expectEqual(@as(usize, 2), rule.body.len);

    try std.testing.expect(rule.body[1] == .comparison);
    try std.testing.expectEqual(CompOp.eq, rule.body[1].comparison.op);
}

test "parser not-equal comparison" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator, "diff(X, Y) :- pair(X, Y), X != Y.");
    defer parser.deinit();

    const result = try parser.parseProgram();

    try std.testing.expectEqual(@as(usize, 1), result.rules.len);
    const rule = result.rules[0];
    try std.testing.expectEqual(@as(usize, 2), rule.body.len);

    try std.testing.expect(rule.body[1] == .comparison);
    try std.testing.expectEqual(CompOp.neq, rule.body[1].comparison.op);
}

// =============================================================================
// Bare number literal tests
// =============================================================================

test "lexer number literal" {
    var lexer = Lexer.init("42");
    const tok = lexer.next();
    try std.testing.expectEqual(TokenType.number, tok.type);
    try std.testing.expectEqualStrings("42", tok.text);
}

test "lexer number in context" {
    var lexer = Lexer.init("points(X, 28).");
    try std.testing.expectEqual(TokenType.identifier, lexer.next().type); // points
    try std.testing.expectEqual(TokenType.lparen, lexer.next().type);
    try std.testing.expectEqual(TokenType.identifier, lexer.next().type); // X
    try std.testing.expectEqual(TokenType.comma, lexer.next().type);
    const num = lexer.next();
    try std.testing.expectEqual(TokenType.number, num.type);
    try std.testing.expectEqualStrings("28", num.text);
    try std.testing.expectEqual(TokenType.rparen, lexer.next().type);
    try std.testing.expectEqual(TokenType.dot, lexer.next().type);
}

test "parser bare number in fact" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator, "points(\"Alice\", 28).");
    defer parser.deinit();

    const result = try parser.parseProgram();

    try std.testing.expectEqual(@as(usize, 1), result.rules.len);
    const head = result.rules[0].head;
    try std.testing.expectEqual(@as(usize, 2), head.terms.len);
    try std.testing.expect(head.terms[1] == .constant);
    try std.testing.expectEqualStrings("28", head.terms[1].constant);
}

test "parser bare number in comparison" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator, "high(X) :- score(X, S), S > 20.");
    defer parser.deinit();

    const result = try parser.parseProgram();

    const rule = result.rules[0];
    try std.testing.expect(rule.body[1] == .comparison);
    const cmp = rule.body[1].comparison;
    try std.testing.expectEqual(CompOp.gt, cmp.op);
    try std.testing.expect(cmp.right == .constant);
    try std.testing.expectEqualStrings("20", cmp.right.constant);
}

test "parser bare number identical to quoted" {
    const allocator = std.testing.allocator;

    var parser1 = Parser.init(allocator, "score(\"Alice\", 28).");
    defer parser1.deinit();
    const r1 = try parser1.parseProgram();

    var parser2 = Parser.init(allocator, "score(\"Alice\", \"28\").");
    defer parser2.deinit();
    const r2 = try parser2.parseProgram();

    // Both produce the same constant term
    const t1 = r1.rules[0].head.terms[1];
    const t2 = r2.rules[0].head.terms[1];
    try std.testing.expect(t1 == .constant);
    try std.testing.expect(t2 == .constant);
    try std.testing.expectEqualStrings(t1.constant, t2.constant);
}

test "parser number on left side of comparison" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator, "check(X) :- val(X), 10 < X.");
    defer parser.deinit();

    const result = try parser.parseProgram();

    const rule = result.rules[0];
    try std.testing.expect(rule.body[1] == .comparison);
    const cmp = rule.body[1].comparison;
    try std.testing.expect(cmp.left == .constant);
    try std.testing.expectEqualStrings("10", cmp.left.constant);
    try std.testing.expectEqual(CompOp.lt, cmp.op);
    try std.testing.expect(cmp.right == .variable);
    try std.testing.expectEqualStrings("X", cmp.right.variable);
}
