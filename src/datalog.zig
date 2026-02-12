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
                const body = try self.parseBody();
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

    fn parseBody(self: *Parser) ParseError![]Atom {
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

// =============================================================================
// FactSource Interface - abstracts where base facts come from
// =============================================================================

pub const Binding = std.StringHashMap([]const u8);

pub const FactSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Match a pattern against facts, return variable bindings
        matchAtom: *const fn (ptr: *anyopaque, pattern: Atom, allocator: Allocator) Allocator.Error![]Binding,
    };

    pub fn matchAtom(self: FactSource, pattern: Atom, alloc: Allocator) Allocator.Error![]Binding {
        return self.vtable.matchAtom(self.ptr, pattern, alloc);
    }
};

/// In-memory fact storage - default implementation
pub const MemoryFactSource = struct {
    facts: std.ArrayList(Atom),
    alloc: Allocator,

    pub fn init(alloc: Allocator) MemoryFactSource {
        return .{ .facts = .{}, .alloc = alloc };
    }

    pub fn addFact(self: *MemoryFactSource, atom: Atom) !void {
        for (self.facts.items) |existing| {
            if (existing.eql(atom)) return;
        }
        const duped = try atom.dupe(self.alloc);
        try self.facts.append(self.alloc, duped);
    }

    pub fn source(self: *MemoryFactSource) FactSource {
        return .{
            .ptr = self,
            .vtable = &.{ .matchAtom = matchAtomImpl },
        };
    }

    fn matchAtomImpl(ptr: *anyopaque, pattern: Atom, alloc: Allocator) Allocator.Error![]Binding {
        const self: *MemoryFactSource = @ptrCast(@alignCast(ptr));
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
                            try binding.put(v, fact_val);
                        }
                    },
                }
            }

            if (matched) {
                try results.append(alloc, binding);
            }
        }

        return results.toOwnedSlice(alloc);
    }
};

// =============================================================================
// Evaluator (Bottom-up / Naive) - Uses arena for all allocations
// =============================================================================

/// Hash map context for Atom - enables O(1) fact deduplication
pub const AtomContext = struct {
    pub fn hash(_: AtomContext, atom: Atom) u64 {
        return atom.hash();
    }

    pub fn eql(_: AtomContext, a: Atom, b: Atom) bool {
        return a.eql(b);
    }
};

/// Per-rule profiling stats
pub const RuleProfile = struct {
    predicate: []const u8,
    total_time_ns: u64 = 0,
    bindings_generated: u64 = 0,
    facts_derived: u64 = 0,
    iterations: u64 = 0,
};

pub const Evaluator = struct {
    arena: std.heap.ArenaAllocator,
    base_facts: ?FactSource,           // External fact source (optional)
    derived_facts: std.ArrayList(Atom), // Facts derived by rules
    by_predicate: std.StringHashMapUnmanaged(std.ArrayListUnmanaged(Atom)), // Predicate index
    fact_set: std.HashMapUnmanaged(Atom, void, AtomContext, 80), // O(1) dedup
    // Semi-naive: delta facts from previous iteration
    delta_by_predicate: std.StringHashMapUnmanaged(std.ArrayListUnmanaged(Atom)),
    rules: []Rule,
    profiling_enabled: bool = false,
    rule_profiles: ?[]RuleProfile = null,

    /// Init with no external fact source (pure in-memory mode)
    pub fn init(backing_allocator: Allocator, rules: []Rule) Evaluator {
        return .{
            .arena = std.heap.ArenaAllocator.init(backing_allocator),
            .base_facts = null,
            .derived_facts = .{},
            .by_predicate = .{},
            .fact_set = .{},
            .delta_by_predicate = .{},
            .rules = rules,
        };
    }

    /// Init with external fact source (e.g., hypergraph)
    pub fn initWithSource(backing_allocator: Allocator, rules: []Rule, source: FactSource) Evaluator {
        return .{
            .arena = std.heap.ArenaAllocator.init(backing_allocator),
            .base_facts = source,
            .derived_facts = .{},
            .by_predicate = .{},
            .fact_set = .{},
            .delta_by_predicate = .{},
            .rules = rules,
        };
    }

    /// Enable profiling and allocate profile storage
    pub fn enableProfiling(self: *Evaluator) !void {
        self.profiling_enabled = true;
        const profiles = try self.allocator().alloc(RuleProfile, self.rules.len);
        for (profiles, self.rules) |*p, rule| {
            p.* = .{ .predicate = rule.head.predicate };
        }
        self.rule_profiles = profiles;
    }

    /// Print profiling results sorted by time
    pub fn printProfile(self: *Evaluator) void {
        const profiles = self.rule_profiles orelse return;

        // Sort by total time descending
        std.mem.sort(RuleProfile, profiles, {}, struct {
            fn cmp(_: void, a: RuleProfile, b: RuleProfile) bool {
                return a.total_time_ns > b.total_time_ns;
            }
        }.cmp);

        std.debug.print("\n=== Rule Profile (sorted by time) ===\n", .{});
        std.debug.print("{s:<40} {s:>12} {s:>12} {s:>10} {s:>8}\n", .{ "Predicate", "Time (ms)", "Bindings", "Facts", "Iters" });
        std.debug.print("{s:-<40} {s:->12} {s:->12} {s:->10} {s:->8}\n", .{ "", "", "", "", "" });

        for (profiles) |p| {
            if (p.total_time_ns == 0) continue;
            const time_ms = @as(f64, @floatFromInt(p.total_time_ns)) / 1_000_000.0;
            std.debug.print("{s:<40} {d:>12.2} {d:>12} {d:>10} {d:>8}\n", .{
                p.predicate,
                time_ms,
                p.bindings_generated,
                p.facts_derived,
                p.iterations,
            });
        }
    }

    pub fn deinit(self: *Evaluator) void {
        self.arena.deinit(); // Frees everything at once
    }

    fn allocator(self: *Evaluator) Allocator {
        return self.arena.allocator();
    }

    /// Add a fact to derived facts (for initial facts when no base_facts source)
    pub fn addFact(self: *Evaluator, atom: Atom) !void {
        const alloc = self.allocator();
        // O(1) check using hash set
        if (self.fact_set.contains(atom)) return;
        // Dupe into arena and add to all indexes
        const duped = try atom.dupe(alloc);
        try self.derived_facts.append(alloc, duped);
        try self.indexFact(duped);
    }

    /// Add a fact to the predicate index and hash set
    fn indexFact(self: *Evaluator, atom: Atom) !void {
        const alloc = self.allocator();
        // Add to predicate index
        const gop = try self.by_predicate.getOrPut(alloc, atom.predicate);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{};
        }
        try gop.value_ptr.append(alloc, atom);
        // Add to hash set for O(1) dedup
        try self.fact_set.put(alloc, atom, {});
    }

    /// Check if a fact exists in derived facts - O(1) hash lookup
    fn factExistsInDerived(self: *Evaluator, atom: Atom) bool {
        return self.fact_set.contains(atom);
    }

    /// Check if a fact exists (in derived or base)
    fn factExists(self: *Evaluator, atom: Atom) !bool {
        if (self.factExistsInDerived(atom)) return true;
        // Check base facts if we have a source
        if (self.base_facts) |source| {
            const matches = try source.matchAtom(atom, self.allocator());
            return matches.len > 0;
        }
        return false;
    }

    /// Run semi-naive fixed-point evaluation until no new facts are derived.
    /// Semi-naive only considers rule firings involving at least one delta fact.
    pub fn evaluate(self: *Evaluator) !void {
        var iteration: usize = 0;
        const max_iterations = 10000; // Safety limit

        // First pass: evaluate all rules naively to seed delta
        for (self.rules, 0..) |rule, rule_idx| {
            if (rule.body.len == 0) continue;

            const start_time = if (self.profiling_enabled) std.time.nanoTimestamp() else 0;
            const bindings = try self.matchBody(rule.body);

            var facts_this_rule: u64 = 0;
            for (bindings) |binding| {
                const new_fact = try self.substitute(rule.head, binding);
                if (!self.factExistsInDerived(new_fact)) {
                    try self.derived_facts.append(self.allocator(), new_fact);
                    try self.indexFact(new_fact);
                    try self.addToDelta(new_fact);
                    facts_this_rule += 1;
                }
            }

            if (self.profiling_enabled) {
                if (self.rule_profiles) |profiles| {
                    const elapsed = @as(u64, @intCast(std.time.nanoTimestamp() - start_time));
                    profiles[rule_idx].total_time_ns += elapsed;
                    profiles[rule_idx].bindings_generated += bindings.len;
                    profiles[rule_idx].facts_derived += facts_this_rule;
                    profiles[rule_idx].iterations += 1;
                }
            }
        }

        // Semi-naive iterations: only use delta facts
        while (self.hasDeltaFacts() and iteration < max_iterations) {
            iteration += 1;

            // Swap delta to "previous delta" for this iteration
            var prev_delta = self.delta_by_predicate;
            self.delta_by_predicate = .{};

            for (self.rules, 0..) |rule, rule_idx| {
                if (rule.body.len == 0) continue;

                const start_time = if (self.profiling_enabled) std.time.nanoTimestamp() else 0;
                var facts_this_rule: u64 = 0;
                var bindings_count: u64 = 0;

                // Semi-naive: for each body position, try using delta there
                // This ensures at least one atom matches a new fact
                for (0..rule.body.len) |delta_pos| {
                    const bindings = try self.matchBodySemiNaive(rule.body, &prev_delta, delta_pos);
                    bindings_count += bindings.len;

                    for (bindings) |binding| {
                        const new_fact = try self.substitute(rule.head, binding);
                        if (!self.factExistsInDerived(new_fact)) {
                            try self.derived_facts.append(self.allocator(), new_fact);
                            try self.indexFact(new_fact);
                            try self.addToDelta(new_fact);
                            facts_this_rule += 1;
                        }
                    }
                }

                if (self.profiling_enabled) {
                    if (self.rule_profiles) |profiles| {
                        const elapsed = @as(u64, @intCast(std.time.nanoTimestamp() - start_time));
                        profiles[rule_idx].total_time_ns += elapsed;
                        profiles[rule_idx].bindings_generated += bindings_count;
                        profiles[rule_idx].facts_derived += facts_this_rule;
                        profiles[rule_idx].iterations += 1;
                    }
                }
            }
        }
    }

    /// Add a fact to the delta index
    fn addToDelta(self: *Evaluator, atom: Atom) !void {
        const alloc = self.allocator();
        const gop = try self.delta_by_predicate.getOrPut(alloc, atom.predicate);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{};
        }
        try gop.value_ptr.append(alloc, atom);
    }

    /// Check if there are any delta facts
    fn hasDeltaFacts(self: *Evaluator) bool {
        var iter = self.delta_by_predicate.valueIterator();
        while (iter.next()) |list| {
            if (list.items.len > 0) return true;
        }
        return false;
    }

    /// Match body with semi-naive strategy: position delta_pos uses delta facts,
    /// earlier positions use all facts, later positions use all facts.
    fn matchBodySemiNaive(
        self: *Evaluator,
        body: []Atom,
        prev_delta: *std.StringHashMapUnmanaged(std.ArrayListUnmanaged(Atom)),
        delta_pos: usize,
    ) ![]Binding {
        const alloc = self.allocator();

        if (body.len == 0) {
            var result: std.ArrayList(Binding) = .{};
            try result.append(alloc, Binding.init(alloc));
            return result.toOwnedSlice(alloc);
        }

        // Check if delta has any facts for the delta position's predicate
        const delta_pred = body[delta_pos].predicate;
        if (prev_delta.get(delta_pred) == null) {
            // No delta facts for this predicate, skip this variant
            return &[_]Binding{};
        }

        // Start with first atom
        var bindings = if (delta_pos == 0)
            try self.matchAtomInDelta(body[0], prev_delta)
        else
            try self.matchAtom(body[0]);

        // Join with remaining atoms
        for (body[1..], 1..) |atom, pos| {
            var new_bindings: std.ArrayList(Binding) = .{};

            for (bindings) |binding| {
                const substituted = try self.substituteAtom(atom, binding);
                const matches = if (pos == delta_pos)
                    try self.matchAtomInDelta(substituted, prev_delta)
                else
                    try self.matchAtom(substituted);

                for (matches) |match| {
                    if (try self.mergeBindings(binding, match)) |m| {
                        try new_bindings.append(alloc, m);
                    }
                }
            }
            bindings = try new_bindings.toOwnedSlice(alloc);
        }

        return bindings;
    }

    /// Match atom only against delta facts (no base facts)
    fn matchAtomInDelta(
        self: *Evaluator,
        pattern: Atom,
        delta: *std.StringHashMapUnmanaged(std.ArrayListUnmanaged(Atom)),
    ) ![]Binding {
        var results: std.ArrayList(Binding) = .{};

        if (delta.get(pattern.predicate)) |facts_for_pred| {
            try self.matchAtomAgainstList(pattern, facts_for_pred.items, &results);
        }

        return results.toOwnedSlice(self.allocator());
    }

    /// Query the fact set with a pattern. Results valid until Evaluator.deinit()
    pub fn query(self: *Evaluator, pattern: Atom) ![]Binding {
        return self.matchAtom(pattern);
    }

    /// Match pattern against both derived facts and base facts
    fn matchAtom(self: *Evaluator, pattern: Atom) ![]Binding {
        const alloc = self.allocator();
        var results: std.ArrayList(Binding) = .{};

        // Match against derived facts using predicate index (O(1) lookup + O(n) within predicate)
        if (self.by_predicate.get(pattern.predicate)) |facts_for_pred| {
            try self.matchAtomAgainstList(pattern, facts_for_pred.items, &results);
        }

        // Match against base facts if we have a source
        if (self.base_facts) |source| {
            const base_results = try source.matchAtom(pattern, alloc);
            for (base_results) |binding| {
                try results.append(alloc, binding);
            }
        }

        return results.toOwnedSlice(alloc);
    }

    fn matchAtomAgainstList(self: *Evaluator, pattern: Atom, facts: []const Atom, results: *std.ArrayList(Binding)) !void {
        const alloc = self.allocator();

        for (facts) |fact| {
            // Predicate already matched by index lookup, just check arity
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
                            try binding.put(v, fact_val);
                        }
                    },
                }
            }

            if (matched) {
                try results.append(alloc, binding);
            }
        }
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
    try std.testing.expectEqualStrings("parent", rule.body[0].predicate);
    try std.testing.expectEqualStrings("parent", rule.body[1].predicate);
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

test "evaluator simple query" {
    const allocator = std.testing.allocator;

    var parser = Parser.init(allocator,
        \\parent("tom", "bob").
        \\parent("bob", "jim").
    );
    defer parser.deinit();

    const result = try parser.parseProgram();

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
    defer parser.deinit();

    const result = try parser.parseProgram();

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
    defer parser.deinit();

    const result = try parser.parseProgram();

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
    defer parser.deinit();

    const result = try parser.parseProgram();

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
