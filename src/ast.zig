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

pub const CompOp = enum {
    eq,
    neq,
    lt,
    gt,
    le,
    ge,

    pub fn format(self: CompOp, writer: anytype) !void {
        try writer.writeAll(switch (self) {
            .eq => "=",
            .neq => "!=",
            .lt => "<",
            .gt => ">",
            .le => "<=",
            .ge => ">=",
        });
    }
};

pub const Comparison = struct {
    left: Term,
    op: CompOp,
    right: Term,
};

pub const BodyElement = union(enum) {
    atom: Atom,
    negated_atom: Atom,
    comparison: Comparison,

    pub fn getAtom(self: BodyElement) ?Atom {
        return switch (self) {
            .atom => |a| a,
            .negated_atom => |a| a,
            .comparison => null,
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
            .comparison => |cmp| {
                try writer.print("{f}", .{cmp.left});
                try writer.writeAll(" ");
                try writer.print("{f}", .{cmp.op});
                try writer.writeAll(" ");
                try writer.print("{f}", .{cmp.right});
            },
        }
    }

    pub fn dupe(self: BodyElement, allocator: Allocator) !BodyElement {
        return switch (self) {
            .atom => |a| .{ .atom = try a.dupe(allocator) },
            .negated_atom => |a| .{ .negated_atom = try a.dupe(allocator) },
            .comparison => |cmp| .{ .comparison = .{
                .left = try cmp.left.dupe(allocator),
                .op = cmp.op,
                .right = try cmp.right.dupe(allocator),
            } },
        };
    }

    pub fn free(self: BodyElement, allocator: Allocator) void {
        switch (self) {
            .atom => |a| a.free(allocator),
            .negated_atom => |a| a.free(allocator),
            .comparison => |cmp| {
                cmp.left.free(allocator);
                cmp.right.free(allocator);
            },
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

pub const Binding = std.StringHashMap([]const u8);
