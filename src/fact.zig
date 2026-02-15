const std = @import("std");

/// An entity is a typed value: "host:10.2.3.12", "port:8080", etc.
pub const Entity = struct {
    type: []const u8,
    id: []const u8,

    pub fn eql(self: Entity, other: Entity) bool {
        return std.mem.eql(u8, self.type, other.type) and std.mem.eql(u8, self.id, other.id);
    }

    /// Create "type\x00id" key for indexing
    pub fn toKey(self: Entity, allocator: std.mem.Allocator) ![]u8 {
        const key = try allocator.alloc(u8, self.type.len + 1 + self.id.len);
        @memcpy(key[0..self.type.len], self.type);
        key[self.type.len] = 0;
        @memcpy(key[self.type.len + 1 ..], self.id);
        return key;
    }
};

/// A fact is a hyperedge connecting multiple entities.
/// Facts are immutable and append-only.
pub const Fact = struct {
    id: u64,
    entities: []const Entity,
    source: ?[]const u8,

    const Self = @This();

    /// Serialize fact to bytes (pure, no I/O)
    pub fn serialize(self: Self, buf: []u8) ![]u8 {
        var fbs = std.io.fixedBufferStream(buf);
        const writer = fbs.writer();

        try writer.writeInt(u64, self.id, .big);
        try writer.writeInt(u32, @intCast(self.entities.len), .big);

        for (self.entities) |entity| {
            try writer.writeInt(u16, @intCast(entity.type.len), .big);
            try writer.writeAll(entity.type);
            try writer.writeInt(u16, @intCast(entity.id.len), .big);
            try writer.writeAll(entity.id);
        }

        if (self.source) |s| {
            try writer.writeInt(u16, @intCast(s.len), .big);
            try writer.writeAll(s);
        } else {
            try writer.writeInt(u16, 0, .big);
        }

        return fbs.getWritten();
    }

    /// Deserialize fact from bytes (pure, no I/O)
    pub fn deserialize(data: []const u8, allocator: std.mem.Allocator) !Self {
        var fbs = std.io.fixedBufferStream(data);
        const reader = fbs.reader();

        const id = try reader.readInt(u64, .big);
        const entity_count = try reader.readInt(u32, .big);

        // Sanity check: entity count can't exceed remaining bytes
        // Each entity needs at least 4 bytes (2x u16 length fields)
        const remaining = data.len - fbs.pos;
        if (entity_count > remaining / 4) return error.InvalidData;

        var entities = try allocator.alloc(Entity, entity_count);
        errdefer allocator.free(entities);

        for (0..entity_count) |i| {
            const type_len = try reader.readInt(u16, .big);
            const type_buf = try allocator.alloc(u8, type_len);
            errdefer allocator.free(type_buf);
            _ = try reader.readAll(type_buf);

            const id_len = try reader.readInt(u16, .big);
            const id_buf = try allocator.alloc(u8, id_len);
            errdefer allocator.free(id_buf);
            _ = try reader.readAll(id_buf);

            entities[i] = .{ .type = type_buf, .id = id_buf };
        }

        const source_len = try reader.readInt(u16, .big);
        const source = if (source_len > 0) blk: {
            const src_buf = try allocator.alloc(u8, source_len);
            _ = try reader.readAll(src_buf);
            break :blk src_buf;
        } else null;

        return Self{
            .id = id,
            .entities = entities,
            .source = source,
        };
    }

    /// Clone a fact, allocating new memory for all data.
    pub fn clone(self: Self, allocator: std.mem.Allocator) !Self {
        const entities = try allocator.alloc(Entity, self.entities.len);
        errdefer allocator.free(entities);

        for (self.entities, 0..) |entity, i| {
            const type_buf = try allocator.dupe(u8, entity.type);
            errdefer allocator.free(type_buf);
            const id_buf = try allocator.dupe(u8, entity.id);
            entities[i] = .{
                .type = type_buf,
                .id = id_buf,
            };
        }

        return .{
            .id = self.id,
            .entities = entities,
            .source = if (self.source) |s| try allocator.dupe(u8, s) else null,
        };
    }

    /// Free memory allocated by deserialize
    pub fn deinit(self: Self, allocator: std.mem.Allocator) void {
        for (self.entities) |entity| {
            allocator.free(@constCast(entity.type));
            allocator.free(@constCast(entity.id));
        }
        allocator.free(self.entities);
        if (self.source) |s| {
            allocator.free(@constCast(s));
        }
    }
};

test "entity equality" {
    const a = Entity{ .type = "book", .id = "odyssey" };
    const b = Entity{ .type = "book", .id = "odyssey" };
    const c = Entity{ .type = "book", .id = "iliad" };
    try std.testing.expect(a.eql(b));
    try std.testing.expect(!a.eql(c));
}

test "entity toKey" {
    const allocator = std.testing.allocator;
    const entity = Entity{ .type = "author", .id = "homer" };
    const key = try entity.toKey(allocator);
    defer allocator.free(key);
    try std.testing.expectEqualStrings("author\x00homer", key);
}

test "fact serialize/deserialize roundtrip" {
    const allocator = std.testing.allocator;

    const original = Fact{
        .id = 42,
        .entities = &.{
            .{ .type = "book", .id = "odyssey" },
            .{ .type = "author", .id = "homer" },
        },
        .source = "test-source",
    };

    var buf: [4096]u8 = undefined;
    const serialized = try original.serialize(&buf);

    const restored = try Fact.deserialize(serialized, allocator);
    defer restored.deinit(allocator);

    try std.testing.expectEqual(original.id, restored.id);
    try std.testing.expectEqual(original.entities.len, restored.entities.len);
    try std.testing.expectEqualStrings(original.entities[0].type, restored.entities[0].type);
    try std.testing.expectEqualStrings(original.entities[0].id, restored.entities[0].id);
    try std.testing.expectEqualStrings(original.entities[1].type, restored.entities[1].type);
    try std.testing.expectEqualStrings(original.entities[1].id, restored.entities[1].id);
    try std.testing.expectEqualStrings(original.source.?, restored.source.?);
}

test "fact serialize/deserialize with null source" {
    const allocator = std.testing.allocator;

    const original = Fact{
        .id = 1,
        .entities = &.{
            .{ .type = "rel", .id = "test" },
        },
        .source = null,
    };

    var buf: [4096]u8 = undefined;
    const serialized = try original.serialize(&buf);

    const restored = try Fact.deserialize(serialized, allocator);
    defer restored.deinit(allocator);

    try std.testing.expectEqual(original.id, restored.id);
    try std.testing.expect(restored.source == null);
}
