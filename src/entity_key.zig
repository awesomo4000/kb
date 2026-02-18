const std = @import("std");

/// Value type tag — one byte in the encoded key, placed after the \x00 separator.
pub const ValueType = enum(u8) {
    string = 0x01,
    u16_val = 0x02,
    u32_val = 0x03,
    i64_val = 0x04,
    u64_val = 0x05,
    uuid = 0x06,
    ipv4 = 0x07,
    ipv6 = 0x08,
    bytes = 0x09,

    /// Number of bytes the encoded value occupies (excluding the type tag).
    /// Returns null for variable-length types (string, bytes) — these
    /// consume everything to the end of the key.
    pub fn fixedLen(self: ValueType) ?usize {
        return switch (self) {
            .u16_val => 2,
            .u32_val, .ipv4 => 4,
            .i64_val, .u64_val => 8,
            .uuid, .ipv6 => 16,
            .string, .bytes => null,
        };
    }
};

/// A typed entity value.
pub const Value = union(enum) {
    string: []const u8,
    u16_val: u16,
    u32_val: u32,
    i64_val: i64,
    u64_val: u64,
    uuid: [16]u8,
    ipv4: u32,
    ipv6: [16]u8,
    bytes: []const u8,

    /// Returns the ValueType tag for this value.
    pub fn valueType(self: Value) ValueType {
        return switch (self) {
            .string => .string,
            .u16_val => .u16_val,
            .u32_val => .u32_val,
            .i64_val => .i64_val,
            .u64_val => .u64_val,
            .uuid => .uuid,
            .ipv4 => .ipv4,
            .ipv6 => .ipv6,
            .bytes => .bytes,
        };
    }
};

/// An encoded entity key. Bytes are borrowed — they reference the buffer
/// passed to encode() or an LMDB value slice passed to fromBytes().
pub const EntityKey = struct {
    raw: []const u8,

    /// Decode the entity type portion (bytes before the \x00 separator).
    pub fn entityType(self: EntityKey) []const u8 {
        const sep = std.mem.indexOfScalar(u8, self.raw, 0).?;
        return self.raw[0..sep];
    }

    /// Decode the value portion (after the \x00 separator + type tag).
    pub fn value(self: EntityKey) Value {
        const sep = std.mem.indexOfScalar(u8, self.raw, 0).?;
        const type_tag: ValueType = @enumFromInt(self.raw[sep + 1]);
        const value_bytes = self.raw[sep + 2 ..];

        return switch (type_tag) {
            .string => .{ .string = value_bytes },
            .u16_val => .{ .u16_val = std.mem.bigToNative(u16, std.mem.bytesToValue(u16, value_bytes[0..2])) },
            .u32_val => .{ .u32_val = std.mem.bigToNative(u32, std.mem.bytesToValue(u32, value_bytes[0..4])) },
            .i64_val => .{ .i64_val = decodeI64(value_bytes[0..8].*) },
            .u64_val => .{ .u64_val = std.mem.bigToNative(u64, std.mem.bytesToValue(u64, value_bytes[0..8])) },
            .uuid => .{ .uuid = value_bytes[0..16].* },
            .ipv4 => .{ .ipv4 = std.mem.bigToNative(u32, std.mem.bytesToValue(u32, value_bytes[0..4])) },
            .ipv6 => .{ .ipv6 = value_bytes[0..16].* },
            .bytes => .{ .bytes = value_bytes },
        };
    }

    /// Return the ValueType tag without decoding the full value.
    pub fn valueType(self: EntityKey) ValueType {
        const sep = std.mem.indexOfScalar(u8, self.raw, 0).?;
        return @enumFromInt(self.raw[sep + 1]);
    }

    /// Raw encoded bytes for LMDB storage or interner key.
    pub fn asBytes(self: EntityKey) []const u8 {
        return self.raw;
    }
};

/// Encode an entity key into the provided buffer.
pub fn encode(buf: []u8, entity_type: []const u8, val: Value) !EntityKey {
    const value_len: usize = switch (val) {
        .string => |s| s.len,
        .u16_val => 2,
        .u32_val, .ipv4 => 4,
        .i64_val, .u64_val => 8,
        .uuid, .ipv6 => 16,
        .bytes => |b| b.len,
    };
    const total_len = entity_type.len + 1 + 1 + value_len; // type + \x00 + tag + value
    if (total_len > buf.len) return error.BufferTooSmall;

    // Write entity type
    @memcpy(buf[0..entity_type.len], entity_type);
    // Null separator
    buf[entity_type.len] = 0;
    // Type tag
    buf[entity_type.len + 1] = @intFromEnum(val.valueType());
    // Value bytes
    const vstart = entity_type.len + 2;
    switch (val) {
        .string => |s| @memcpy(buf[vstart..][0..s.len], s),
        .u16_val => |v| @as(*[2]u8, @ptrCast(buf[vstart..][0..2])).* = std.mem.toBytes(std.mem.nativeToBig(u16, v)),
        .u32_val => |v| @as(*[4]u8, @ptrCast(buf[vstart..][0..4])).* = std.mem.toBytes(std.mem.nativeToBig(u32, v)),
        .i64_val => |v| @as(*[8]u8, @ptrCast(buf[vstart..][0..8])).* = encodeI64(v),
        .u64_val => |v| @as(*[8]u8, @ptrCast(buf[vstart..][0..8])).* = std.mem.toBytes(std.mem.nativeToBig(u64, v)),
        .uuid => |v| @as(*[16]u8, @ptrCast(buf[vstart..][0..16])).* = v,
        .ipv4 => |v| @as(*[4]u8, @ptrCast(buf[vstart..][0..4])).* = std.mem.toBytes(std.mem.nativeToBig(u32, v)),
        .ipv6 => |v| @as(*[16]u8, @ptrCast(buf[vstart..][0..16])).* = v,
        .bytes => |b| @memcpy(buf[vstart..][0..b.len], b),
    }

    return EntityKey{ .raw = buf[0..total_len] };
}

/// Convenience: encode an entity type + bare string value.
pub fn encodeString(buf: []u8, entity_type: []const u8, id: []const u8) !EntityKey {
    return encode(buf, entity_type, .{ .string = id });
}

/// Decode an EntityKey from raw bytes.
/// The returned EntityKey borrows the input slice.
pub fn fromBytes(raw: []const u8) !EntityKey {
    const sep = std.mem.indexOfScalar(u8, raw, 0) orelse return error.InvalidKey;
    if (sep + 1 >= raw.len) return error.InvalidKey;

    // Validate the type tag is a known value
    const tag_byte = raw[sep + 1];
    const type_tag: ValueType = std.meta.intToEnum(ValueType, tag_byte) catch return error.InvalidKey;

    const value_bytes = raw[sep + 2 ..];

    // For fixed-width types, validate length matches expected size
    if (type_tag.fixedLen()) |expected| {
        if (value_bytes.len != expected) return error.InvalidKey;
    }

    return EntityKey{ .raw = raw };
}

/// Compare two encoded values semantically.
/// For same-type keys, compares values in their natural order.
/// For different types, falls back to type tag order.
pub fn compareValues(a: EntityKey, b: EntityKey) std.math.Order {
    const a_type = a.valueType();
    const b_type = b.valueType();

    if (a_type != b_type) {
        return std.math.order(@intFromEnum(a_type), @intFromEnum(b_type));
    }

    // Same type — compare value bytes directly (encodings are order-preserving)
    const a_sep = std.mem.indexOfScalar(u8, a.raw, 0).?;
    const b_sep = std.mem.indexOfScalar(u8, b.raw, 0).?;
    const a_val_bytes = a.raw[a_sep + 2 ..];
    const b_val_bytes = b.raw[b_sep + 2 ..];

    return std.mem.order(u8, a_val_bytes, b_val_bytes);
}

// =============================================================================
// Internal encoding helpers
// =============================================================================

fn encodeI64(val: i64) [8]u8 {
    const as_u64: u64 = @bitCast(val);
    const flipped = as_u64 ^ (@as(u64, 1) << 63);
    return std.mem.toBytes(std.mem.nativeToBig(u64, flipped));
}

fn decodeI64(bytes: [8]u8) i64 {
    const be_val = std.mem.bigToNative(u64, std.mem.bytesToValue(u64, &bytes));
    const unflipped = be_val ^ (@as(u64, 1) << 63);
    return @bitCast(unflipped);
}

// =============================================================================
// Tests
// =============================================================================

const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const expectEqualStrings = std.testing.expectEqualStrings;
const expectEqualSlices = std.testing.expectEqualSlices;
const expectError = std.testing.expectError;

test "encode/decode string roundtrip" {
    var buf: [512]u8 = undefined;
    const key = try encodeString(&buf, "author", "Homer");
    try expectEqualStrings("author", key.entityType());
    try expectEqual(ValueType.string, key.valueType());
    try expectEqualStrings("Homer", key.value().string);
}

test "encode/decode u16 roundtrip" {
    var buf: [512]u8 = undefined;
    const key = try encode(&buf, "port", .{ .u16_val = 443 });
    try expectEqual(ValueType.u16_val, key.valueType());
    try expectEqual(@as(u16, 443), key.value().u16_val);
}

test "encode/decode i64 roundtrip - negative values" {
    var buf: [512]u8 = undefined;
    const key = try encode(&buf, "offset", .{ .i64_val = -42 });
    try expectEqual(@as(i64, -42), key.value().i64_val);
}

test "encode/decode ipv4 roundtrip" {
    var buf: [512]u8 = undefined;
    const key = try encode(&buf, "host", .{ .ipv4 = 0x0A000001 }); // 10.0.0.1
    try expectEqual(@as(u32, 0x0A000001), key.value().ipv4);
}

test "encode/decode uuid roundtrip" {
    var buf: [512]u8 = undefined;
    const test_uuid = [16]u8{ 0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00 };
    const key = try encode(&buf, "guid", .{ .uuid = test_uuid });
    try expectEqualSlices(u8, &test_uuid, &key.value().uuid);
}

test "encode/decode bytes roundtrip - with null bytes" {
    var buf: [512]u8 = undefined;
    const val_with_null = "abc\x00def";
    const key = try encode(&buf, "blob", .{ .bytes = val_with_null });
    try expectEqualSlices(u8, val_with_null, key.value().bytes);
}

test "string sort order - lexicographic" {
    var buf_a: [512]u8 = undefined;
    var buf_b: [512]u8 = undefined;
    var buf_c: [512]u8 = undefined;
    const a = try encodeString(&buf_a, "x", "Amy");
    const b = try encodeString(&buf_b, "x", "Homer");
    const c = try encodeString(&buf_c, "x", "Jo");
    // Pure lexicographic: Amy < Homer < Jo (not length-first)
    try expect(std.mem.order(u8, a.asBytes(), b.asBytes()) == .lt);
    try expect(std.mem.order(u8, b.asBytes(), c.asBytes()) == .lt);
}

test "u16 sort order - numeric" {
    var buf_a: [512]u8 = undefined;
    var buf_b: [512]u8 = undefined;
    const a = try encode(&buf_a, "port", .{ .u16_val = 80 });
    const b = try encode(&buf_b, "port", .{ .u16_val = 443 });
    try expect(std.mem.order(u8, a.asBytes(), b.asBytes()) == .lt);
}

test "i64 sort order - MIN < -1 < 0 < 1 < MAX" {
    var bufs: [5][512]u8 = undefined;
    const vals = [_]i64{ std.math.minInt(i64), -1, 0, 1, std.math.maxInt(i64) };
    var keys: [5]EntityKey = undefined;
    for (vals, 0..) |v, i| {
        keys[i] = try encode(&bufs[i], "v", .{ .i64_val = v });
    }
    for (0..4) |i| {
        try expect(std.mem.order(u8, keys[i].asBytes(), keys[i + 1].asBytes()) == .lt);
    }
}

test "ipv4 sort order - network order" {
    var buf_a: [512]u8 = undefined;
    var buf_b: [512]u8 = undefined;
    const a = try encode(&buf_a, "host", .{ .ipv4 = 0x0A000001 }); // 10.0.0.1
    const b = try encode(&buf_b, "host", .{ .ipv4 = 0xC0A80101 }); // 192.168.1.1
    try expect(std.mem.order(u8, a.asBytes(), b.asBytes()) == .lt);
}

test "entity type grouping - prefix scan friendly" {
    var buf_a: [512]u8 = undefined;
    var buf_b: [512]u8 = undefined;
    var buf_c: [512]u8 = undefined;
    const a1 = try encodeString(&buf_a, "author", "Zebra");
    const a2 = try encodeString(&buf_b, "author", "Alpha");
    const b1 = try encodeString(&buf_c, "book", "Alpha");
    // All "author" keys sort before all "book" keys
    try expect(std.mem.order(u8, a1.asBytes(), b1.asBytes()) == .lt);
    try expect(std.mem.order(u8, a2.asBytes(), b1.asBytes()) == .lt);
    // Within "author", sorts by value
    try expect(std.mem.order(u8, a2.asBytes(), a1.asBytes()) == .lt);
}

test "compareValues - string comparison" {
    var buf_a: [512]u8 = undefined;
    var buf_b: [512]u8 = undefined;
    const a = try encodeString(&buf_a, "x", "apple");
    const b = try encodeString(&buf_b, "x", "banana");
    try expect(compareValues(a, b) == .lt);
}

test "compareValues - cross-type returns type tag order" {
    var buf_a: [512]u8 = undefined;
    var buf_b: [512]u8 = undefined;
    const a = try encodeString(&buf_a, "x", "hello");
    const b = try encode(&buf_b, "x", .{ .u16_val = 42 });
    // string (0x01) < u16 (0x02)
    try expect(compareValues(a, b) == .lt);
}

test "buffer too small returns error" {
    var buf: [5]u8 = undefined;
    try expectError(error.BufferTooSmall, encodeString(&buf, "author", "Homer"));
}
