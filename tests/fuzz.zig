const std = @import("std");
const kb = @import("kb");
const Fact = kb.Fact;
const Entity = kb.Entity;
const FactStore = kb.FactStore;
const entity_key = kb.entity_key;

// =============================================================================
// Fuzz target 1: Fact deserialization
// =============================================================================

test "fuzz deserialize" {
    const corpus = [_][]const u8{
        // Valid small fact
        "\x00\x00\x00\x00\x00\x00\x00\x2a" ++ // id = 42
            "\x00\x00\x00\x01" ++ // 1 entity
            "\x00\x04" ++ "test" ++ // type = "test"
            "\x00\x02" ++ "id" ++ // id = "id"
            "\x00\x00", // no source
        // Valid fact with source
        "\x00\x00\x00\x00\x00\x00\x00\x01" ++
            "\x00\x00\x00\x01" ++
            "\x00\x04" ++ "type" ++
            "\x00\x05" ++ "value" ++
            "\x00\x06" ++ "source",
    };

    try std.testing.fuzz({}, fuzzDeserialize, .{ .corpus = &corpus });
}

fn fuzzDeserialize(_: void, input: []const u8) !void {
    var buf: [1024 * 1024]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const allocator = fba.allocator();

    const fact = Fact.deserialize(input, allocator) catch return;

    // Round-trip test
    var ser_buf: [65536]u8 = undefined;
    const serialized = fact.serialize(&ser_buf) catch return;

    fba.reset();
    const roundtrip = Fact.deserialize(serialized, allocator) catch return;

    if (fact.id != roundtrip.id) return error.IdMismatch;
    if (fact.entities.len != roundtrip.entities.len) return error.EntityCountMismatch;
}

// =============================================================================
// Fuzz target 2: Entity key generation
// =============================================================================

test "fuzz entity toKey" {
    const corpus = [_][]const u8{
        "type\x00id",
        "a\x00b",
        "\x00\x00",
        "long_type_name\x00long_id_value_here",
    };

    try std.testing.fuzz({}, fuzzEntityKey, .{ .corpus = &corpus });
}

fn fuzzEntityKey(_: void, input: []const u8) !void {
    // Split input at first null byte to get type and id
    const sep = std.mem.indexOfScalar(u8, input, 0) orelse return;
    if (sep == 0 or sep >= input.len - 1) return;

    const entity_type = input[0..sep];
    const entity_id = input[sep + 1 ..];

    var buf: [4096]u8 = undefined;
    const ek = entity_key.encodeString(&buf, entity_type, entity_id) catch return;

    // Verify key format: type + \x00 + type_tag + id
    const key = ek.asBytes();
    if (key.len != entity_type.len + 1 + 1 + entity_id.len) return error.KeyLengthMismatch;
    if (!std.mem.startsWith(u8, key, entity_type)) return error.KeyTypeMismatch;
    if (key[entity_type.len] != 0) return error.KeySeparatorMismatch;
    if (key[entity_type.len + 1] != @intFromEnum(entity_key.ValueType.string)) return error.KeyTypeByteMismatch;
    if (!std.mem.endsWith(u8, key, entity_id)) return error.KeyIdMismatch;

    // Verify roundtrip through fromBytes
    const decoded = entity_key.fromBytes(key) catch return error.DecodeFailure;
    if (!std.mem.eql(u8, decoded.entityType(), entity_type)) return error.EntityTypeMismatch;
    if (!std.mem.eql(u8, decoded.value().string, entity_id)) return error.ValueMismatch;
}

// =============================================================================
// Fuzz target 3: Typed entity key roundtrip
// =============================================================================

test "fuzz entity key typed" {
    const corpus = [_][]const u8{
        "port\x00\x00\x01\xBB", // u16: type_byte % 3 == 0 but sep+1 gives 0x00
        "host\x00\x01\x0A\x00\x00\x01", // string fallback
        "val\x00\x02\x00\x2A\x00\x00\x00\x00\x00\x00", // i64
    };

    try std.testing.fuzz({}, fuzzEntityKeyTyped, .{ .corpus = &corpus });
}

fn fuzzEntityKeyTyped(_: void, input: []const u8) !void {
    if (input.len < 3) return;
    const sep = std.mem.indexOfScalar(u8, input, 0) orelse return;
    if (sep == 0 or sep >= input.len - 2) return;

    const entity_type = input[0..sep];
    const type_byte = input[sep + 1];
    const value_data = input[sep + 2 ..];

    var buf: [4096]u8 = undefined;
    const val: entity_key.Value = switch (type_byte % 3) {
        0 => .{ .string = value_data },
        1 => if (value_data.len >= 2)
            .{ .u16_val = std.mem.bytesToValue(u16, value_data[0..2]) }
        else
            return,
        2 => if (value_data.len >= 8)
            .{ .i64_val = @bitCast(std.mem.bytesToValue(u64, value_data[0..8])) }
        else
            return,
        else => unreachable,
    };

    const ek_result = entity_key.encode(&buf, entity_type, val) catch return;
    const decoded = entity_key.fromBytes(ek_result.asBytes()) catch return error.DecodeFailure;

    // Verify entity type roundtrips
    if (!std.mem.eql(u8, decoded.entityType(), entity_type)) return error.EntityTypeMismatch;

    // Verify value type roundtrips
    if (decoded.valueType() != val.valueType()) return error.ValueTypeMismatch;

    // Verify value roundtrips
    switch (val) {
        .string => |s| {
            if (!std.mem.eql(u8, decoded.value().string, s)) return error.ValueMismatch;
        },
        .u16_val => |v| {
            if (decoded.value().u16_val != v) return error.ValueMismatch;
        },
        .i64_val => |v| {
            if (decoded.value().i64_val != v) return error.ValueMismatch;
        },
        else => {},
    }
}

// =============================================================================
// Fuzz target 4: FactStore operations with arbitrary entities
// NOTE: Disabled for continuous fuzzing - LMDB doesn't play well with fuzzer's
// process model. Run corpus test only: zig build test-fuzz
// =============================================================================

test "fuzz factstore add (corpus only)" {
    // This test only runs corpus, not continuous fuzzing
    // The FactStore requires disk I/O which is problematic for the fuzzer
    const corpus = [_][]const u8{
        "book\x00odyssey\x00author\x00homer",
        "a\x00b\x00c\x00d",
        "type\x00id",
    };

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const path = "/tmp/fuzz-factstore-corpus";
    std.fs.deleteTreeAbsolute(path) catch {};
    defer std.fs.deleteTreeAbsolute(path) catch {};

    var store = try FactStore.open(allocator, .{ .path = path, .no_sync = true });
    defer store.close();

    for (corpus) |input| {
        // Parse input as null-separated type/id pairs
        var entities_buf: [16]Entity = undefined;
        var entity_count: usize = 0;

        var iter = std.mem.splitScalar(u8, input, 0);
        while (iter.next()) |part| {
            if (entity_count >= 15) break;
            if (part.len == 0) continue;

            const next = iter.next() orelse break;
            if (next.len == 0) continue;

            entities_buf[entity_count] = .{ .type = part, .id = next };
            entity_count += 1;
        }

        if (entity_count == 0) continue;

        _ = store.addFact(entities_buf[0..entity_count], "fuzz") catch continue;
    }
}
