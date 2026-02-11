const std = @import("std");
const kb = @import("kb");
const Fact = kb.Fact;
const Entity = kb.Entity;

fn fuzzDeserialize(allocator: std.mem.Allocator, input: []const u8) void {
    // Try to deserialize arbitrary bytes - should never crash
    const fact = Fact.deserialize(input, allocator) catch return;
    defer fact.deinit(allocator);

    // If we got here, verify it round-trips
    var buf: [65536]u8 = undefined;
    const serialized = fact.serialize(&buf) catch return;

    const roundtrip = Fact.deserialize(serialized, allocator) catch return;
    defer roundtrip.deinit(allocator);

    // Verify consistency
    std.debug.assert(fact.id == roundtrip.id);
    std.debug.assert(fact.entities.len == roundtrip.entities.len);
}

test "fuzz deserialize" {
    const allocator = std.testing.allocator;

    // Manual corpus of interesting inputs
    const corpus = [_][]const u8{
        // Empty
        "",
        // Too short for header
        "\x00\x00\x00\x00",
        // Valid header, zero entities
        "\x00\x00\x00\x00\x00\x00\x00\x01" ++ // id = 1
            "\x00\x00\x00\x00" ++ // 0 entities
            "\x00\x00", // source len = 0
        // Huge entity count (should fail gracefully)
        "\x00\x00\x00\x00\x00\x00\x00\x01" ++ // id = 1
            "\xff\xff\xff\xff", // 4 billion entities
        // Huge string length
        "\x00\x00\x00\x00\x00\x00\x00\x01" ++
            "\x00\x00\x00\x01" ++ // 1 entity
            "\xff\xff", // type len = 65535
        // Valid small fact
        "\x00\x00\x00\x00\x00\x00\x00\x2a" ++ // id = 42
            "\x00\x00\x00\x01" ++ // 1 entity
            "\x00\x04" ++ "test" ++ // type = "test"
            "\x00\x02" ++ "id" ++ // id = "id"
            "\x00\x00", // no source
    };

    for (corpus) |input| {
        const fact = Fact.deserialize(input, allocator) catch continue;
        defer fact.deinit(allocator);

        // If deserialize succeeded, serialize should work
        var buf: [65536]u8 = undefined;
        _ = fact.serialize(&buf) catch continue;
    }
}
