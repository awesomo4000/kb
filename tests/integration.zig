const std = @import("std");
const kb = @import("kb");
const FactStore = kb.FactStore;
const Entity = kb.Entity;

fn getTempPath(allocator: std.mem.Allocator) ![:0]const u8 {
    const random = std.crypto.random.int(u64);
    const path = try std.fmt.allocPrint(allocator, "/tmp/kb-test-{x}", .{random});
    // Convert to null-terminated
    const path_z = try allocator.realloc(path, path.len + 1);
    path_z[path.len] = 0;
    return path_z[0..path.len :0];
}

test "fact store open and close" {
    const allocator = std.testing.allocator;
    const path = try getTempPath(allocator);
    defer allocator.free(path);
    defer std.fs.deleteTreeAbsolute(path) catch {};

    var store = try FactStore.open(allocator, .{ .path = path });
    defer store.close();
}

test "add and query facts" {
    const allocator = std.testing.allocator;
    const path = try getTempPath(allocator);
    defer allocator.free(path);
    defer std.fs.deleteTreeAbsolute(path) catch {};

    var store = try FactStore.open(allocator, .{ .path = path });
    defer store.close();

    // Add a fact with two entities
    const entities = [_]Entity{
        .{ .type = "host", .id = "10.2.3.12" },
        .{ .type = "port", .id = "8080" },
    };
    const fact_id = try store.addFact(&entities, "test");

    try std.testing.expectEqual(@as(u64, 1), fact_id);

    // Query by host
    const host_facts = try store.getFactsByEntity(.{ .type = "host", .id = "10.2.3.12" }, allocator);
    defer allocator.free(host_facts);
    try std.testing.expectEqual(@as(usize, 1), host_facts.len);
    try std.testing.expectEqual(fact_id, host_facts[0]);

    // Query by port
    const port_facts = try store.getFactsByEntity(.{ .type = "port", .id = "8080" }, allocator);
    defer allocator.free(port_facts);
    try std.testing.expectEqual(@as(usize, 1), port_facts.len);
    try std.testing.expectEqual(fact_id, port_facts[0]);
}
