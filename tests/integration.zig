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

test "batch insert stress test" {
    const allocator = std.testing.allocator;
    const path = try getTempPath(allocator);
    defer allocator.free(path);
    defer std.fs.deleteTreeAbsolute(path) catch {};

    var store = try FactStore.open(allocator, .{ .path = path, .no_sync = true });
    defer store.close();

    const batch_size: usize = 100;
    const num_batches: usize = 10;

    // Insert batches
    for (0..num_batches) |batch_num| {
        var batch: [batch_size]FactStore.FactInput = undefined;
        var entity_storage: [batch_size][3]Entity = undefined;

        for (0..batch_size) |j| {
            const idx = batch_num * batch_size + j;
            entity_storage[j] = .{
                .{ .type = "person", .id = "alice" },
                .{ .type = "color", .id = if (idx % 2 == 0) "blue" else "green" },
                .{ .type = "fruit", .id = "apple" },
            };
            batch[j] = .{ .entities = &entity_storage[j], .source = "stress" };
        }

        const ids = try store.addFacts(&batch);
        defer allocator.free(ids);

        // Verify IDs are sequential
        for (ids, 0..) |id, i| {
            try std.testing.expectEqual(batch_num * batch_size + i + 1, id);
        }
    }

    // Query and verify counts
    const person_facts = try store.getFactsByEntity(.{ .type = "person", .id = "alice" }, allocator);
    defer allocator.free(person_facts);
    try std.testing.expectEqual(batch_size * num_batches, person_facts.len);

    const blue_facts = try store.getFactsByEntity(.{ .type = "color", .id = "blue" }, allocator);
    defer allocator.free(blue_facts);
    try std.testing.expectEqual(batch_size * num_batches / 2, blue_facts.len);
}

test "list entities" {
    const allocator = std.testing.allocator;
    const path = try getTempPath(allocator);
    defer allocator.free(path);
    defer std.fs.deleteTreeAbsolute(path) catch {};

    var store = try FactStore.open(allocator, .{ .path = path });
    defer store.close();

    // Add facts with various entities
    _ = try store.addFact(&.{
        .{ .type = "person", .id = "alice" },
        .{ .type = "city", .id = "paris" },
    }, "test");
    _ = try store.addFact(&.{
        .{ .type = "person", .id = "bob" },
        .{ .type = "city", .id = "tokyo" },
    }, "test");
    _ = try store.addFact(&.{
        .{ .type = "person", .id = "alice" },
        .{ .type = "food", .id = "pizza" },
    }, "test");

    // List entity types
    const types = try store.listEntityTypes(allocator);
    defer {
        for (types) |t| allocator.free(t);
        allocator.free(types);
    }
    try std.testing.expectEqual(@as(usize, 3), types.len);

    // List people
    const people = try store.listEntities("person", allocator);
    defer {
        for (people) |p| allocator.free(p);
        allocator.free(people);
    }
    try std.testing.expectEqual(@as(usize, 2), people.len);
}

test "get fact by id" {
    const allocator = std.testing.allocator;
    const path = try getTempPath(allocator);
    defer allocator.free(path);
    defer std.fs.deleteTreeAbsolute(path) catch {};

    var store = try FactStore.open(allocator, .{ .path = path });
    defer store.close();

    const entities = [_]Entity{
        .{ .type = "book", .id = "the-odyssey" },
        .{ .type = "author", .id = "homer" },
    };
    const fact_id = try store.addFact(&entities, "library");

    // Retrieve the fact
    const fact = try store.getFact(fact_id, allocator) orelse return error.FactNotFound;
    defer fact.deinit(allocator);

    try std.testing.expectEqual(fact_id, fact.id);
    try std.testing.expectEqual(@as(usize, 2), fact.entities.len);
    try std.testing.expectEqualStrings("book", fact.entities[0].type);
    try std.testing.expectEqualStrings("the-odyssey", fact.entities[0].id);
    try std.testing.expectEqualStrings("library", fact.source.?);

    // Non-existent fact
    const missing = try store.getFact(9999, allocator);
    try std.testing.expect(missing == null);
}
