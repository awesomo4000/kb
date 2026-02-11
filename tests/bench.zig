const std = @import("std");
const kb = @import("kb");
const FactStore = kb.FactStore;
const FactInput = kb.FactInput;
const Entity = kb.Entity;

fn getTempPath(allocator: std.mem.Allocator) ![:0]const u8 {
    const random = std.crypto.random.int(u64);
    const path = try std.fmt.allocPrint(allocator, "/tmp/kb-bench-{x}", .{random});
    const path_z = try allocator.realloc(path, path.len + 1);
    path_z[path.len] = 0;
    return path_z[0..path.len :0];
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const path = try getTempPath(allocator);
    defer allocator.free(path);
    defer std.fs.deleteTreeAbsolute(path) catch {};

    var store = try FactStore.open(allocator, .{ .path = path, .no_sync = true });
    defer store.close();

    // Test with unique entities (no hot keys) vs shared entities (hot keys)
    std.debug.print("\n=== Unique entities (no key contention) ===\n", .{});
    try benchUnique(&store, allocator, path);

    std.debug.print("\n=== Shared entities (hot keys) ===\n", .{});
    try benchShared(&store, allocator, path);

    std.debug.print("\n=== BATCHED: Shared entities (1000 facts per txn) ===\n", .{});
    try benchBatched(&store, allocator, path);
}

fn benchUnique(store: *FactStore, allocator: std.mem.Allocator, path: [:0]const u8) !void {
    const counts = [_]usize{ 1000, 10000, 100000 };

    for (counts) |count| {
        store.close();
        std.fs.deleteTreeAbsolute(path) catch {};
        store.* = try FactStore.open(allocator, .{ .path = path, .no_sync = true });

        var timer = try std.time.Timer.start();

        var i: usize = 0;
        while (i < count) : (i += 1) {
            var item_buf: [32]u8 = undefined;
            var cat_buf: [8]u8 = undefined;
            var tag_buf: [16]u8 = undefined;

            // ALL UNIQUE - each entity appears in only one fact
            const item_id = std.fmt.bufPrint(&item_buf, "item-{}-{}-{}", .{
                (i / 65536) % 256,
                (i / 256) % 256,
                i % 256,
            }) catch unreachable;

            const cat_id = std.fmt.bufPrint(&cat_buf, "{}", .{i}) catch unreachable;
            const tag_id = std.fmt.bufPrint(&tag_buf, "tag-{}", .{i}) catch unreachable;

            const entities = [_]Entity{
                .{ .type = "item", .id = item_id },
                .{ .type = "category", .id = cat_id },
                .{ .type = "tag", .id = tag_id },
            };

            _ = try store.addFact(&entities, "bench");
        }

        const elapsed_ns = timer.read();
        const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
        const per_fact_us = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(count)) / 1000.0;

        std.debug.print("{} facts: {d:.1} ms ({d:.1} µs/fact)\n", .{
            count,
            elapsed_ms,
            per_fact_us,
        });
    }
}

fn benchShared(store: *FactStore, allocator: std.mem.Allocator, path: [:0]const u8) !void {
    const counts = [_]usize{ 1000, 10000, 100000 };

    for (counts) |count| {
        store.close();
        std.fs.deleteTreeAbsolute(path) catch {};
        store.* = try FactStore.open(allocator, .{ .path = path, .no_sync = true });

        var timer = try std.time.Timer.start();

        var i: usize = 0;
        while (i < count) : (i += 1) {
            var item_buf: [32]u8 = undefined;
            var cat_buf: [8]u8 = undefined;

            // SHARED - same 100 categories, same tag across all facts
            const item_id = std.fmt.bufPrint(&item_buf, "item-{}-{}", .{
                (i / 256) % 256,
                i % 256,
            }) catch unreachable;

            const cat_id = std.fmt.bufPrint(&cat_buf, "{}", .{
                1000 + (i % 100),
            }) catch unreachable;

            const entities = [_]Entity{
                .{ .type = "item", .id = item_id },
                .{ .type = "category", .id = cat_id },
                .{ .type = "tag", .id = "common" }, // HOT KEY
            };

            _ = try store.addFact(&entities, "bench");
        }

        const elapsed_ns = timer.read();
        const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
        const per_fact_us = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(count)) / 1000.0;

        std.debug.print("{} facts: {d:.1} ms ({d:.1} µs/fact)\n", .{
            count,
            elapsed_ms,
            per_fact_us,
        });
    }
}

fn benchBatched(store: *FactStore, allocator: std.mem.Allocator, path: [:0]const u8) !void {
    const counts = [_]usize{ 1000, 10000, 100000 };
    const batch_size: usize = 1000;

    for (counts) |count| {
        store.close();
        std.fs.deleteTreeAbsolute(path) catch {};
        store.* = try FactStore.open(allocator, .{ .path = path, .no_sync = true });

        // Pre-allocate batch buffer
        var batch: [batch_size]FactInput = undefined;
        var entity_storage: [batch_size][3]Entity = undefined;

        var timer = try std.time.Timer.start();

        var i: usize = 0;
        while (i < count) {
            const this_batch = @min(batch_size, count - i);

            // Fill batch
            for (0..this_batch) |j| {
                var item_buf: [32]u8 = undefined;
                var cat_buf: [8]u8 = undefined;

                const idx = i + j;
                const item_id = std.fmt.bufPrint(&item_buf, "item-{}-{}", .{
                    (idx / 256) % 256,
                    idx % 256,
                }) catch unreachable;

                const cat_id = std.fmt.bufPrint(&cat_buf, "{}", .{
                    1000 + (idx % 100),
                }) catch unreachable;

                entity_storage[j] = .{
                    .{ .type = "item", .id = item_id },
                    .{ .type = "category", .id = cat_id },
                    .{ .type = "tag", .id = "common" },
                };
                batch[j] = .{ .entities = &entity_storage[j], .source = "bench" };
            }

            const ids = try store.addFacts(batch[0..this_batch]);
            allocator.free(ids);

            i += this_batch;
        }

        const elapsed_ns = timer.read();
        const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
        const per_fact_us = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(count)) / 1000.0;

        std.debug.print("{} facts: {d:.1} ms ({d:.1} µs/fact)\n", .{
            count,
            elapsed_ms,
            per_fact_us,
        });
    }
}
