const std = @import("std");
const lmdb = @import("lmdb");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const path = "/tmp/kb-bench-raw";
    std.fs.deleteTreeAbsolute(path) catch {};
    std.fs.makeDirAbsolute(path) catch {};
    defer std.fs.deleteTreeAbsolute(path) catch {};

    const env = try lmdb.Environment.init(path, .{
        .map_size = 1024 * 1024 * 1024,
        .max_dbs = 4,
        // no_sync = false means fsync on each commit (default, safe)
    });
    defer env.deinit();

    const count: usize = 1000; // Fewer ops since fsync is slow

    // Test 1: Regular DB (single value per key)
    {
        std.debug.print("\n=== Regular DB (1 value per key) ===\n", .{});
        var timer = try std.time.Timer.start();

        for (0..count) |i| {
            const txn = try env.transaction(.{ .mode = .ReadWrite });
            errdefer txn.abort();

            const db = try txn.database("regular", .{ .create = true });

            var key_buf: [32]u8 = undefined;
            const key = std.fmt.bufPrint(&key_buf, "key-{}", .{i}) catch unreachable;

            try db.set(key, "value");
            try txn.commit();
        }

        const elapsed_ns = timer.read();
        const per_op_us = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(count)) / 1000.0;
        std.debug.print("{} ops: {d:.1} µs/op\n", .{ count, per_op_us });
    }

    // Test 2: DUPSORT DB (multiple values per key)
    {
        std.debug.print("\n=== DUPSORT DB (multiple values per key) ===\n", .{});
        var timer = try std.time.Timer.start();

        for (0..count) |i| {
            const txn = try env.transaction(.{ .mode = .ReadWrite });
            errdefer txn.abort();

            const db = try txn.database("dupsort", .{ .dup_sort = true, .create = true });

            var val_buf: [32]u8 = undefined;
            const val = std.fmt.bufPrint(&val_buf, "val-{}", .{i}) catch unreachable;

            // All same key, different values
            try db.set("shared-key", val);
            try txn.commit();
        }

        const elapsed_ns = timer.read();
        const per_op_us = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(count)) / 1000.0;
        std.debug.print("{} ops: {d:.1} µs/op\n", .{ count, per_op_us });
    }

    // Test 3: DUPSORT DB with unique keys
    {
        std.debug.print("\n=== DUPSORT DB (unique keys) ===\n", .{});
        var timer = try std.time.Timer.start();

        for (0..count) |i| {
            const txn = try env.transaction(.{ .mode = .ReadWrite });
            errdefer txn.abort();

            const db = try txn.database("dupsort2", .{ .dup_sort = true, .create = true });

            var key_buf: [32]u8 = undefined;
            const key = std.fmt.bufPrint(&key_buf, "key-{}", .{i}) catch unreachable;

            try db.set(key, "value");
            try txn.commit();
        }

        const elapsed_ns = timer.read();
        const per_op_us = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(count)) / 1000.0;
        std.debug.print("{} ops: {d:.1} µs/op\n", .{ count, per_op_us });
    }

    _ = allocator;
}
