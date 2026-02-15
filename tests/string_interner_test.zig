const std = @import("std");
const lmdb = @import("lmdb");
const kb = @import("kb");
const StringInterner = kb.StringInterner;

fn getTempPath(allocator: std.mem.Allocator) ![:0]const u8 {
    const random = std.crypto.random.int(u64);
    const path = try std.fmt.allocPrint(allocator, "/tmp/kb-interner-test-{x}", .{random});
    const path_z = try allocator.realloc(path, path.len + 1);
    path_z[path.len] = 0;
    return path_z[0..path.len :0];
}

fn createTempLmdbEnv(path: [:0]const u8) !lmdb.Environment {
    std.fs.makeDirAbsolute(path) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };
    return try lmdb.Environment.init(path, .{
        .max_dbs = 8,
    });
}

test "persist and load roundtrip" {
    const allocator = std.testing.allocator;
    const path = try getTempPath(allocator);
    defer allocator.free(path);
    defer std.fs.deleteTreeAbsolute(path) catch {};

    var env = try createTempLmdbEnv(path);
    defer env.deinit();

    // Populate and persist
    {
        var si = StringInterner.init(allocator);
        defer si.deinit();

        _ = try si.intern("homer");
        _ = try si.intern("virgil");
        _ = try si.intern("dante");

        try si.persist(env);
    }

    // Load into fresh interner
    {
        var si = try StringInterner.load(allocator, env);
        defer si.deinit();

        // Same IDs as before
        try std.testing.expectEqual(@as(u32, 3), si.count());
        try std.testing.expectEqualStrings("homer", si.resolve(0));
        try std.testing.expectEqualStrings("virgil", si.resolve(1));
        try std.testing.expectEqualStrings("dante", si.resolve(2));

        // Lookup works
        try std.testing.expectEqual(@as(?u32, 0), si.lookup("homer"));

        // New interns continue from where we left off
        const id = try si.intern("new_entity");
        try std.testing.expectEqual(@as(u32, 3), id);
    }
}

test "persist and load empty interner" {
    const allocator = std.testing.allocator;
    const path = try getTempPath(allocator);
    defer allocator.free(path);
    defer std.fs.deleteTreeAbsolute(path) catch {};

    var env = try createTempLmdbEnv(path);
    defer env.deinit();

    {
        var si = StringInterner.init(allocator);
        defer si.deinit();
        try si.persist(env);
    }

    {
        var si = try StringInterner.load(allocator, env);
        defer si.deinit();
        try std.testing.expectEqual(@as(u32, 0), si.count());
    }
}

test "load from fresh database returns empty interner" {
    const allocator = std.testing.allocator;
    const path = try getTempPath(allocator);
    defer allocator.free(path);
    defer std.fs.deleteTreeAbsolute(path) catch {};

    var env = try createTempLmdbEnv(path);
    defer env.deinit();

    // Don't persist anything - load from fresh DB
    var si = try StringInterner.load(allocator, env);
    defer si.deinit();

    try std.testing.expectEqual(@as(u32, 0), si.count());

    // Can still intern new strings
    const id = try si.intern("first");
    try std.testing.expectEqual(@as(u32, 0), id);
}
