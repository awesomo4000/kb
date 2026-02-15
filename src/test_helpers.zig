const std = @import("std");
const lmdb = @import("lmdb");

pub fn createTempLmdbEnv(allocator: std.mem.Allocator) !struct { env: lmdb.Environment, path: [:0]const u8 } {
    const random = std.crypto.random.int(u64);
    const path = try std.fmt.allocPrint(allocator, "/tmp/kb-bitmap-test-{x}", .{random});
    const path_z = try allocator.realloc(path, path.len + 1);
    path_z[path.len] = 0;
    const path_sentinel = path_z[0..path.len :0];

    std.fs.makeDirAbsolute(path_sentinel) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };

    const env = try lmdb.Environment.init(path_sentinel, .{ .max_dbs = 8 });
    return .{ .env = env, .path = path_sentinel };
}

pub fn destroyTempLmdbEnv(state: anytype, allocator: std.mem.Allocator) void {
    state.env.deinit();
    std.fs.deleteTreeAbsolute(state.path) catch {};
    allocator.free(state.path);
}
