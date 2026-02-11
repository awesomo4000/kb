const std = @import("std");
const kb = @import("kb");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        try printUsage();
        return;
    }

    const command = args[1];

    if (std.mem.eql(u8, command, "ls")) {
        try cmdLs(allocator, args[2..]);
    } else if (std.mem.eql(u8, command, "ingest")) {
        try cmdIngest(allocator, args[2..]);
    } else if (std.mem.eql(u8, command, "help")) {
        try printUsage();
    } else {
        std.debug.print("Unknown command: {s}\n", .{command});
        try printUsage();
    }
}

fn printUsage() !void {
    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buffer);
    const stdout = &stdout_writer.interface;
    try stdout.writeAll(
        \\kb - hypergraph knowledge base
        \\
        \\Usage: kb <command> [args]
        \\
        \\Commands:
        \\  ls [path]     Query the fact store
        \\  ingest        Read JSON facts from stdin
        \\  help          Show this message
        \\
    );
    try stdout.flush();
}

fn cmdLs(allocator: std.mem.Allocator, args: []const []const u8) !void {
    _ = allocator;
    _ = args;
    // TODO: implement query
    std.debug.print("ls: not yet implemented\n", .{});
}

fn cmdIngest(allocator: std.mem.Allocator, args: []const []const u8) !void {
    _ = allocator;
    _ = args;
    // TODO: implement ingest
    std.debug.print("ingest: not yet implemented\n", .{});
}
