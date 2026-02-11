const std = @import("std");
const kb = @import("kb");
const FactStore = kb.FactStore;
const Entity = kb.Entity;

const default_store_name = ".kb";

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

    if (std.mem.eql(u8, command, "get")) {
        cmdGet(allocator, args[2..]) catch |err| {
            std.debug.print("error: {}\n", .{err});
            std.process.exit(1);
        };
    } else if (std.mem.eql(u8, command, "ingest")) {
        try cmdIngest(allocator, args[2..]);
    } else if (std.mem.eql(u8, command, "datalog")) {
        try cmdDatalog(allocator, args[2..]);
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
        \\  get [path]         Query the fact store
        \\  ingest <file>      Read JSON facts from file
        \\  datalog <file>     Run Datalog rules against hypergraph
        \\  help               Show this message
        \\
    );
    try stdout.flush();
}

fn cmdGet(allocator: std.mem.Allocator, args: []const []const u8) !void {
    // Build absolute path to store
    var cwd_buf: [std.fs.max_path_bytes]u8 = undefined;
    const cwd = try std.process.getCwd(&cwd_buf);
    const store_path = try std.fs.path.joinZ(allocator, &.{ cwd, default_store_name });
    defer allocator.free(store_path);

    // Open the store
    var store = try FactStore.open(allocator, .{ .path = store_path });
    defer store.close();

    var stdout_buf: [4096]u8 = undefined;
    var stdout_writer = std.fs.File.stdout().writer(&stdout_buf);
    const stdout = &stdout_writer.interface;

    // No path: list entity types
    if (args.len == 0) {
        try listEntityTypes(&store, allocator, stdout);
        try stdout.flush();
        return;
    }

    // Parse path into entity filters
    const path = args[0];
    var filters: std.ArrayList(Entity) = .{};
    defer filters.deinit(allocator);

    // Check if path ends with / (list entities of a type)
    if (path.len > 0 and path[path.len - 1] == '/') {
        const entity_type = path[0 .. path.len - 1];
        try listEntitiesOfType(&store, allocator, stdout, entity_type);
        try stdout.flush();
        return;
    }

    // Parse type/id pairs from path
    var iter = std.mem.splitScalar(u8, path, '/');
    var project_type: ?[]const u8 = null; // Type to project results to

    while (iter.next()) |part| {
        if (part.len == 0) continue;

        // Expect type/id pairs
        const entity_type = part;
        const entity_id = iter.next() orelse {
            // Just a type with no id at the end
            if (filters.items.len == 0) {
                // No filters yet - just list entities of this type
                try listEntitiesOfType(&store, allocator, stdout, entity_type);
                try stdout.flush();
                return;
            } else {
                // Have filters - project to this type
                project_type = entity_type;
                break;
            }
        };
        if (entity_id.len == 0) {
            if (filters.items.len == 0) {
                try listEntitiesOfType(&store, allocator, stdout, entity_type);
                try stdout.flush();
                return;
            } else {
                project_type = entity_type;
                break;
            }
        }

        try filters.append(allocator, .{ .type = entity_type, .id = entity_id });
    }

    if (filters.items.len == 0) {
        try listEntityTypes(&store, allocator, stdout);
        try stdout.flush();
        return;
    }

    // Get facts matching all filters (intersection)
    try listCooccurring(&store, allocator, stdout, filters.items, project_type);
    try stdout.flush();
}

fn listEntityTypes(store: *FactStore, allocator: std.mem.Allocator, writer: anytype) !void {
    const types = store.listEntityTypes(allocator) catch |err| switch (err) {
        error.MDB_NOTFOUND => {
            try writer.print("(empty store)\n", .{});
            return;
        },
        else => return err,
    };
    defer {
        for (types) |t| allocator.free(t);
        allocator.free(types);
    }

    if (types.len == 0) {
        try writer.print("(empty store)\n", .{});
        return;
    }

    for (types) |entity_type| {
        // Count entities of this type
        const entities = try store.listEntities(entity_type, allocator);
        defer {
            for (entities) |e| allocator.free(e);
            allocator.free(entities);
        }

        try writer.print("{s}/\t\t{} entities\n", .{ entity_type, entities.len });
    }
}

fn listEntitiesOfType(store: *FactStore, allocator: std.mem.Allocator, writer: anytype, entity_type: []const u8) !void {
    const entities = try store.listEntities(entity_type, allocator);
    defer {
        for (entities) |e| allocator.free(e);
        allocator.free(entities);
    }

    for (entities) |entity_id| {
        try writer.print("{s}\n", .{entity_id});
    }
}

fn listCooccurring(store: *FactStore, allocator: std.mem.Allocator, writer: anytype, filters: []const Entity, project_type: ?[]const u8) !void {
    if (filters.len == 0) return;

    // Get fact IDs for first filter
    var fact_ids = try store.getFactsByEntity(filters[0], allocator);
    defer allocator.free(fact_ids);

    // Intersect with remaining filters
    for (filters[1..]) |filter| {
        const other_ids = try store.getFactsByEntity(filter, allocator);
        defer allocator.free(other_ids);

        // Intersect (both are sorted by insertion order, so we need to sort first)
        std.mem.sort(u64, fact_ids, {}, std.sort.asc(u64));
        std.mem.sort(u64, other_ids, {}, std.sort.asc(u64));

        var intersected: std.ArrayList(u64) = .{};
        var i: usize = 0;
        var j: usize = 0;
        while (i < fact_ids.len and j < other_ids.len) {
            if (fact_ids[i] == other_ids[j]) {
                try intersected.append(allocator, fact_ids[i]);
                i += 1;
                j += 1;
            } else if (fact_ids[i] < other_ids[j]) {
                i += 1;
            } else {
                j += 1;
            }
        }

        allocator.free(fact_ids);
        fact_ids = try intersected.toOwnedSlice(allocator);
    }

    if (fact_ids.len == 0) {
        try writer.print("(no matching facts)\n", .{});
        return;
    }

    // Collect all entities from matching facts, grouped by type
    var by_type = std.StringHashMap(std.StringHashMap(void)).init(allocator);
    defer {
        var type_iter = by_type.iterator();
        while (type_iter.next()) |entry| {
            // Free all id keys in the inner map
            var id_iter = entry.value_ptr.keyIterator();
            while (id_iter.next()) |id_key| {
                allocator.free(id_key.*);
            }
            entry.value_ptr.deinit();
            // Free the type key
            allocator.free(entry.key_ptr.*);
        }
        by_type.deinit();
    }

    for (fact_ids) |fact_id| {
        const fact = try store.getFact(fact_id, allocator) orelse continue;
        defer fact.deinit(allocator);

        for (fact.entities) |entity| {
            // Skip entities that are in our filter (don't show what we searched for)
            var is_filter = false;
            for (filters) |f| {
                if (std.mem.eql(u8, f.type, entity.type) and std.mem.eql(u8, f.id, entity.id)) {
                    is_filter = true;
                    break;
                }
            }
            if (is_filter) continue;

            // If projecting, only include matching type
            if (project_type) |pt| {
                if (!std.mem.eql(u8, entity.type, pt)) continue;
            }

            // Add to grouped results
            const type_key = try allocator.dupe(u8, entity.type);
            const gop = try by_type.getOrPut(type_key);
            if (!gop.found_existing) {
                gop.value_ptr.* = std.StringHashMap(void).init(allocator);
            } else {
                allocator.free(type_key);
            }

            const id_key = try allocator.dupe(u8, entity.id);
            const id_gop = try gop.value_ptr.getOrPut(id_key);
            if (id_gop.found_existing) {
                allocator.free(id_key);
            }
        }
    }

    // Print grouped results
    var type_iter = by_type.iterator();
    while (type_iter.next()) |entry| {
        const entity_type = entry.key_ptr.*;
        const id_set = entry.value_ptr.*;

        // Collect IDs into a list for display
        var ids: std.ArrayList([]const u8) = .{};
        defer ids.deinit(allocator);

        var id_iter = id_set.keyIterator();
        while (id_iter.next()) |id| {
            try ids.append(allocator, id.*);
        }

        // Sort for consistent output
        std.mem.sort([]const u8, ids.items, {}, struct {
            fn lessThan(_: void, a: []const u8, b: []const u8) bool {
                return std.mem.order(u8, a, b) == .lt;
            }
        }.lessThan);

        // If projecting, just list values (no type prefix)
        if (project_type != null) {
            for (ids.items) |id| {
                try writer.print("{s}\n", .{id});
            }
        } else {
            // Print: type/    id1, id2, id3
            try writer.print("{s}/\t\t", .{entity_type});
            for (ids.items, 0..) |id, i| {
                if (i > 0) try writer.print(", ", .{});
                try writer.print("{s}", .{id});
            }
            try writer.print("\n", .{});
        }
    }

    if (project_type == null) {
        try writer.print("\n{} facts\n", .{fact_ids.len});
    }
}

fn cmdIngest(allocator: std.mem.Allocator, args: []const []const u8) !void {
    // Build absolute path to store
    var cwd_buf: [std.fs.max_path_bytes]u8 = undefined;
    const cwd = try std.process.getCwd(&cwd_buf);
    const store_path = try std.fs.path.joinZ(allocator, &.{ cwd, default_store_name });
    defer allocator.free(store_path);

    var store = try FactStore.open(allocator, .{ .path = store_path });
    defer store.close();

    // Read from file (stdin not supported for now - need file arg)
    if (args.len == 0) {
        std.debug.print("Usage: kb ingest <file.jsonl>\n", .{});
        return;
    }

    const content = try std.fs.cwd().readFileAlloc(allocator, args[0], 10 * 1024 * 1024);
    defer allocator.free(content);

    var count: usize = 0;
    var line_num: usize = 0;

    var lines = std.mem.splitScalar(u8, content, '\n');
    while (lines.next()) |line| {
        line_num += 1;
        if (line.len == 0) continue;

        // Parse JSON line
        const parsed = std.json.parseFromSlice(std.json.Value, allocator, line, .{}) catch |err| {
            std.debug.print("JSON parse error on line {}: {}\n", .{ line_num, err });
            continue;
        };
        defer parsed.deinit();

        const root = parsed.value;
        if (root != .object) continue;

        // Extract edges array
        const edges_val = root.object.get("edges") orelse continue;
        if (edges_val != .array) continue;

        // Convert to entities
        var entities: std.ArrayList(Entity) = .{};
        defer entities.deinit(allocator);

        for (edges_val.array.items) |edge| {
            if (edge != .array) continue;
            if (edge.array.items.len < 2) continue;

            const type_val = edge.array.items[0];
            const id_val = edge.array.items[1];

            if (type_val != .string or id_val != .string) continue;

            try entities.append(allocator, .{
                .type = type_val.string,
                .id = id_val.string,
            });
        }

        if (entities.items.len == 0) continue;

        // Extract source
        const source: ?[]const u8 = if (root.object.get("source")) |s|
            if (s == .string) s.string else null
        else
            null;

        // Add fact
        _ = try store.addFact(entities.items, source);
        count += 1;
    }

    std.debug.print("Ingested {} facts\n", .{count});
}

fn cmdDatalog(allocator: std.mem.Allocator, args: []const []const u8) !void {
    const datalog = kb.datalog;
    const HypergraphFactSource = kb.HypergraphFactSource;

    if (args.len == 0) {
        std.debug.print("Usage: kb datalog <rules.dl>\n", .{});
        return;
    }

    // Build absolute path to store
    var cwd_buf: [std.fs.max_path_bytes]u8 = undefined;
    const cwd = try std.process.getCwd(&cwd_buf);
    const store_path = try std.fs.path.joinZ(allocator, &.{ cwd, default_store_name });
    defer allocator.free(store_path);

    var store = try kb.FactStore.open(allocator, .{ .path = store_path });
    defer store.close();

    // Read Datalog rules file
    const rules_content = try std.fs.cwd().readFileAlloc(allocator, args[0], 1024 * 1024);
    defer allocator.free(rules_content);

    // Parse rules
    var parser = datalog.Parser.init(allocator, rules_content);
    defer parser.deinit();

    const parsed = parser.parseProgram() catch |err| {
        std.debug.print("Parse error: {}\n", .{err});
        var stderr_buf: [4096]u8 = undefined;
        var stderr_writer = std.fs.File.stderr().writer(&stderr_buf);
        parser.formatError(&stderr_writer.interface) catch {};
        stderr_writer.interface.flush() catch {};
        return;
    };

    std.debug.print("Parsed {} rules, {} mappings, {} queries\n", .{
        parsed.rules.len,
        parsed.mappings.len,
        parsed.queries.len,
    });

    // Create hypergraph fact source
    var hg_source = HypergraphFactSource.init(&store, parsed.mappings);

    // Create evaluator
    var eval = datalog.Evaluator.initWithSource(allocator, parsed.rules, hg_source.source());
    defer eval.deinit();

    // Add any ground facts from rules (facts with empty body)
    for (parsed.rules) |rule| {
        if (rule.body.len == 0) {
            try eval.addFact(rule.head);
        }
    }

    // Run evaluation
    const start = std.time.milliTimestamp();
    try eval.evaluate();
    const elapsed = std.time.milliTimestamp() - start;

    std.debug.print("Evaluation complete: {} derived facts in {}ms\n", .{
        eval.derived_facts.items.len,
        elapsed,
    });

    // Run queries from the file
    for (parsed.queries) |query_atoms| {
        if (query_atoms.len == 0) continue;

        const pattern = query_atoms[0]; // Single-atom queries for now
        std.debug.print("\n?- {s}(", .{pattern.predicate});
        for (pattern.terms, 0..) |term, i| {
            if (i > 0) std.debug.print(", ", .{});
            switch (term) {
                .constant => |c| std.debug.print("\"{s}\"", .{c}),
                .variable => |v| std.debug.print("{s}", .{v}),
            }
        }
        std.debug.print(")\n", .{});

        const results = try eval.query(pattern);
        if (results.len == 0) {
            std.debug.print("  (no results)\n", .{});
        } else {
            for (results) |binding| {
                std.debug.print("  ", .{});
                var first = true;
                var iter = binding.iterator();
                while (iter.next()) |entry| {
                    if (!first) std.debug.print(", ", .{});
                    std.debug.print("{s} = {s}", .{ entry.key_ptr.*, entry.value_ptr.* });
                    first = false;
                }
                std.debug.print("\n", .{});
            }
            std.debug.print("  ({} results)\n", .{results.len});
        }
    }
}
