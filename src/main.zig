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
    const batch_size: usize = 50_000;

    var cwd_buf: [std.fs.max_path_bytes]u8 = undefined;
    const cwd = try std.process.getCwd(&cwd_buf);
    const store_path = try std.fs.path.joinZ(allocator, &.{ cwd, default_store_name });
    defer allocator.free(store_path);

    // Use no_sync for bulk ingest - much faster, safe since data can be re-ingested
    var store = try FactStore.open(allocator, .{ .path = store_path, .no_sync = true });
    defer store.close();

    if (args.len == 0) {
        std.debug.print("Usage: kb ingest <file.jsonl>\n", .{});
        return;
    }

    // Read entire file - memory mapped by OS, efficient for large files
    const content = try std.fs.cwd().readFileAlloc(allocator, args[0], std.math.maxInt(usize));
    defer allocator.free(content);

    // Arena for current batch - reset after each flush
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    var batch: std.ArrayList(FactStore.FactInput) = .{};
    var total: usize = 0;
    var line_num: usize = 0;

    var lines = std.mem.splitScalar(u8, content, '\n');
    while (lines.next()) |line| {
        line_num += 1;
        if (line.len == 0) continue;

        const arena_alloc = arena.allocator();

        // Parse JSON line
        const parsed = std.json.parseFromSlice(std.json.Value, arena_alloc, line, .{}) catch |err| {
            std.debug.print("JSON parse error on line {}: {}\n", .{ line_num, err });
            continue;
        };

        const root = parsed.value;
        if (root != .object) continue;

        const edges_val = root.object.get("edges") orelse continue;
        if (edges_val != .array) continue;

        var entities: std.ArrayList(Entity) = .{};

        for (edges_val.array.items) |edge| {
            if (edge != .array) continue;
            if (edge.array.items.len < 2) continue;

            const type_val = edge.array.items[0];
            const id_val = edge.array.items[1];

            if (type_val != .string or id_val != .string) continue;

            try entities.append(arena_alloc, .{
                .type = type_val.string,
                .id = id_val.string,
            });
        }

        if (entities.items.len == 0) continue;

        const source: ?[]const u8 = if (root.object.get("source")) |s|
            if (s == .string) s.string else null
        else
            null;

        try batch.append(arena_alloc, .{
            .entities = entities.items,
            .source = source,
        });

        // Flush batch when full
        if (batch.items.len >= batch_size) {
            const ids = try store.addFacts(batch.items);
            allocator.free(ids);
            total += batch.items.len;
            std.debug.print("Ingested {} facts...\n", .{total});

            // Reset arena and batch for next chunk
            _ = arena.reset(.retain_capacity);
            batch = .{};
        }
    }

    // Flush remaining
    if (batch.items.len > 0) {
        const ids = try store.addFacts(batch.items);
        allocator.free(ids);
        total += batch.items.len;
    }

    std.debug.print("Ingested {} facts\n", .{total});
}

/// Accumulated parse results from a file and all its includes
const AccumulatedParse = struct {
    rules: std.ArrayList(kb.datalog.Rule),
    queries: std.ArrayList([]kb.datalog.Atom),
    mappings: std.ArrayList(kb.datalog.Mapping),
    parsers: std.ArrayList(kb.datalog.Parser), // Keep parsers alive (they own the memory)
    file_contents: std.ArrayList([]const u8), // Keep file contents alive
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator) AccumulatedParse {
        return .{
            .rules = .{},
            .queries = .{},
            .mappings = .{},
            .parsers = .{},
            .file_contents = .{},
            .allocator = allocator,
        };
    }

    fn deinit(self: *AccumulatedParse) void {
        // Deinit parsers (which frees the memory for rules/queries/mappings)
        for (self.parsers.items) |*p| {
            p.deinit();
        }
        self.parsers.deinit(self.allocator);
        // Free file contents
        for (self.file_contents.items) |content| {
            self.allocator.free(content);
        }
        self.file_contents.deinit(self.allocator);
        self.rules.deinit(self.allocator);
        self.queries.deinit(self.allocator);
        self.mappings.deinit(self.allocator);
    }
};

/// Parse a Datalog file and recursively process @include directives
fn parseDatalogWithIncludes(
    allocator: std.mem.Allocator,
    file_path: []const u8,
    visited: *std.StringHashMap(void),
    result: *AccumulatedParse,
) !void {
    // Resolve to absolute path for cycle detection
    const abs_path = try std.fs.cwd().realpathAlloc(allocator, file_path);
    defer allocator.free(abs_path);

    // Check for cycles
    if (visited.contains(abs_path)) {
        return; // Already included, skip
    }
    try visited.put(try allocator.dupe(u8, abs_path), {});

    // Read file content (keep it alive - parser references it)
    const content = std.fs.cwd().readFileAlloc(allocator, file_path, 1024 * 1024) catch |err| {
        std.debug.print("Error reading {s}: {}\n", .{ file_path, err });
        return err;
    };
    // Don't defer free - content is owned by parser arena

    // Parse - keep parser alive, it owns the memory for rules/mappings/queries
    var parser = kb.datalog.Parser.init(allocator, content);

    const parsed = parser.parseProgram() catch |err| {
        std.debug.print("Parse error in {s}: {}\n", .{ file_path, err });
        var stderr_buf: [4096]u8 = undefined;
        var stderr_writer = std.fs.File.stderr().writer(&stderr_buf);
        parser.formatError(&stderr_writer.interface) catch {};
        stderr_writer.interface.flush() catch {};
        parser.deinit();
        allocator.free(content);
        return err;
    };

    // Store parser and content so they stay alive
    try result.parsers.append(allocator, parser);
    try result.file_contents.append(allocator, content);

    // Get directory of current file for resolving relative includes
    const dir = std.fs.path.dirname(file_path) orelse ".";

    // Process includes first (depth-first)
    for (parsed.includes) |include_path| {
        const resolved = try std.fs.path.join(allocator, &.{ dir, include_path });
        defer allocator.free(resolved);
        try parseDatalogWithIncludes(allocator, resolved, visited, result);
    }

    // Add rules, queries, mappings from this file
    try result.rules.appendSlice(allocator, parsed.rules);
    try result.queries.appendSlice(allocator, parsed.queries);
    try result.mappings.appendSlice(allocator, parsed.mappings);
}

fn cmdDatalog(allocator: std.mem.Allocator, args: []const []const u8) !void {
    const BitmapEvaluator = kb.bitmap_evaluator.BitmapEvaluator;
    const HypergraphFetcher = kb.fact_fetcher.HypergraphFetcher;

    if (args.len == 0) {
        std.debug.print("Usage: kb datalog [--profile] <rules.dl>\n", .{});
        return;
    }

    // Parse flags
    var profile_enabled = false;
    var file_idx: usize = 0;
    for (args, 0..) |arg, i| {
        if (std.mem.eql(u8, arg, "--profile")) {
            profile_enabled = true;
        } else {
            file_idx = i;
            break;
        }
    }

    if (file_idx >= args.len) {
        std.debug.print("Usage: kb datalog [--profile] <rules.dl>\n", .{});
        return;
    }

    // Parse rules file with include handling
    var visited = std.StringHashMap(void).init(allocator);
    defer {
        var it = visited.keyIterator();
        while (it.next()) |key| {
            allocator.free(key.*);
        }
        visited.deinit();
    }

    var accumulated = AccumulatedParse.init(allocator);
    defer accumulated.deinit();

    parseDatalogWithIncludes(allocator, args[file_idx], &visited, &accumulated) catch |err| {
        std.debug.print("Failed to parse: {}\n", .{err});
        return;
    };

    std.debug.print("Parsed {} rules, {} mappings, {} queries\n", .{
        accumulated.rules.items.len,
        accumulated.mappings.items.len,
        accumulated.queries.items.len,
    });

    // Create bitmap evaluator
    var eval = BitmapEvaluator.init(allocator, accumulated.rules.items);
    defer eval.deinit();

    // Load @map facts from LMDB if mappings exist
    if (accumulated.mappings.items.len > 0) {
        // Build absolute path to store
        var cwd_buf: [std.fs.max_path_bytes]u8 = undefined;
        const cwd = try std.process.getCwd(&cwd_buf);
        const store_path = try std.fs.path.joinZ(allocator, &.{ cwd, default_store_name });
        defer allocator.free(store_path);

        var store = FactStore.open(allocator, .{ .path = store_path }) catch {
            std.debug.print("Error: @map directives require a fact store (.kb directory)\n", .{});
            std.debug.print("Run 'kb ingest <file>' first to create the store.\n", .{});
            return;
        };
        defer store.close();

        var hg_fetcher = HypergraphFetcher.init(&store);
        try eval.loadMappedFacts(
            hg_fetcher.fetcher(),
            accumulated.mappings.items,
        );
    }

    // Add ground facts from .dl
    try eval.addGroundFacts(accumulated.rules.items);

    // Run evaluation
    var timer = try std.time.Timer.start();
    eval.evaluate() catch |err| {
        if (err == error.UnstratifiableProgram) {
            std.debug.print("error: program contains circular negation and cannot be stratified\n", .{});
            std.process.exit(1);
        }
        if (err == error.UnsafeNegation) {
            std.process.exit(1);
        }
        return err;
    };
    const elapsed_ns = timer.read();

    if (profile_enabled) {
        std.debug.print("Relations: {}\n", .{eval.relations.count()});
    }

    std.debug.print("Evaluation complete in {d:.3}ms\n", .{
        @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0,
    });

    // Run queries from the file
    for (accumulated.queries.items) |query_atoms| {
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
        defer eval.freeQueryResults(results);

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
