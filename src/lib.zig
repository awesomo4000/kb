const std = @import("std");
const lmdb = @import("lmdb");

pub const FactStore = @import("fact_store.zig").FactStore;
pub const FactInput = FactStore.FactInput;
pub const Fact = @import("fact.zig").Fact;
pub const Entity = @import("fact.zig").Entity;

// Datalog engine
pub const datalog = @import("datalog.zig");

// String interner for bitmap evaluator
pub const StringInterner = @import("string_interner.zig").StringInterner;

// Relation types for bitmap evaluator
pub const relation = @import("relation.zig");

// Fact fetcher interface for bitmap evaluator
pub const fact_fetcher = @import("fact_fetcher.zig");

// Bitmap ingest pipeline
pub const bitmap_ingest = @import("bitmap_ingest.zig");

// Bitmap evaluator
pub const bitmap_evaluator = @import("bitmap_evaluator.zig");

// Stratification for negation
pub const stratify = @import("stratify.zig");

// Entity key encoding
pub const entity_key = @import("entity_key.zig");

test {
    std.testing.refAllDecls(@This());
    _ = @import("datalog.zig");
    _ = @import("string_interner.zig");
    _ = @import("relation.zig");
    _ = @import("fact_fetcher.zig");
    _ = @import("bitmap_ingest.zig");
    _ = @import("bitmap_evaluator.zig");
    _ = @import("stratify.zig");
    _ = @import("entity_key.zig");
    _ = @import("test_helpers.zig");
}
