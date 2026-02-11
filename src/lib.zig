const std = @import("std");
const lmdb = @import("lmdb");

pub const FactStore = @import("fact_store.zig").FactStore;
pub const FactInput = FactStore.FactInput;
pub const Fact = @import("fact.zig").Fact;
pub const Entity = @import("fact.zig").Entity;

// Datalog engine
pub const datalog = @import("datalog.zig");

// Hypergraph-Datalog bridge
pub const HypergraphFactSource = @import("hypergraph_source.zig").HypergraphFactSource;

test {
    std.testing.refAllDecls(@This());
    _ = @import("datalog.zig");
    _ = @import("hypergraph_source.zig");
}
