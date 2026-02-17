# Step 11: Entity Key Abstraction + Typed Value Encoding

Introduces `entity_key.zig` — a single module that owns all entity key
encoding, decoding, and comparison. Replaces the raw `\x00`-delimited key
format with a typed, binary-safe, order-preserving encoding. All existing
callsites migrate to use this module, so the internal encoding can be changed
in one place without touching the rest of the codebase.

---

## 0. Motivation

The current entity key format is `"type\x00id"` — entity type, null byte,
entity ID as a raw string. This has three problems:

1. **Not binary-safe.** If an entity ID contains `\x00` (binary SIDs, raw
   IPv4, packed integers), the key is silently truncated at the null byte.
   Produces collisions that are nearly impossible to debug.

2. **No type information.** Every value is stored as a string. Comparing
   `port > 443` requires parsing strings to integers at runtime — slow,
   fragile, and `"9" > "443"` is lexicographically true but semantically
   wrong.

3. **Scattered encoding logic.** The `type\x00id` format is constructed in
   4 separate locations with inline byte manipulation. Changing the format
   means coordinated edits across all of them.

This step solves all three: one module, one encoding, full type support.

---

## 1. Scope

**In scope:**

- New `src/entity_key.zig` module with `Value`, `ValueType`, encode/decode
- Migrate all 4 existing `\x00` key construction sites to use entity_key
- Update `Entity.toKey()` to delegate to entity_key
- Order-preserving value encodings (free sorted iteration from LMDB)
- Unit tests for all value types, encode/decode roundtrips, sort order

**Out of scope:**

- Changing the evaluator (it uses bare strings, unaffected)
- Changing the Datalog parser or AST
- LMDB data migration (new installs use new format; migration is a later step)
- Comparison operator implementation (next step, builds on this)

---

## 2. Key Format

```
[entity_type bytes]\x00[value_type: u8][encoded value bytes to end]
```

**Entity type** is always a short ASCII label we control (`"author"`, `"port"`,
`"host"`). Never contains `\x00`, so the null terminator is safe. Putting it
first means LMDB prefix scans for "all ports" or "all authors" are a direct
cursor seek to the entity type string — no skipping a type byte first.

**Finding the `\x00`** boundary is a single `std.mem.indexOfScalar(u8, key, 0)`
call. For 4-8 byte entity types, this fits in a single SIMD register and
completes in one comparison cycle on modern hardware.

**value_type** is one byte telling you how to parse everything that follows.

**No length prefixes anywhere.** The value is always the terminal field in
the key. Fixed-width types (u16, u32, i64, etc.) know their size from the
type tag via `fixedLen()`. Variable-width types (string, bytes) consume
everything from the type tag to the end of the key:
`value_bytes = raw[separator_pos + 2 ..]`. This means:

- Zero overhead for variable-length values
- Strings sort in pure lexicographic order under `memcmp` (not length-first)
- Decoding is trivial: find `\x00`, read type tag, rest is the value

**LMDB sort order with this layout:**

```
"author" \x00 0x01 "Dante"
"author" \x00 0x01 "Homer"
"author" \x00 0x01 "Virgil"
"book"   \x00 0x01 "Iliad"
"book"   \x00 0x01 "Odyssey"
"host"   \x00 0x07 [0A 00 00 01]          -- 10.0.0.1
"host"   \x00 0x07 [C0 A8 01 01]          -- 192.168.1.1
"port"   \x00 0x02 [00 50]                -- 80
"port"   \x00 0x02 [01 BB]                -- 443
"port"   \x00 0x02 [1F 90]                -- 8080
```

Groups by entity type (the semantic concept you'd query on), then values in
correct semantic order within each group. LMDB's `memcmp` gives us all of
this for free — no custom comparators, no post-sort. This is the same
approach FoundationDB's tuple layer and CockroachDB's ordered encoding use.

---

## 3. The Value Type System

```zig
pub const ValueType = enum(u8) {
    string   = 0x01,  // UTF-8 text (the common case today)
    u16_val  = 0x02,  // port numbers, small counts
    u32_val  = 0x03,  // IPv4 (as integer), medium integers
    i64_val  = 0x04,  // timestamps, signed values
    u64_val  = 0x05,  // file sizes, hashes, large unsigned
    uuid     = 0x06,  // 16-byte UUIDs
    ipv4     = 0x07,  // 4 bytes (same encoding as u32, distinct semantics)
    ipv6     = 0x08,  // 16-byte IPv6 addresses
    bytes    = 0x09,  // arbitrary binary blob

    /// Number of bytes the encoded value occupies (excluding the type tag).
    /// Returns null for variable-length types (string, bytes) — these
    /// consume everything to the end of the key.
    pub fn fixedLen(self: ValueType) ?usize {
        return switch (self) {
            .u16_val => 2,
            .u32_val, .ipv4 => 4,
            .i64_val, .u64_val => 8,
            .uuid, .ipv6 => 16,
            .string, .bytes => null,
        };
    }
};
```

```zig
pub const Value = union(enum) {
    string: []const u8,
    u16_val: u16,
    u32_val: u32,
    i64_val: i64,
    u64_val: u64,
    uuid: [16]u8,
    ipv4: u32,       // stored as u32 BE for network byte order
    ipv6: [16]u8,
    bytes: []const u8,

    /// Returns the ValueType tag for this value.
    pub fn valueType(self: Value) ValueType {
        return switch (self) {
            .string  => .string,
            .u16_val => .u16_val,
            .u32_val => .u32_val,
            .i64_val => .i64_val,
            .u64_val => .u64_val,
            .uuid    => .uuid,
            .ipv4    => .ipv4,
            .ipv6    => .ipv6,
            .bytes   => .bytes,
        };
    }
};
```

### Why these types

| Type | Use case | Example |
|---|---|---|
| `string` | All current data, names, identifiers | `"Homer"`, `"10.0.0.5"` |
| `u16_val` | Port numbers, small enums | `443`, `80` |
| `u32_val` | IPv4 as integer, counters | `167772161` (10.0.0.1) |
| `i64_val` | Unix timestamps, signed metrics | `1708000000`, `-42` |
| `u64_val` | File sizes, hash values | `18446744073709551615` |
| `uuid` | AD objectGUID, correlation IDs | `550e8400-e29b-41d4-...` |
| `ipv4` | IPv4 addresses (semantic alias) | `10.0.0.1` → 4 bytes BE |
| `ipv6` | IPv6 addresses | `::1` → 16 bytes |
| `bytes` | Binary blobs, objectSid | arbitrary `[]u8` |

For this step, all existing data continues to use `Value.string`. The other
types become available for future ingest pipelines and the comparison
operator implementation.

---

## 4. Order-Preserving Value Encodings

All encodings are chosen so that LMDB's byte-order comparison (`memcmp`)
produces correct semantic ordering. No custom comparators needed.

**No length prefixes.** The value is always the last field in the key, so
its length is implicit: fixed-width types know their size from the type tag,
variable-width types consume everything remaining. This gives strings pure
lexicographic sort order (not length-first).

| ValueType | Encoding | Bytes | Sort property |
|---|---|---|---|
| `string` (0x01) | raw UTF-8 bytes | len | Lexicographic |
| `u16_val` (0x02) | 2 bytes BE | 2 | Numeric |
| `u32_val` (0x03) | 4 bytes BE | 4 | Numeric |
| `i64_val` (0x04) | 8 bytes BE, sign bit flipped | 8 | Numeric (neg < pos) |
| `u64_val` (0x05) | 8 bytes BE | 8 | Numeric |
| `uuid` (0x06) | 16 bytes raw | 16 | Byte order |
| `ipv4` (0x07) | 4 bytes BE | 4 | Network order |
| `ipv6` (0x08) | 16 bytes raw | 16 | Byte order |
| `bytes` (0x09) | raw bytes | len | Byte order |

**Big-endian integers.** Unsigned integers stored BE sort correctly under
`memcmp`. `80` as u16 BE is `0x0050`, `443` is `0x01BB`. Byte comparison:
`0x00 < 0x01` → 80 < 443. Correct. The rest of the codebase already uses BE
for integer encoding (fact serialization, interner persistence), so this is
consistent.

**The sign-bit flip for i64.** XOR the high bit of the big-endian
representation. This maps `i64.MIN` → `0x0000000000000000` and `i64.MAX` →
`0xFFFFFFFFFFFFFFFF`, so negative numbers sort before positive numbers in
byte comparison. Used by CockroachDB, FoundationDB, and SQLite.

```zig
fn encodeI64(val: i64) [8]u8 {
    const as_u64: u64 = @bitCast(val);
    const flipped = as_u64 ^ (@as(u64, 1) << 63);
    return std.mem.toBytes(std.mem.nativeToBig(u64, flipped));
}

fn decodeI64(bytes: [8]u8) i64 {
    const be_val = std.mem.bigToNative(u64, std.mem.bytesToValue(u64, &bytes));
    const unflipped = be_val ^ (@as(u64, 1) << 63);
    return @bitCast(unflipped);
}
```

**Strings: raw bytes, no prefix.** Strings are written directly after the
type tag with no length prefix. Since the value is always the terminal field,
length is derived from the key's total size. `memcmp` on the raw UTF-8 bytes
gives pure lexicographic ordering: `"Amy" < "Homer" < "Jo" < "Virgil"`.

### Decoding logic

```zig
pub fn fromBytes(raw: []const u8) !EntityKey {
    const sep = std.mem.indexOfScalar(u8, raw, 0) orelse return error.InvalidKey;
    if (sep + 1 >= raw.len) return error.InvalidKey;
    const type_tag: ValueType = @enumFromInt(raw[sep + 1]);
    const value_bytes = raw[sep + 2 ..];

    // For fixed-width types, validate length matches expected size
    if (type_tag.fixedLen()) |expected| {
        if (value_bytes.len != expected) return error.InvalidKey;
    }
    // For variable-width types (string, bytes), value_bytes is the entire value

    return EntityKey{ .raw = raw };
}
```

### Example encodings

**`("author", "Homer")` — current common case:**
```
61 75 74 68 6F 72  00  01  48 6F 6D 65 72
└── "author" ───┘  ╵   ╵   └── "Homer" ──┘
            null sep  str
```
13 bytes. Only 1 byte overhead vs today's format (the type tag).

**`("port", 443 as u16)` — future typed ingest:**
```
70 6F 72 74  00  02  01 BB
└ "port" ─┘  ╵   ╵  └443┘
         null   u16   BE
```
8 bytes. Compact, directly comparable, sorts numerically.

**`("host", 10.0.0.1 as ipv4)` — future network data:**
```
68 6F 73 74  00  07  0A 00 00 01
└ "host" ─┘  ╵   ╵  └10.0.0.1─┘
         null  ipv4   BE
```
10 bytes. Sorts in network order.

---

## 5. Public Interface

The module exposes an opaque key type and functions to encode/decode. Callers
never construct key bytes directly.

```zig
// src/entity_key.zig

/// An encoded entity key. Bytes are borrowed — they reference the buffer
/// passed to encode() or an LMDB value slice passed to fromBytes().
pub const EntityKey = struct {
    raw: []const u8,

    /// Decode the entity type portion (bytes before the \x00 separator).
    pub fn entityType(self: EntityKey) []const u8;

    /// Decode the value portion (after the \x00 separator + type tag).
    pub fn value(self: EntityKey) Value;

    /// Return the ValueType tag without decoding the full value.
    pub fn valueType(self: EntityKey) ValueType;

    /// Raw encoded bytes for LMDB storage or interner key.
    pub fn asBytes(self: EntityKey) []const u8;
};

/// Encode an entity key into the provided buffer.
/// Returns an EntityKey referencing the buffer. Returns error if buffer
/// is too small.
pub fn encode(
    buf: []u8,
    entity_type: []const u8,
    val: Value,
) !EntityKey;

/// Decode an EntityKey from raw LMDB bytes.
/// The returned EntityKey borrows the input slice.
pub fn fromBytes(raw: []const u8) !EntityKey;

/// Convenience: encode an entity type + bare string value.
/// This is the migration path — all existing callsites use this.
pub fn encodeString(
    buf: []u8,
    entity_type: []const u8,
    id: []const u8,
) !EntityKey;

/// Compare two encoded values of the same ValueType semantically.
/// For types with order-preserving encoding, this is equivalent to
/// memcmp on the value bytes — but this function handles cross-type
/// comparison and decodes when needed.
pub fn compareValues(a: EntityKey, b: EntityKey) std.math.Order;
```

### Stack buffer convention

All existing callsites already use 512-byte stack buffers. The new module
continues this pattern. No heap allocation in the hot path.

```zig
// Typical usage at a callsite:
var buf: [512]u8 = undefined;
const key = try entity_key.encodeString(&buf, entity.type, entity.id);
try by_entity_db.set(key.asBytes(), &fact_id_bytes);
```

The `encodeString` convenience function covers all existing callsites with
minimal diff.

---

## 6. Existing Callsites to Migrate

### 6a. `fact.zig` — `Entity.toKey()`

**Current:**
```zig
pub fn toKey(self: Entity, allocator: std.mem.Allocator) ![]u8 {
    const key = try allocator.alloc(u8, self.type.len + 1 + self.id.len);
    @memcpy(key[0..self.type.len], self.type);
    key[self.type.len] = 0;
    @memcpy(key[self.type.len + 1 ..], self.id);
    return key;
}
```

**After:** Delegate to entity_key. Keep the allocating signature for backward
compatibility, but implement via the new encoding:

```zig
const entity_key_mod = @import("entity_key.zig");

pub fn toKey(self: Entity, allocator: std.mem.Allocator) ![]u8 {
    var buf: [512]u8 = undefined;
    const ek = try entity_key_mod.encodeString(&buf, self.type, self.id);
    return allocator.dupe(u8, ek.asBytes());
}
```

Also add a non-allocating convenience for the stack-buffer pattern:

```zig
pub fn toKeyBuf(self: Entity, buf: []u8) !entity_key_mod.EntityKey {
    return entity_key_mod.encodeString(buf, self.type, self.id);
}
```

### 6b. `fact_store.zig` — `addFacts()` inline key construction

**Current (lines ~97-109):**
```zig
var key_buf: [512]u8 = undefined;
const entity_key = blk: {
    const total_len = entity.type.len + 1 + entity.id.len;
    if (total_len <= key_buf.len) {
        @memcpy(key_buf[0..entity.type.len], entity.type);
        key_buf[entity.type.len] = 0;
        @memcpy(key_buf[entity.type.len + 1 ..][0..entity.id.len], entity.id);
        break :blk key_buf[0..total_len];
    } else {
        // heap fallback ...
    }
};
```

**After:**
```zig
const ek = @import("entity_key.zig");
// ...
var key_buf: [512]u8 = undefined;
const key = try ek.encodeString(&key_buf, entity.type, entity.id);
try by_entity_db.set(key.asBytes(), &fact_id_bytes);
try fact_edges_db.set(&fact_id_bytes, key.asBytes());
```

The heap fallback is eliminated — the new encoding adds only 1 byte overhead
per key (the type tag), so the 512-byte buffer handles entity IDs up to ~500
bytes. If we ever need longer, the `encode` function returns
`error.BufferTooSmall`.

### 6c. `fact_store.zig` — `getFactsByEntity()` inline key construction

Same pattern as 6b. Replace with:

```zig
var key_buf: [512]u8 = undefined;
const key = try ek.encodeString(&key_buf, entity.type, entity.id);
var value = try cursor.goToKeyValue(key.asBytes());
```

### 6d. `bitmap_ingest.zig` — `internEntity()`

**Current:**
```zig
fn internEntity(interner: *StringInterner, entity: Entity) !u32 {
    var buf: [512]u8 = undefined;
    const total_len = entity.type.len + 1 + entity.id.len;
    if (total_len > buf.len) {
        @panic("entity key too long for stack buffer");
    }
    @memcpy(buf[0..entity.type.len], entity.type);
    buf[entity.type.len] = 0;
    @memcpy(buf[entity.type.len + 1 ..][0..entity.id.len], entity.id);
    const key = buf[0..total_len];
    return interner.intern(key);
}
```

**After:**
```zig
const ek = @import("entity_key.zig");

fn internEntity(interner: *StringInterner, entity: Entity) !u32 {
    var buf: [512]u8 = undefined;
    const key = try ek.encodeString(&buf, entity.type, entity.id);
    return interner.intern(key.asBytes());
}
```

### 6e. Evaluator — NOT changed

`bitmap_evaluator.zig` `loadMappedFacts()` interns bare `entity.id` strings
(no type prefix). This is deliberate — the evaluator namespace uses bare
strings so Datalog constants like `"Homer"` match. This callsite is **not
migrated** and continues to use bare strings.

The entity_key module is for the persistence/indexing layer only. The
evaluator will interact with typed values when comparisons are implemented
(next step), through a different path — comparing decoded `Value`s, not
encoded entity keys.

---

## 7. Implementation Plan

### Step A: Create `entity_key.zig` with `Value`, `ValueType`, encode/decode

New file. Pure functions, no LMDB dependency. Fully testable in isolation.

Implement:
- `ValueType` enum with `fixedLen()`
- `Value` tagged union with `valueType()`
- `encode()` — writes entity_type + `\x00` + type_tag + encoded value to buffer
- `encodeString()` — convenience wrapper for the string case
- `fromBytes()` — finds `\x00` via `indexOfScalar`, reads type tag, decodes value
- `EntityKey` struct with `entityType()`, `value()`, `valueType()`, `asBytes()`
- `compareValues()` — type-aware comparison

The i64 sign-bit flip:
```zig
fn encodeI64(val: i64) [8]u8 {
    const as_u64: u64 = @bitCast(val);
    const flipped = as_u64 ^ (@as(u64, 1) << 63);
    return std.mem.toBytes(std.mem.nativeToBig(u64, flipped));
}

fn decodeI64(bytes: [8]u8) i64 {
    const be_val = std.mem.bigToNative(u64, std.mem.bytesToValue(u64, &bytes));
    const unflipped = be_val ^ (@as(u64, 1) << 63);
    return @bitCast(unflipped);
}
```

### Step B: Migrate `fact.zig` `Entity.toKey()`

Change `toKey` to delegate to `entity_key.encodeString`. Add `toKeyBuf` for
the stack-buffer pattern. Update the `"entity toKey"` test.

### Step C: Migrate `fact_store.zig`

Replace inline key construction in `addFacts()` and `getFactsByEntity()` with
`entity_key.encodeString()`. Remove the heap-fallback `blk:` blocks. Import
entity_key.

### Step D: Migrate `bitmap_ingest.zig` `internEntity()`

Replace inline byte manipulation with `entity_key.encodeString()`. Update the
`"internEntity uses type-null-id format"` test.

### Step E: Wire into `lib.zig`, update tests

Export entity_key from lib.zig. Update any integration tests that depend on
the exact byte format of entity keys. Run `zig build test` and
`zig build test-all`.

---

## 8. Tests

### 8a. Unit tests in `entity_key.zig`

```zig
test "encode/decode string roundtrip" {
    var buf: [512]u8 = undefined;
    const key = try encodeString(&buf, "author", "Homer");
    try expectEqualStrings("author", key.entityType());
    try expectEqual(ValueType.string, key.valueType());
    try expectEqualStrings("Homer", key.value().string);
}

test "encode/decode u16 roundtrip" {
    var buf: [512]u8 = undefined;
    const key = try encode(&buf, "port", .{ .u16_val = 443 });
    try expectEqual(ValueType.u16_val, key.valueType());
    try expectEqual(@as(u16, 443), key.value().u16_val);
}

test "encode/decode i64 roundtrip — negative values" {
    var buf: [512]u8 = undefined;
    const key = try encode(&buf, "offset", .{ .i64_val = -42 });
    try expectEqual(@as(i64, -42), key.value().i64_val);
}

test "encode/decode ipv4 roundtrip" {
    var buf: [512]u8 = undefined;
    const key = try encode(&buf, "host", .{ .ipv4 = 0x0A000001 }); // 10.0.0.1
    try expectEqual(@as(u32, 0x0A000001), key.value().ipv4);
}

test "encode/decode uuid roundtrip" {
    var buf: [512]u8 = undefined;
    const test_uuid = [16]u8{ 0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
                               0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00 };
    const key = try encode(&buf, "guid", .{ .uuid = test_uuid });
    try expectEqualSlices(u8, &test_uuid, &key.value().uuid);
}

test "encode/decode bytes roundtrip — with null bytes" {
    var buf: [512]u8 = undefined;
    const val_with_null = "abc\x00def";
    const key = try encode(&buf, "blob", .{ .bytes = val_with_null });
    try expectEqualSlices(u8, val_with_null, key.value().bytes);
}

test "string sort order — lexicographic" {
    var buf_a: [512]u8 = undefined;
    var buf_b: [512]u8 = undefined;
    var buf_c: [512]u8 = undefined;
    const a = try encodeString(&buf_a, "x", "Amy");
    const b = try encodeString(&buf_b, "x", "Homer");
    const c = try encodeString(&buf_c, "x", "Jo");
    // Pure lexicographic: Amy < Homer < Jo (not length-first)
    try expect(std.mem.order(u8, a.asBytes(), b.asBytes()) == .lt);
    try expect(std.mem.order(u8, b.asBytes(), c.asBytes()) == .lt);
}

test "u16 sort order — numeric" {
    var buf_a: [512]u8 = undefined;
    var buf_b: [512]u8 = undefined;
    const a = try encode(&buf_a, "port", .{ .u16_val = 80 });
    const b = try encode(&buf_b, "port", .{ .u16_val = 443 });
    try expect(std.mem.order(u8, a.asBytes(), b.asBytes()) == .lt);
}

test "i64 sort order — MIN < -1 < 0 < 1 < MAX" {
    var bufs: [5][512]u8 = undefined;
    const vals = [_]i64{ std.math.minInt(i64), -1, 0, 1, std.math.maxInt(i64) };
    var keys: [5]EntityKey = undefined;
    for (vals, 0..) |v, i| {
        keys[i] = try encode(&bufs[i], "v", .{ .i64_val = v });
    }
    for (0..4) |i| {
        try expect(std.mem.order(u8, keys[i].asBytes(), keys[i + 1].asBytes()) == .lt);
    }
}

test "entity type grouping — prefix scan friendly" {
    var buf_a: [512]u8 = undefined;
    var buf_b: [512]u8 = undefined;
    var buf_c: [512]u8 = undefined;
    const a1 = try encodeString(&buf_a, "author", "Zebra");
    const a2 = try encodeString(&buf_b, "author", "Alpha");
    const b1 = try encodeString(&buf_c, "book", "Alpha");
    // All "author" keys sort before all "book" keys
    try expect(std.mem.order(u8, a1.asBytes(), b1.asBytes()) == .lt);
    try expect(std.mem.order(u8, a2.asBytes(), b1.asBytes()) == .lt);
    // Within "author", sorts by value
    try expect(std.mem.order(u8, a2.asBytes(), a1.asBytes()) == .lt);
}

test "ipv4 sort order — network order" {
    var buf_a: [512]u8 = undefined;
    var buf_b: [512]u8 = undefined;
    const a = try encode(&buf_a, "host", .{ .ipv4 = 0x0A000001 }); // 10.0.0.1
    const b = try encode(&buf_b, "host", .{ .ipv4 = 0xC0A80101 }); // 192.168.1.1
    try expect(std.mem.order(u8, a.asBytes(), b.asBytes()) == .lt);
}

test "compareValues — string comparison" {
    var buf_a: [512]u8 = undefined;
    var buf_b: [512]u8 = undefined;
    const a = try encodeString(&buf_a, "x", "apple");
    const b = try encodeString(&buf_b, "x", "banana");
    try expect(compareValues(a, b) == .lt);
}

test "compareValues — cross-type returns type tag order" {
    var buf_a: [512]u8 = undefined;
    var buf_b: [512]u8 = undefined;
    const a = try encodeString(&buf_a, "x", "hello");
    const b = try encode(&buf_b, "x", .{ .u16_val = 42 });
    // string (0x01) < u16 (0x02)
    try expect(compareValues(a, b) == .lt);
}

test "buffer too small returns error" {
    var buf: [5]u8 = undefined;
    try expectError(error.BufferTooSmall, encodeString(&buf, "author", "Homer"));
}
```

### 8b. Updated tests in migrated files

The test `"internEntity uses type-null-id format"` in `bitmap_ingest.zig`
changes. Currently it asserts:

```zig
try expectEqualStrings("author\x00Homer", interner.resolve(id));
```

After migration, the interned key includes the type tag.
Update to decode through entity_key:

```zig
const resolved = interner.resolve(id);
const decoded = try entity_key.fromBytes(resolved);
try expectEqualStrings("author", decoded.entityType());
try expectEqualStrings("Homer", decoded.value().string);
```

Similarly, the `"entity toKey"` test in `fact.zig` updates to verify the new
format through decode rather than raw string comparison.

Tests in `bitmap_ingest.zig` that do `interner.lookup("author\x00Virgil")`
change to use `entity_key.encodeString` to build the lookup key.

---

## 9. What Does NOT Change

- **`bitmap_evaluator.zig`** — evaluator uses bare strings. Unaffected.
- **`string_interner.zig`** — stores opaque byte slices. Unaffected (it
  just stores longer byte slices now).
- **`datalog.zig`** — parser, AST, lexer. Unaffected.
- **`stratify.zig`** — stratification. Unaffected.
- **`relation.zig`** — bitmap relations store u32 IDs. Unaffected.
- **`fact.zig` `Fact.serialize/deserialize`** — fact record encoding already
  uses u16 length-prefixed fields. Unaffected.
- **`fact_fetcher.zig`** — fetcher interface. Unaffected.

---

## 10. LMDB Data Compatibility

This step changes the entity key encoding. Existing LMDB databases created
with the old `\x00` format will not be readable with the new code without
migration.

**For now:** This is acceptable. The kb project is pre-1.0, there are no
production databases. Document the breaking change in the PR description.
Users re-run `kb ingest` to rebuild.

**Future migration path (not this step):** Add a version marker to the LMDB
metadata database. On open, check the version. If old format, run a migration
that reads all entity keys, decodes as old format, re-encodes as new format,
and writes back. Bump the version marker.

---

## 11. How This Enables Future Work

### Comparison operators (next step)

With entity_key in place, the comparison operator implementation becomes
straightforward:

1. **Parser** adds `<`, `>`, `<=`, `>=`, `=`, `!=` tokens and `Comparison`
   body element (already designed in datalog-extensions.md).

2. **Evaluator** resolves comparison operands to `Value`s. For ground-fact
   constants (strings from `.dl` files), attempt numeric parsing:
   - If both sides parse as integers → compare as `i64`
   - Otherwise → compare as strings

3. **For @map data with typed storage,** the `Value` is already typed —
   a port ingested as `u16_val` compares numerically without parsing.

4. **`entity_key.compareValues()`** provides the comparison primitive.

### Range queries via sorted IDs (future)

Because the entity key encoding is order-preserving, a future optimization
can assign interner IDs in key-sorted order instead of insertion order. This
would make related entities contiguous in the u32 ID space, which means:

- "All ports" is a single range `[first_port_id, last_port_id]`
- `port > 443` is a contiguous range in the bitmap
- Roaring bitmap run containers compress contiguous ranges into `[start, length]`
  pairs — maximally compact

This doesn't require any changes to entity_key — it's an interner-level
optimization that benefits from the ordering foundation laid here.

---

## 12. File Changes

| File | Change |
|---|---|
| `src/entity_key.zig` | **New.** Value, ValueType, encode/decode/compare |
| `src/fact.zig` | `Entity.toKey` delegates to entity_key. Add `toKeyBuf`. |
| `src/fact_store.zig` | Replace 2 inline key constructions with entity_key calls |
| `src/bitmap_ingest.zig` | Replace `internEntity` internals with entity_key call |
| `src/lib.zig` | Export entity_key module |
| Tests in above files | Update key format assertions to use decode |

---

## 13. Checklist

- [ ] `src/entity_key.zig` created with Value, ValueType, encode, decode
- [ ] `encodeString` convenience for the string-value case
- [ ] `fromBytes` finds `\x00` separator, reads type tag, decodes value
- [ ] `compareValues` does type-aware comparison
- [ ] i64 sign-bit flip for order-preserving encoding
- [ ] All fixed-width types (u16, u32, i64, u64, uuid, ipv4, ipv6) encode/decode
- [ ] Variable-width types (string, bytes) consume remaining bytes (no length prefix)
- [ ] `Entity.toKey` delegates to entity_key (fact.zig)
- [ ] `Entity.toKeyBuf` added for stack-buffer pattern (fact.zig)
- [ ] `fact_store.zig` `addFacts` uses entity_key
- [ ] `fact_store.zig` `getFactsByEntity` uses entity_key
- [ ] `bitmap_ingest.zig` `internEntity` uses entity_key
- [ ] `lib.zig` exports entity_key
- [ ] Unit tests: roundtrip for every ValueType
- [ ] Unit tests: sort order (string lexicographic, numeric, i64 sign, entity type grouping)
- [ ] Unit tests: buffer overflow returns error
- [ ] Unit tests: binary safety (null bytes in value)
- [ ] Unit tests: compareValues same-type and cross-type
- [ ] Existing tests updated for new key format
- [ ] `zig build test` passes
- [ ] `zig build test-all` passes
