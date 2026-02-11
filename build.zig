const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Sanitizer options
    const sanitize_thread = b.option(bool, "sanitize-thread", "Enable ThreadSanitizer") orelse false;

    // LMDB dependency
    const lmdb_dep = b.dependency("lmdb", .{
        .target = target,
        .optimize = optimize,
    });
    const lmdb_mod = lmdb_dep.module("lmdb");

    // Main library module
    const lib_mod = b.addModule("kb", .{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    lib_mod.addImport("lmdb", lmdb_mod);

    // Main executable
    const exe_mod = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    exe_mod.addImport("lmdb", lmdb_mod);
    exe_mod.addImport("kb", lib_mod);

    const exe = b.addExecutable(.{
        .name = "kb",
        .root_module = exe_mod,
    });
    b.installArtifact(exe);

    // Run command
    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_cmd.addArgs(args);
    }
    const run_step = b.step("run", "Run kb");
    run_step.dependOn(&run_cmd.step);

    // Unit tests
    const lib_test_mod = b.createModule(.{
        .root_source_file = b.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
        .sanitize_thread = sanitize_thread,
    });
    lib_test_mod.addImport("lmdb", lmdb_mod);

    const lib_tests = b.addTest(.{
        .root_module = lib_test_mod,
    });

    const run_lib_tests = b.addRunArtifact(lib_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_lib_tests.step);

    // Integration tests
    const integration_mod = b.createModule(.{
        .root_source_file = b.path("tests/integration.zig"),
        .target = target,
        .optimize = optimize,
        .sanitize_thread = sanitize_thread,
    });
    integration_mod.addImport("lmdb", lmdb_mod);
    integration_mod.addImport("kb", lib_mod);

    const integration_tests = b.addTest(.{
        .root_module = integration_mod,
    });

    const run_integration_tests = b.addRunArtifact(integration_tests);
    const integration_step = b.step("test-integration", "Run integration tests");
    integration_step.dependOn(&run_integration_tests.step);

    // All tests
    const all_tests_step = b.step("test-all", "Run all tests");
    all_tests_step.dependOn(&run_lib_tests.step);
    all_tests_step.dependOn(&run_integration_tests.step);

    // Benchmark
    const bench_mod = b.createModule(.{
        .root_source_file = b.path("tests/bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    bench_mod.addImport("lmdb", lmdb_mod);
    bench_mod.addImport("kb", lib_mod);

    const bench_exe = b.addExecutable(.{
        .name = "bench",
        .root_module = bench_mod,
    });

    const run_bench = b.addRunArtifact(bench_exe);
    const bench_step = b.step("bench", "Run benchmark");
    bench_step.dependOn(&run_bench.step);

    // Fuzz corpus tests
    const fuzz_test = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/fuzz.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    fuzz_test.root_module.addImport("lmdb", lmdb_mod);
    fuzz_test.root_module.addImport("kb", lib_mod);

    const run_fuzz_test = b.addRunArtifact(fuzz_test);
    const fuzz_step = b.step("test-fuzz", "Run fuzz corpus tests");
    fuzz_step.dependOn(&run_fuzz_test.step);

    // Raw LMDB benchmark
    const bench_raw_mod = b.createModule(.{
        .root_source_file = b.path("tests/bench_raw.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    bench_raw_mod.addImport("lmdb", lmdb_mod);

    const bench_raw_exe = b.addExecutable(.{
        .name = "bench-raw",
        .root_module = bench_raw_mod,
    });

    const run_bench_raw = b.addRunArtifact(bench_raw_exe);
    const bench_raw_step = b.step("bench-raw", "Run raw LMDB benchmark");
    bench_raw_step.dependOn(&run_bench_raw.step);
}
