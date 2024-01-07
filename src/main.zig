const std = @import("std");
const builtin = @import("builtin");

pub const std_options = struct {
    pub const log_level = .info;
};

const Measurement = struct {
    sum: f64,
    max: f32,
    min: f32,
    amount: u32,
};

const HashMap = std.StringHashMap(Measurement);

fn lessThanString(context: void, a: []const u8, b: []const u8) bool {
    _ = context;
    for (a, 0..) |char, index| {
        if (b[index] > char) {
            return true;
        } else if (b[index] < char) {
            return false;
        }
    }
    return true;
}

const Context = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    buffer: []const u8,
    wait_group: *std.Thread.WaitGroup,
    map: HashMap,
    mutex: *std.Thread.Mutex,
    main_map: *HashMap,

    fn init(allocator: std.mem.Allocator, buffer: []const u8, wait_group: *std.Thread.WaitGroup, mutex: *std.Thread.Mutex, main_map: *HashMap) Self {
        return .{ .allocator = allocator, .buffer = buffer, .wait_group = wait_group, .mutex = mutex, .main_map = main_map, .map = HashMap.init(allocator) };
    }

    fn deinit(self: *Self) void {
        self.map.deinit();
    }
};

fn run(context: Context) void {
    defer context.wait_group.finish();

    var pos: usize = 0;
    const buffer = context.buffer;
    var map = context.map;

    while (pos < buffer.len) {
        const line_end = std.mem.indexOfScalarPos(u8, buffer, pos, '\n') orelse buffer.len;
        const line = buffer[pos..line_end];
        const temp_pos = std.mem.indexOfScalarPos(u8, line, 0, ';').?;
        const name = line[0..temp_pos];
        const temp = std.fmt.parseFloat(f32, line[temp_pos + 1 ..]) catch unreachable;
        // std.log.debug("{s}: {d:.1}", .{ name, temp });

        const measurement_optional = map.getPtr(name);
        if (measurement_optional == null) {
            map.put(name, .{ .sum = temp, .max = temp, .min = temp, .amount = 1 }) catch unreachable;
        } else {
            var measurement = measurement_optional.?;
            measurement.sum += temp;
            measurement.min = @min(measurement.min, temp);
            measurement.max = @max(measurement.max, temp);
            measurement.amount += 1;
        }

        pos = line_end + 1;
    }

    var iterator = map.iterator();
    while (iterator.next()) |city| {
        context.mutex.lock();
        defer context.mutex.unlock();

        if (context.main_map.getPtr(city.key_ptr.*)) |value| {
            value.sum += city.value_ptr.sum;
            value.min = @min(value.min, city.value_ptr.min);
            value.max = @max(value.max, city.value_ptr.max);
            value.amount += city.value_ptr.amount;
        } else {
            context.main_map.put(city.key_ptr.*, city.value_ptr.*) catch unreachable;
        }
    }
}

pub fn main() !void {
    var allocator: std.mem.Allocator = undefined;

    // Use the General Purpose Allocator to detect memory leaks if not in ReleaseFast
    // TODO: for some reason it doesn't work with threads. Disabling for now
    switch (builtin.mode) {
        // .Debug, .ReleaseSafe => {
        //     var gpa = std.heap.GeneralPurposeAllocator(.{}){};
        //     defer {
        //         if (gpa.deinit() == .leak) {
        //             std.log.err("Memory leaked!", .{});
        //         }
        //     }
        //     allocator = gpa.allocator();
        // },
        else => allocator = std.heap.c_allocator,
    }

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.skip();
    const filename = args.next() orelse "measurements.txt";
    const num_threads = try std.Thread.getCpuCount();
    std.log.info("Processing {s} file with {d} threads", .{ filename, num_threads });

    var map = HashMap.init(allocator);
    defer map.deinit();
    var file = try std.fs.cwd().openFile(filename, .{});
    defer file.close();
    const file_length = try file.getEndPos();
    const file_ptr = try std.os.mmap(null, file_length, std.os.PROT.READ, std.os.MAP.PRIVATE, file.handle, 0);
    defer std.os.munmap(file_ptr);

    var thread_pool: std.Thread.Pool = undefined;
    try thread_pool.init(.{ .allocator = allocator, .n_jobs = @intCast(num_threads) });
    defer thread_pool.deinit();
    var mutex = std.Thread.Mutex{};
    var wait_group = std.Thread.WaitGroup{};
    const chunk_size = file_length / num_threads;
    var prev_pos: usize = 0;

    for (0..num_threads) |_| {
        const end = std.mem.indexOfScalarPos(u8, file_ptr, prev_pos + chunk_size, '\n') orelse file_length;
        const context = Context.init(allocator, file_ptr[prev_pos..end], &wait_group, &mutex, &map);
        prev_pos = end + 1;
        wait_group.start();
        try thread_pool.spawn(run, .{context});
    }

    thread_pool.waitAndWork(&wait_group);

    var array = std.ArrayList([]const u8).init(allocator);
    defer array.deinit();

    var iterator = map.keyIterator();
    while (iterator.next()) |key| {
        try array.append(key.*);
    }

    const cities = try array.toOwnedSlice();
    std.mem.sort([]const u8, cities, {}, lessThanString);

    _ = try std.io.getStdOut().write("{");
    var buf: [128]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    var i: u32 = 0;
    const stdout = std.io.getStdOut();
    for (cities) |city| {
        const measurement = map.get(city).?;
        fbs.reset();
        try std.fmt.format(fbs.writer(), "{s}={d:.1}/{d:.1}/{d:.1}", .{ city, measurement.min, measurement.sum / @as(f32, @floatFromInt(measurement.amount)), measurement.max });
        _ = try stdout.write(fbs.getWritten());
        if (i != cities.len - 1) {
            _ = try stdout.write(", ");
        }
        i += 1;
    }
    _ = try std.io.getStdOut().write("}");

    allocator.free(cities);
}
