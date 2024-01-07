const std = @import("std");
const builtin = @import("builtin");

const Measurement = struct {
    sum: f64,
    max: f32,
    min: f32,
    amount: u32,
};

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

pub const std_options = struct {
    pub const log_level = .info;
};

pub fn main() !void {
    var allocator: std.mem.Allocator = undefined;

    // Use the General Purpose Allocator to detect memory leaks if not in ReleaseFast
    switch (builtin.mode) {
        .Debug, .ReleaseSafe => {
            var gpa = std.heap.GeneralPurposeAllocator(.{}){};
            defer {
                if (gpa.deinit() == .leak) {
                    std.log.err("Memory leaked!", .{});
                }
            }
            allocator = gpa.allocator();
        },
        else => allocator = std.heap.c_allocator,
    }

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.skip();
    var filename = args.next() orelse "measurements.txt";
    std.log.info("Processing {s} file", .{filename});

    var hash = std.StringHashMap(Measurement).init(allocator);
    defer hash.deinit();
    var file = try std.fs.cwd().openFile(filename, .{});
    defer file.close();
    const file_length = try file.getEndPos();
    const file_ptr = try std.os.mmap(null, file_length, std.os.PROT.READ, std.os.MAP.PRIVATE, file.handle, 0);
    defer std.os.munmap(file_ptr);

    var pos: usize = 0;
    while (pos < file_length) {
        const line_end = std.mem.indexOfScalarPos(u8, file_ptr, pos, '\n') orelse file_length;
        const line = file_ptr[pos..line_end];
        const temp_pos = std.mem.indexOfScalarPos(u8, line, 0, ';').?;
        const name = line[0..temp_pos];
        const temp = try std.fmt.parseFloat(f32, line[temp_pos + 1 ..]);
        // std.log.debug("{s}: {d:.1}", .{ name, temp });

        var measurement_optional = hash.getPtr(name);
        if (measurement_optional == null) {
            try hash.put(name, .{ .sum = temp, .max = temp, .min = temp, .amount = 1 });
        } else {
            var measurement = measurement_optional.?;
            measurement.sum += temp;
            measurement.min = @min(measurement.min, temp);
            measurement.max = @max(measurement.max, temp);
            measurement.amount += 1;
        }

        pos = line_end + 1;
    }

    var array = std.ArrayList([]const u8).init(allocator);
    defer array.deinit();

    var iterator = hash.keyIterator();
    while (iterator.next()) |key| {
        try array.append(key.*);
    }

    var cities = try array.toOwnedSlice();
    std.mem.sort([]const u8, cities, {}, lessThanString);

    _ = try std.io.getStdOut().write("{");
    var buf: [128]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&buf);
    var i: u32 = 0;
    const stdout = std.io.getStdOut();
    for (cities) |city| {
        var measurement = hash.get(city).?;
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
