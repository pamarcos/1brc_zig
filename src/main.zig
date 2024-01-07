const std = @import("std");

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
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        if (gpa.deinit() == .leak) {
            std.log.err("Memory leaked!", .{});
        }
    }
    const allocator = gpa.allocator();

    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.skip();
    var filename = args.next() orelse "measurements.txt";
    std.log.info("Processing {s} file", .{filename});

    var hash = std.StringHashMap(Measurement).init(allocator);
    defer hash.deinit();
    var file = try std.fs.cwd().openFile(filename, .{});
    defer file.close();

    var buf_reader = std.io.bufferedReader(file.reader());
    var in_stream = buf_reader.reader();

    var buf: [1024]u8 = undefined;
    var line_number: u32 = 0;
    while (try in_stream.readUntilDelimiterOrEof(&buf, '\n')) |line| : (line_number += 1) {
        const temp_pos = std.mem.indexOfScalarPos(u8, line, 0, ';').?;
        const name = line[0..temp_pos];
        const temp = try std.fmt.parseFloat(f32, line[temp_pos + 1 ..]);
        // std.log.debug("{s}: {d:.1}", .{ name, temp });

        var measurement_optional = hash.getPtr(name);
        if (measurement_optional == null) {
            var copy_name = try allocator.alloc(u8, name.len);
            std.mem.copyForwards(u8, copy_name, name);
            try hash.put(copy_name, .{ .sum = temp, .max = temp, .min = temp, .amount = 1 });
        } else {
            var measurement = measurement_optional.?;
            measurement.sum += temp;
            measurement.min = @min(measurement.min, temp);
            measurement.max = @max(measurement.max, temp);
            measurement.amount += 1;
        }
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
    var fbs = std.io.fixedBufferStream(&buf);
    var i: u32 = 0;
    const stdout = std.io.getStdOut();
    for (cities) |city| {
        var measurement = hash.get(city).?;
        fbs.reset();
        try std.fmt.format(fbs.writer(), "{s}={d:.1}/{d:.1}/{d:.1}", .{ city, measurement.min, measurement.sum / @as(f32, @floatFromInt(measurement.amount)), measurement.max });
        allocator.free(city);
        _ = try stdout.write(fbs.getWritten());
        if (i != cities.len - 1) {
            _ = try stdout.write(", ");
        }
        i += 1;
    }
    _ = try std.io.getStdOut().write("}");

    allocator.free(cities);
}
