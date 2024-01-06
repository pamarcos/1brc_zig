const std = @import("std");

const Measurement = struct {
    sum: f64,
    max: f32,
    min: f32,
    amount: u32,
};

fn compareString(context: void, a: []const u8, b: []const u8) bool {
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

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        if (gpa.deinit() == .leak) {
            std.log.err("Memory leaked!", .{});
        }
    }
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        std.log.debug("Need to pass an input file argument", .{});
        std.process.exit(1);
    }

    var filename = try allocator.alloc(u8, args[1].len);
    defer allocator.free(filename);
    std.mem.copyForwards(u8, filename, args[1]);

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
        const temp_pos = std.mem.indexOf(u8, line, ";").?;
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

        if (line_number % 1000000 == 0) {
            std.log.info("Processed {d} lines", .{line_number});
        }
    }

    var array = std.ArrayList([]const u8).init(allocator);
    defer array.deinit();

    var iterator = hash.keyIterator();
    while (iterator.next()) |key| {
        try array.append(key.*);
    }

    var cities = try array.toOwnedSlice();
    std.mem.sort([]const u8, cities, {}, compareString);

    _ = try std.io.getStdOut().write("{");
    var fbs = std.io.fixedBufferStream(&buf);
    for (cities) |city| {
        var measurement = hash.get(city).?;
        fbs.reset();
        try std.fmt.format(fbs.writer(), "{s}={d:.1}/{d:.1}/{d:.1}, ", .{ city, measurement.min, measurement.sum / @as(f32, @floatFromInt(measurement.amount)), measurement.max });
        allocator.free(city);
        _ = try std.io.getStdOut().write(fbs.getWritten());
    }
    _ = try std.io.getStdOut().write("}");

    allocator.free(cities);
}
