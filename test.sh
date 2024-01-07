#!/bin/bash

set -euo pipefail

zig build -Doptimize=ReleaseFast

/usr/bin/time -v zig-out/bin/1brc_zig > result_zig.txt
diff -uw result.txt result_zig.txt