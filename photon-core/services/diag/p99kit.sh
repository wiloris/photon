#!/bin/sh
# Контрольный замер p99 записи журнала диагностики (ADR-130 §5.4, §5.6) на
# другой машине — например, с clocksource = tsc.
#
# Сборка (здесь, где есть Go 1.22 и исходники):
#   sh services/diag/p99kit.sh build <каталог>
#     → <каталог>/diag.test (статический linux/amd64, без cgo) и копия этого скрипта.
#   Для arm64 (Android по adb): GOARCH=arm64 sh services/diag/p99kit.sh build <каталог>
#
# Запуск (на целевой машине; Go не нужен):
#   sh p99kit.sh run <каталог-результатов>
#     → env.txt (машина, ядро, clocksource, CPU, флаги TSC), latency.txt (сырые
#       перцентили каждого повтора), latency-run.txt (вывод), bench.txt.
#
# Методика — та же, что в IMPLEMENTATION_LOG: 8 и 1 писатель × 10⁶ записей,
# время каждой — счётчиком процессора (runtime.cputicks, калибровка по
# монотонным часам), 5 повторов, критерий — медиана p99 Record при 8
# писателях ≤ 2000 нс. Порог не настраивается.
set -eu
cmd=${1:-}; dir=${2:-}
[ -n "$cmd" ] && [ -n "$dir" ] || { echo "usage: $0 build|run <каталог>"; exit 2; }

case "$cmd" in
build)
	mkdir -p "$dir"
	here=$(cd "$(dirname "$0")" && pwd)
	(cd "$here" && CGO_ENABLED=0 GOOS=linux GOARCH="${GOARCH:-amd64}" go test -c -o "$dir/diag.test" .)
	cp "$0" "$dir/p99kit.sh"
	echo "собрано: $dir/diag.test ($(go version))"
	;;
run)
	mkdir -p "$dir"
	bin=$(dirname "$0")/diag.test
	{
		echo "date: $(date -u +%FT%TZ)"
		echo "uname: $(uname -a)"
		echo "clocksource: $(cat /sys/devices/system/clocksource/clocksource0/current_clocksource 2>&1)"
		echo "available: $(cat /sys/devices/system/clocksource/clocksource0/available_clocksource 2>&1)"
		echo "nproc: $(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN)"
		echo "cpu: $(grep -m1 -E 'model name|Hardware|CPU part' /proc/cpuinfo 2>/dev/null)"
		echo "tsc flags: $(grep -m1 -o -w -E 'constant_tsc|nonstop_tsc|tsc_known_freq' /proc/cpuinfo 2>/dev/null | sort -u | tr '\n' ' ')"
		echo "virt: $(systemd-detect-virt 2>/dev/null; true)"
		echo "load: $(cat /proc/loadavg)"
		echo "binary: $("$bin" -test.run '^$' -test.v 2>/dev/null | head -1 || true)"
	} > "$dir/env.txt"
	PHOTON_DIAG_MEASURE=1 \
	PHOTON_DIAG_MEASURE_MODES=time.Now/8,monotime/1,monotime/8,recordMono/1,recordMono/8,Record/1,Record/8 \
	PHOTON_DIAG_MEASURE_OUT="$dir/latency.txt" \
		"$bin" -test.run '^TestMeasureRecordLatency$' -test.v -test.timeout 40m > "$dir/latency-run.txt" 2>&1 || true
	"$bin" -test.run '^$' -test.bench '^Benchmark(Record|RecordMono|TimeNow)$' -test.benchmem \
		-test.cpu 1,8 -test.count 10 -test.benchtime 1000000x > "$dir/bench.txt" 2>&1 || true
	cat "$dir/env.txt"
	grep -E 'писателей|критерий' "$dir/latency-run.txt" || tail -5 "$dir/latency-run.txt"
	;;
*)
	echo "usage: $0 build|run <каталог>"; exit 2
	;;
esac
