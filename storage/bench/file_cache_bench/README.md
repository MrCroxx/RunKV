## Usage

```bash
sudo docker stop `sudo docker ps | grep jaeger | awk '{print $1}'` || true && \
sudo docker run --rm -d -p6831:6831/udp -p16686:16686 -p14268:14268 --name jaeger jaegertracing/all-in-one:latest && \
sudo rm -rf /data/filecache && \
cargo build --bin file-cache-bench --features "bpf trace" --release && \
sudo ./target/release/file-cache-bench -p /data/filecache --capacity 10240 --total-buffer-capacity 1024 --w-rate 100 --r-rate 100 --concurrency 8 --time 60 --slow 5
```

## Output Examples

```plain
Event {
    magic: 16045690984833335023,
    sid: 56298568754921497,
    vfs_read_enter_ts: 255073825749145,
    vfs_read_leave_ts: 255073841493162,
    ext4_file_read_iter_enter_ts: 255073825750206,
    ext4_file_read_iter_leave_ts: 255073841492771,
    iomap_dio_rw_enter_ts: 255073836674011,
    iomap_dio_rw_leave_ts: 255073841492271,
    filemap_write_and_wait_range_enter_ts: 255073836674320,
    filemap_write_and_wait_range_leave_ts: 255073836674622,
}
vfs_read                       |   15.744ms | ==================================================
ext4_file_read_iter            |   15.743ms | =================================================
iomap_dio_rw                   |    4.818ms |                                   ===============
filemap_write_and_wait_range   |  302.000ns |                                   =
```

```plain
Total:
disk total iops: 10835.1
disk total throughput: 1.3 GiB/s
disk read iops: 4379.6
disk read throughput: 543.1 MiB/s
disk write iops: 6455.4
disk write throughput: 780.3 MiB/s
insert iops: 788.6/s
insert throughput: 788.6 MiB/s
insert lat p50: 2us
insert lat p90: 5us
insert lat p99: 12us
get iops: 656.6/s
get miss: 6.06%
get hit lat p50: 9087us
get hit lat p90: 23551us
get hit lat p99: 31487us
get miss lat p50: 16us
get miss lat p90: 36us
get miss lat p99: 563us
flush iops: 253.0/s
flush throughput: 770.6 MiB/s
```