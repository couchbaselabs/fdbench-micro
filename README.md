# fdbench-micro
forestdb micro benchmark testing

**Standalone**
```bash
mkdir build && cd build 
cmake ../
make
./fdb_bench
```

**Build 2 Build Comparision**
```bash
#usage
sudo ./compare_bench_report.sh old_rev [new_rev]

#ie..
sudo ./compare_bench_report.sh 22106 f0b1bf

#compare to latest commit
sudo ./compare_bench_report.sh 22106 "--"

# re-run last run as baseline
sudo ./compare_bench_report.sh . f0b1bf

# just regenerate report 
sudo ./compare_bench_report.sh . .

```
**dependencies**
See: https://github.com/couchbase/forestdb/blob/master/INSTALL.md#dependencies
