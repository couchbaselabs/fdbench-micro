# fdbench-micro
forestdb micro benchmark testing

**Standalone**
```bash
mkdir build && cd build 
cmake ../
make
./fdb_bench
```

**Scenarios**
```bash
#usage
sudo ./compare_bench_report.sh old_rev [new_rev]

#compare 2 commit hash
sudo ./compare_bench_report.sh 22106 f0b1bf

#compare to latest commit
sudo ./compare_bench_report.sh 22106 "--"

#use recent run as baseline against specific rev
sudo ./compare_bench_report.sh . f0b1bf

# just regenerate report 
sudo ./compare_bench_report.sh . .

#use known commit hash baseline against current installed fdb lib
# ie.. when running against a gerrit branch with no commit hash
# then leave out the 2nd arg and manually install the libraries
sudo ./compare_bench_report.sh 22106


```
**dependencies**
See: https://github.com/couchbase/forestdb/blob/master/INSTALL.md#dependencies
