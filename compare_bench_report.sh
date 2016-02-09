DEFAULT_HEAD="bench_head"
NEW_BENCH_STATS="cmp_bench_new.stats"
OLD_BENCH_STATS="cmp_bench_old.stats"
BENCH_REPORT='cmp_bench_report.txt'
BUILD_DIR="build"
THRESHOLD_PERC=0.10
PHASE_COUNT=5
REGRESSION_CNT=0

sudo echo "Hi r00t"

if [ -n "$WORKSPACE" ]; then # jenkins override
  BUILD_DIR="$WORKSPACE/build/fdbbench"
fi
if [ -n "$_BUILD_DIR" ]; then # explicit override
  BUILD_DIR="$_BUILD_DIR"
fi
FDB_BENCH="$BUILD_DIR/fdb_bench"

function build_bench {
  pushd .
  mkdir -p $BUILD_DIR
  cd $BUILD_DIR
  cmake ..
  make clean
  make
  popd
}

function run_bench {
  build_bench

  STAT_FILE=$1
  rm $STAT_FILE 2>/dev/null
  $FDB_BENCH | tee $STAT_FILE
}

function install_fdb_lib {
    pushd .

    sudo rm -rf forestdb
    git clone https://github.com/couchbase/forestdb.git
    cd forestdb
    git checkout $1
    if [ $? != 0 ]; then
      echo "ERROR: [git checkout $1] - resolve any uncommitted changes before proceeding"
      exit 1
    fi

    mkdir build && cd build
    cmake -DCMAKE_BUILD_TYPE=Debug ../
    make -j4
    sudo make install

    popd
}

# CHECK if Using stale data
if [ "$2" != '.' ]; then
    if [ -n "$2" ]; then
        echo "Setting Baseline revision: ".$2
        install_fdb_lib $2
    else
        echo "Using preinstalled fdb lib"
    fi
    sleep 1

    # run bench 5 times at current repo
    for i in $(seq 1 $PHASE_COUNT); do

      PHASEDIR=phase.$i
      mkdir -p $PHASEDIR
      echo $PHASEDIR/$NEW_BENCH_STATS
      run_bench $PHASEDIR/$NEW_BENCH_STATS
    done
fi

# CHECK if using stale data
if [ "$1" != '.' ]; then

    # CHECK IF DOING BUILD-2-BUILD comparision
    if [ -n "$1" ]; then
      OLD_REV=$1
      echo "Comparing to Revision: ".$OLD_REV
    else
      NEW_REV="--"
      echo "Comparing to Master/Head"
    fi

    sleep 1

    # install baseline revision
    install_fdb_lib $OLD_REV
    # run bench 5 times at old repo
    for i in $(seq 1 $PHASE_COUNT); do

      PHASEDIR=phase.$i
      mkdir -p $PHASEDIR
      run_bench $PHASEDIR/$OLD_BENCH_STATS
    done
fi

# generate bench report for each phase
for i in $(seq 1 $PHASE_COUNT); do

  # run diff compares 2 files and prints diff in 3rd column
  # the third indicates added latency and regression is based
  # on threshold.percentage
  #
  #... example output ...
  #
  # BENCH-SEQUENTIAL_SET-WALFLUSH-1
  # stat            old             new             diff (ms)       thresh     regress
  # set             11.381200ns     10.151800ns     -1.2294         100
  # commit_wal      22366.000000ns  21965.000000ns  -401            100000

  PHASEDIR=phase.$i
  NEW_STATS=$PHASEDIR/$NEW_BENCH_STATS
  OLD_STATS=$PHASEDIR/$OLD_BENCH_STATS
  REPORT=$PHASEDIR/$BENCH_REPORT
  awk 'NR==FNR {a[NR]=$2; next} $1~/BENCH/{printf "%s\n%-15s %-15s %-15s %-15s %-10s %-10s\n", $1, "stat", "old", "new", "diff (ms)", "thresh", "regress"; next} {if(a[FNR]-$2>($2*'$THRESHOLD_PERC')){ b[NR]="yes" }} {printf "%-15s %-15s %-15s %-15s %-10s %-10s\n", $1, $2, a[FNR], a[FNR]-$2, $2*'$THRESHOLD_PERC', b[NR]}' $NEW_STATS $OLD_STATS  |   sed -E 's/^ .*yes//' | tee $REPORT

done


# Regression Detection for all stats
ALL_STATS=$(cat phase.1/$NEW_BENCH_STATS | grep "^[a-z]" | awk '{print $1}' | sort | uniq)
for stat in ${ALL_STATS}; do
  NTH_STAT=$(echo $(seq 1 5 | xargs -I '{}' cat phase.'{}'/cmp_bench_report.txt  | grep "^$stat" | wc -l) / $PHASE_COUNT | bc)

  # for each stat look up regression count across the phases
  for n in $(seq 0 $NTH_STAT); do
      N_REGRESSIONS=$(seq 1 $PHASE_COUNT | xargs -I '{}' cat phase.'{}'/$BENCH_REPORT | grep "^$stat" | awk "NR % sc==nc" sc="$NTH_STAT" nc="$n" | grep "yes \+$" |wc -l)
      N_CMP_REGRESSIONS=$((PHASE_COUNT-1))
    if [ $N_REGRESSIONS -ge $N_CMP_REGRESSIONS ]; then

      # we've regressed on this stat for every phase!
      echo -e "\nREGRESSION DETECTED: $stat"
      printf "%-15s %-15s %-15s %-15s %-10s %-10s\n" "stat" "old" "new" "diff (us)" "thresh" "regress"
      seq 1 $PHASE_COUNT | xargs -I '{}' cat phase.'{}'/$BENCH_REPORT | grep "^$stat" | awk "NR % sc==nc" sc="$NTH_STAT" nc="$n"
      REGRESSION_CNT=$((REGRESSION_CNT+1))
    fi
  done
done



echo -e "\nDone: $REGRESSION_CNT possible regressions\n\n"

# cleanup
rm bench*
for i in $(seq 1 $PHASE_COUNT); do

  PHASEDIR=phase.$i
#  rm -rf $PHASEDIR

done


# return to new revision
git checkout $NEW_REV

# validator exit code
if [ $REGRESSION_CNT -gt 0 ]; then
  exit 1
fi
