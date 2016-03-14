/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */


#include <stdio.h>
#include <assert.h>

#include <algorithm>
#include <cmath>
#include <iterator>
#include <numeric>
#include <string>
#include <vector>

#include "config.h"
#include "timing.h"

#include <libforestdb/forestdb.h>

template<typename T>
struct Stats {
    std::string name;
    double mean;
    double median;
    double stddev;
    double pct5;
    double pct95;
    double pct99;
    std::vector<T>* values;
};

class StatCollector {
public:
    StatCollector(int _num_stats, int _num_samples) {
        num_stats = _num_stats;
        num_samples = _num_samples;
        t_stats = new stat_history_t*[num_stats];
        for (int i = 0; i < num_stats; ++i) {
            t_stats[i] = new stat_history_t[num_samples];
        }
    }

    ~StatCollector() {
        for (int i = 0; i < num_stats; ++i) {
            delete[] t_stats[i];
        }
        delete[] t_stats;
    }

    void aggregateAndPrintAll(const char* title, int count, const char* unit) {
        std::vector<std::pair<std::string, std::vector<uint64_t>*> > all_timings;
        for (int i = 0; i < num_stats; ++i) {
            for (int j = 1; j < num_samples; ++j) {
                t_stats[i][0].latencies.insert(t_stats[i][0].latencies.end(),
                                                   t_stats[i][j].latencies.begin(),
                                                   t_stats[i][j].latencies.end());
                t_stats[i][j].latencies.clear();
            }

            all_timings.push_back(std::make_pair(t_stats[i][0].name,
                                                 &t_stats[i][0].latencies));
        }

        int printed = 0;
        printf("\n========== Avg Latencies (%s) - %d samples (%s) %n",
                title, count, unit, &printed);
        fillLineWith('=', 88-printed);

        print_values(all_timings, unit);

        fillLineWith('=', 87);
    }

    stat_history_t** t_stats;

private:

    // Given a vector of values (each a vector<T>) calcuate metrics on them
    // and print to stdout.
    template<typename T>
    void print_values(
                std::vector<std::pair<std::string, std::vector<T>*> > values,
                std::string unit) {
        // First, calculate mean, median, standard deviation and percentiles
        // of each set of values, both for printing and to derive what the
        // range of the graphs should be.
        std::vector<Stats<T> > value_stats;
        for (const auto& t : values) {
            Stats<T> stats;
            if (t.second->size() == 0) {
                continue;
            }
            stats.name = t.first;
            stats.values = t.second;
            std::vector<T>& vec = *t.second;

            // Calculate latency percentiles
            std::sort(vec.begin(), vec.end());
            stats.median = vec[(vec.size() * 50) / 100];
            stats.pct5 = vec[(vec.size() * 5) / 100];
            stats.pct95 = vec[(vec.size() * 95) / 100];
            stats.pct99 = vec[(vec.size() * 99) / 100];

            const double sum = std::accumulate(vec.begin(), vec.end(), 0.0);
            stats.mean = sum / vec.size();
            double accum = 0.0;
            for (auto &d : vec) {
                accum += (d - stats.mean) * (d - stats.mean);
            }
            stats.stddev = sqrt(accum / (vec.size() - 1));

            value_stats.push_back(stats);
        }

        // From these find the start and end for the spark graphs which covers the
        // a "reasonable sample" of each value set. We define that as from the 5th
        // to the 95th percentile, so we ensure *all* sets have that range covered.
        T spark_start = std::numeric_limits<T>::max();
        T spark_end = 0;
        for (const auto& stats : value_stats) {
            spark_start = (stats.pct5 < spark_start) ? stats.pct5 : spark_start;
            spark_end = (stats.pct95 > spark_end) ? stats.pct95 : spark_end;
        }

        printf("\n                                Percentile           \n");
        printf("  %-16s Median     95th     99th  Std Dev  "
               "Histogram of samples\n\n", "");
        // Finally, print out each set.
        for (const auto& stats : value_stats) {
            if (unit == "µs") {
                printf("%-16s %8.03f %8.03f %8.03f %8.03f  ",
                        stats.name.c_str(), stats.median, stats.pct95,
                        stats.pct99, stats.stddev);
            } else if (unit == "ms") {
                printf("%-16s %8.03f %8.03f %8.03f %8.03f  ",
                        stats.name.c_str(), stats.median/1e3, stats.pct95/1e3,
                        stats.pct99/1e3, stats.stddev/1e3);
            } else {    // unit == "s"
                printf("%-16s %8.03f %8.03f %8.03f %8.03f  ",
                        stats.name.c_str(), stats.median/1e6, stats.pct95/1e6,
                        stats.pct99/1e6, stats.stddev/1e6);
            }

            // Calculate and render Sparkline (requires UTF-8 terminal).
            const int nbins = 32;
            int prev_distance = 0;
            std::vector<size_t> histogram;
            for (unsigned int bin = 0; bin < nbins; bin++) {
                const T max_for_bin = (spark_end / nbins) * bin;
                auto it = std::lower_bound(stats.values->begin(),
                                           stats.values->end(),
                                           max_for_bin);
                const int distance = std::distance(stats.values->begin(), it);
                histogram.push_back(distance - prev_distance);
                prev_distance = distance;
            }

            const auto minmax = std::minmax_element(histogram.begin(),
                                                    histogram.end());
            const size_t range = *minmax.second - *minmax.first + 1;
            const int levels = 8;
            for (const auto& h : histogram) {
                int bar_size = ((h - *minmax.first + 1) * (levels - 1)) / range;
                putchar('\xe2');
                putchar('\x96');
                putchar('\x81' + bar_size);
            }
            putchar('\n');
        }
        if (unit == "µs") {
            printf("%52s  %-14d %s %14d\n", "",
                   int(spark_start), unit.c_str(), int(spark_end));
        } else if (unit == "ms") {
            printf("%52s  %-14d %s %14d\n", "",
                   int(spark_start/1e3), unit.c_str(), int(spark_end/1e3));
        } else {    // unit == "s"
            printf("%52s  %-14d %s %14d\n", "",
                   int(spark_start/1e6), unit.c_str(), int(spark_end/1e6));
        }
    }

    void fillLineWith(const char c, int spaces) {
        for (int i = 0; i < spaces; ++i) {
            putchar(c);
        }
        putchar('\n');
    }

    int num_stats;
    int num_samples;
};

bool track_stat(stat_history_t *stat, uint64_t lat) {
    if (lat == ERR_NS) {
      return false;
    }

    if (stat) {
        stat->latencies.push_back(lat);
        return true;
    } else {
        return false;
    }
}

void print_db_stats(fdb_file_handle **dbfiles, int nfiles) {
    int i, j;
    fdb_status status;
    fdb_latency_stat stat;
    int nstats = FDB_LATENCY_NUM_STATS;

    StatCollector *sa = new StatCollector(nstats, 1);

    for (i = 0; i < nstats; i++) {
        const char* name = fdb_latency_stat_name(i);
        sa->t_stats[i][0].name = name;

        for (j = 0; j < nfiles; j++) {
            memset(&stat, 0, sizeof(fdb_latency_stat));
            status = fdb_get_latency_stats(dbfiles[j], &stat, i);
            assert(status == FDB_RESULT_SUCCESS);

            if (stat.lat_count > 0) {
                sa->t_stats[i][0].latencies.push_back(stat.lat_avg);
            }
        }
    }

    sa->aggregateAndPrintAll("FDB_STATS", nfiles, "ms");
    delete sa;
}

void str_gen(char *s, const int len) {
    int i = 0;
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    size_t n_ch = strlen(alphanum);

    if (len < 1){
        return;
    }

    // return same ordering of chars
    while (i < len) {
        s[i] = alphanum[i%n_ch];
        i++;
    }
    s[len-1] = '\0';
}

void swap(char *x, char *y) {
    char temp;
    temp = *x;
    *x = *y;
    *y = temp;
}

void permute(fdb_kvs_handle *kv, char *a, int l, int r) {
    int i;
    char keybuf[256], metabuf[256], bodybuf[1024];
    fdb_doc *doc = NULL;
    str_gen(bodybuf, 1024);

    if (l == r) {
        sprintf(keybuf, a, l);
        sprintf(metabuf, "meta%d", r);
        fdb_doc_create(&doc, (void*)keybuf, strlen(keybuf),
                       (void*)metabuf, strlen(metabuf),
                       (void*)bodybuf, strlen(bodybuf));
        fdb_set(kv, doc);
        fdb_doc_free(doc);
    } else {
        for (i = l; i <= r; i++) {
            swap((a+l), (a+i));
            permute(kv, a, l+1, r);
            swap((a+l), (a+i)); //backtrack
        }
    }
}

void sequential(fdb_kvs_handle *kv, int pos) {
    int i;
    char keybuf[256], metabuf[256], bodybuf[512];
    fdb_doc *doc = NULL;
    str_gen(bodybuf, 512);

    // load flat keys
    for (i = 0; i < 1000; i++){
        sprintf(keybuf, "%d_%dseqkey", pos, i);
        sprintf(metabuf, "meta%d", i);
        fdb_doc_create(&doc, (void*)keybuf, strlen(keybuf),
                       (void*)metabuf, strlen(metabuf),
                       (void*)bodybuf, strlen(bodybuf));
        fdb_set(kv, doc);
        fdb_doc_free(doc);
    }
}

void setup_db(fdb_file_handle **fhandle, fdb_kvs_handle **kv) {

    int r;
    char cmd[64];

    fdb_status status;
    fdb_config config;
    fdb_kvs_config kvs_config;
    kvs_config = fdb_get_default_kvs_config();
    config = fdb_get_default_config();
    config.durability_opt = FDB_DRB_ASYNC;
    config.compaction_mode = FDB_COMPACTION_MANUAL;

    // cleanup first
    sprintf(cmd, "rm %s*>errorlog.txt", BENCHDB_NAME);
    r = system(cmd);
    (void)r;

    status = fdb_open(fhandle, BENCHDB_NAME, &config);
    assert(status == FDB_RESULT_SUCCESS);

    status = fdb_kvs_open(*fhandle, kv, BENCHKV_NAME , &kvs_config);
    assert(status == FDB_RESULT_SUCCESS);
}


void writer(fdb_kvs_handle *db, int pos) {

    char keybuf[KEY_SIZE];

    str_gen(keybuf, KEY_SIZE);
    permute(db, keybuf, 0, PERMUTED_BYTES);
    sequential(db, pos);
}

void reader(reader_context *ctx) {

    bool is_err;
    fdb_kvs_handle *db = ctx->handle;
    fdb_iterator *iterator;
    fdb_doc *doc = NULL, *rdoc = NULL;
    fdb_status status;

    track_stat(ctx->stat_itr_init,
               timed_fdb_iterator_init(db, &iterator, FDB_ITR_NO_DELETES));

    // repeat until fail
    do {
        // sum time of all gets
        track_stat(ctx->stat_itr_get, timed_fdb_iterator_get(iterator, &rdoc));

        // get from kv
        fdb_doc_create(&doc, rdoc->key, rdoc->keylen, NULL, 0, NULL, 0);
        status = fdb_get(db, doc);
        assert(status == FDB_RESULT_SUCCESS);

        fdb_doc_free(doc);
        doc = NULL;

        // kv get
        fdb_doc_free(rdoc);
        rdoc = NULL;

        is_err = track_stat(ctx->stat_itr_next,
                            timed_fdb_iterator_next(iterator));
    } while (!is_err);
    track_stat(ctx->stat_itr_close, timed_fdb_iterator_close(iterator));

}

void deletes(fdb_kvs_handle *db, int pos) {

    int i;
    char keybuf[256];
    fdb_doc *doc = NULL;

    // deletes sequential docs
    for (i = 0; i < 1000; i++){
        sprintf(keybuf, "%d_%dseqkey", pos, i);
        fdb_doc_create(&doc, (void*)keybuf, strlen(keybuf), NULL, 0, NULL, 0);
        fdb_del(db, doc);
        fdb_doc_free(doc);
    }
}

void do_bench(){

    int i, j, r;
    int n_loops = 5;
    int n_kvs = 16;

    char cmd[64], fname[64], dbname[64];
    int n2_kvs = n_kvs * n_kvs;

    // file handlers
    fdb_status status;
    fdb_file_handle **dbfile = alca(fdb_file_handle*, n_kvs);
    fdb_kvs_handle **db = alca(fdb_kvs_handle*, n2_kvs);
    fdb_kvs_handle **snap_db = alca(fdb_kvs_handle*, n2_kvs);
    fdb_kvs_config kvs_config = fdb_get_default_kvs_config();
    fdb_config fconfig = fdb_get_default_config();

    // reader stats
    reader_context *ctx = alca(reader_context, n2_kvs);

    StatCollector *sa = new StatCollector(4, n2_kvs);

    for (i = 0; i < n2_kvs; ++i) {
        sa->t_stats[0][i].name.assign(ST_ITR_INIT);
        sa->t_stats[1][i].name.assign(ST_ITR_NEXT);
        sa->t_stats[2][i].name.assign(ST_ITR_GET);
        sa->t_stats[3][i].name.assign(ST_ITR_CLOSE);
        ctx[i].stat_itr_init = &sa->t_stats[0][i];
        ctx[i].stat_itr_next = &sa->t_stats[1][i];
        ctx[i].stat_itr_get = &sa->t_stats[2][i];
        ctx[i].stat_itr_close = &sa->t_stats[3][i];
    }

    sprintf(cmd, "rm bench* > errorlog.txt");
    r = system(cmd);
    (void)r;

    // setup
    fconfig.compaction_mode = FDB_COMPACTION_MANUAL;
    fconfig.auto_commit = false;
    fconfig.compactor_sleep_duration = 600;
    fconfig.prefetch_duration = 0;
    fconfig.num_compactor_threads = 1;
    fconfig.num_bgflusher_threads = 0;

    // open 16 dbfiles each with 16 kvs
    for (i = 0; i < n_kvs; ++i){
        sprintf(fname, "bench%d",i);
        status = fdb_open(&dbfile[i], fname, &fconfig);
        assert(status == FDB_RESULT_SUCCESS);

        for (j = i*n_kvs; j < (i*n_kvs + n_kvs); ++j){
            sprintf(dbname, "db%d",j);
            status = fdb_kvs_open(dbfile[i], &db[j],
                                  dbname, &kvs_config);
            assert(status == FDB_RESULT_SUCCESS);
        }
    }

    for (i = 0; i < 10; ++i){
        // generate initial commit headers
        for (i = 0; i < n_kvs; i++){
            status = fdb_commit(dbfile[i], FDB_COMMIT_MANUAL_WAL_FLUSH);
            assert(status == FDB_RESULT_SUCCESS);
        }
    }

    for (j = 0; j < n_loops; j++){

        // write to single file 1 kvs
        writer(db[0], 0);

        // reads from single file 1 kvs
        ctx[0].handle = db[0];
        reader(&ctx[0]);

        // snap iterator read
        status = fdb_snapshot_open(db[0], &snap_db[0],
                                   FDB_SNAPSHOT_INMEM);
        assert(status == FDB_RESULT_SUCCESS);
        ctx[0].handle = snap_db[0];
        reader(&ctx[0]);

       // write/read/snap to single file 16 kvs
        for (i = 0;i < n_kvs; ++i){
            writer(db[i], i);
        }
        for (i = 0; i < n_kvs; ++i){
            deletes(db[i], i);
        }
        for (i = 0; i < n_kvs; ++i){
            ctx[i].handle = db[i];
            reader(&ctx[i]);
        }
        for (i = 0; i < n_kvs; ++i){
            status = fdb_snapshot_open(db[i], &snap_db[i],
                                       FDB_SNAPSHOT_INMEM);
            assert(status == FDB_RESULT_SUCCESS);
            ctx[i].handle = snap_db[i];
            reader(&ctx[i]);
        }

        // commit single file
        status = fdb_commit(dbfile[0], FDB_COMMIT_MANUAL_WAL_FLUSH);
        assert(status == FDB_RESULT_SUCCESS);

        // write/write/snap to 16 files 1 kvs
        for (i = 0; i < n2_kvs; i += n_kvs){ // every 16 kvs is new file
            writer(db[i], i);
        }
        for (i = 0; i < n2_kvs; i += n_kvs){
            deletes(db[i], i);
        }
        for (i = 0; i < n2_kvs; i += n_kvs){ // every 16 kvs is new file
            ctx[i].handle = db[i];
            reader(&ctx[i]);
        }
        for (i = 0; i < n2_kvs; i += n_kvs){
            status = fdb_snapshot_open(db[i], &snap_db[i],
                                       FDB_SNAPSHOT_INMEM);
            assert(status == FDB_RESULT_SUCCESS);
            ctx[i].handle = snap_db[i];
            reader(&ctx[i]);
        }

        // write to 16 files 16 kvs each
        for (i = 0; i < n2_kvs; i++){
            writer(db[i], i);
        }
        for (i = 0; i < n2_kvs; ++i){
            deletes(db[i], i);
        }
        for (i = 0; i < n2_kvs; i++){
            ctx[i].handle = db[i];
            reader(&ctx[i]);
        }
        for (i = 0; i < n2_kvs; i++){
            status = fdb_snapshot_open(db[i], &snap_db[i],
                                       FDB_SNAPSHOT_INMEM);
            assert(status == FDB_RESULT_SUCCESS);
            ctx[i].handle = snap_db[i];
            reader(&ctx[i]);
        }

        // commit all
        for (i = 0;i < n_kvs; i++){
            status = fdb_commit(dbfile[i], FDB_COMMIT_MANUAL_WAL_FLUSH);
            assert(status == FDB_RESULT_SUCCESS);
        }
    }
    // compact all
    for (i = 0; i < n_kvs; i++){
        status = fdb_compact(dbfile[i], NULL);
        assert(status == FDB_RESULT_SUCCESS);
    }

    // print aggregated reader stats
    sa->aggregateAndPrintAll("ITERATOR_TEST_STATS", n_kvs * n_kvs, "µs");
    delete sa;

    // print aggregated dbfile stats
    print_db_stats(dbfile, n_kvs);

    // cleanup
    for(i = 0; i < n2_kvs; i++){
        fdb_kvs_close(db[i]);
        fdb_kvs_close(snap_db[i]);
    }
    for(i = 0; i < n_kvs; i++){
        fdb_close(dbfile[i]);
    }

    fdb_shutdown();

    sprintf(cmd, "rm bench* > errorlog.txt");
    r = system(cmd);
    (void)r;
}

/*
 *  ===================
 *  FDB BENCH MARK TEST
 *  ===================
 *  Performs unit benchmarking with 16 dbfiles each with max 16 kvs
 */
int main(int argc, char* args[]) {
    do_bench();
}
