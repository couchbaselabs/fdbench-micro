#include <stdio.h>
#include <assert.h>
#include "config.h"
#include "timing.h"
#include <libforestdb/forestdb.h>

void init_stat(stat_history_t *stat, const char *name){
    memset(stat, 0, sizeof(stat_history_t));
    strcpy(stat->name, name);
}

bool track_stat(stat_history_t *stat, uint64_t lat){
    if(lat == ERR_NS){
      return false;
    }

    stat->latency += lat;
    stat->n++;
    return true;
}

void print_stat(const char *name, float latency){
    printf("%-15s %f\n", name, latency);
}

void print_stat_history(stat_history_t stat){
  float avg = float(stat.latency)/float(stat.n);
  print_stat(stat.name, avg);
}

void print_aggregate_stats_history(stat_history_t *stats, int n){
    if(n==0){return;}

    int i;
    stat_history_t stats_total;
    init_stat(&stats_total, stats[0].name);

    for(i=0;i<n;i++){
        stats_total.latency += stats[i].latency;
        stats_total.n += stats[i].n;
    }
    print_stat_history(stats_total);

}
void print_stats(fdb_file_handle *h){

    int i;
    fdb_status status;
    fdb_latency_stat stat;
    const char *name;

    for (i=0;i<FDB_LATENCY_NUM_STATS;i++){
        memset(&stat, 0, sizeof(fdb_latency_stat));
        status = fdb_get_latency_stats(h, &stat, i);
        assert(status == FDB_RESULT_SUCCESS);
        if(stat.lat_count==0){
          continue;
        }
        name = fdb_latency_stat_name(i);
        printf("%-15s %u\n", name, stat.lat_avg);
    }
}

void print_stats_aggregate(fdb_file_handle **dbfiles, int nfiles){

    int i,j;
    uint32_t cnt, sum, avg = 0;
    fdb_status status;
    fdb_latency_stat stat;
    const char *name;

    for (i=0;i<FDB_LATENCY_NUM_STATS;i++){

        cnt = 0; sum = 0;

        // avg of each stat across dbfiles
        for(j=0;j<nfiles;j++){
            memset(&stat, 0, sizeof(fdb_latency_stat));
            status = fdb_get_latency_stats(dbfiles[j], &stat, i);
            assert(status == FDB_RESULT_SUCCESS);
            cnt += stat.lat_count;
            sum += stat.lat_avg;
        }
        if(cnt > 0){
          name = fdb_latency_stat_name(i);
          avg = sum/nfiles;
          printf("%-15s %u\n", name, avg);
        }
    }
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
    while(i<len){
        s[i] = alphanum[i%n_ch];
        i++;
    }
    s[len-1] = '\0';
}

void swap(char *x, char *y)
{
    char temp;
    temp = *x;
    *x = *y;
    *y = temp;
}

void permute(fdb_kvs_handle *kv, char *a, int l, int r)
{

    int i;
    char keybuf[256], metabuf[256], bodybuf[1024];
    fdb_doc *doc = NULL;
    str_gen(bodybuf, 1024);

    if (l == r) {
        sprintf(keybuf, a, l);
        sprintf(metabuf, "meta%d", r);
        fdb_doc_create(&doc, (void*)keybuf, strlen(keybuf),
            (void*)metabuf, strlen(metabuf), (void*)bodybuf, strlen(bodybuf));
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

void sequential(fdb_kvs_handle *kv, int pos)
{
    int i;
    char keybuf[256], metabuf[256], bodybuf[512];
    fdb_doc *doc = NULL;
    str_gen(bodybuf, 512);

    // load flat keys
    for(i=0;i<1000;i++){
        sprintf(keybuf, "%d_%dseqkey", pos, i);
        sprintf(metabuf, "meta%d", i);
        fdb_doc_create(&doc, (void*)keybuf, strlen(keybuf),
            (void*)metabuf, strlen(metabuf), (void*)bodybuf, strlen(bodybuf));
        fdb_set(kv, doc);
        fdb_doc_free(doc);
    }
}

void setup_db(fdb_file_handle **fhandle, fdb_kvs_handle **kv){

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


void writer(fdb_kvs_handle *db, int pos){

    char keybuf[KEY_SIZE];

    str_gen(keybuf, KEY_SIZE);
    permute(db, keybuf, 0, PERMUTED_BYTES);
    sequential(db, pos);
}

void reader(reader_context *ctx){

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

void deletes(fdb_kvs_handle *db, int pos){

    int i;
    char keybuf[256];
    fdb_doc *doc = NULL;

    // deletes sequential docs
    for(i=0;i<1000;i++){
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
    int n2_kvs = n_kvs*n_kvs;

    // file handlers
    fdb_status status;
    fdb_file_handle **dbfile = alca(fdb_file_handle*, n_kvs);
    fdb_kvs_handle **db = alca(fdb_kvs_handle*, n2_kvs);
    fdb_kvs_handle **snap_db = alca(fdb_kvs_handle*, n2_kvs);
    fdb_kvs_config kvs_config = fdb_get_default_kvs_config();
    fdb_config fconfig = fdb_get_default_config();

    // reader stats
    reader_context *ctx = alca(reader_context, n2_kvs);
    stat_history_t *t_stats_itr_init = alca(stat_history_t, n2_kvs);
    stat_history_t *t_stats_itr_get = alca(stat_history_t, n2_kvs);
    stat_history_t *t_stats_itr_next = alca(stat_history_t, n2_kvs);
    stat_history_t *t_stats_itr_close = alca(stat_history_t, n2_kvs);

    for(i=0;i<n2_kvs;++i){
        init_stat(&t_stats_itr_init[i], ST_ITR_INIT);
        init_stat(&t_stats_itr_get[i], ST_ITR_NEXT);
        init_stat(&t_stats_itr_next[i], ST_ITR_GET);
        init_stat(&t_stats_itr_close[i], ST_ITR_CLOSE);
        ctx[i].stat_itr_init = &t_stats_itr_init[i];
        ctx[i].stat_itr_get = &t_stats_itr_get[i];
        ctx[i].stat_itr_next = &t_stats_itr_next[i];
        ctx[i].stat_itr_close = &t_stats_itr_close[i];
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
    for (i=0; i < n_kvs; ++i){
        sprintf(fname, "bench%d",i);
        status = fdb_open(&dbfile[i], fname, &fconfig);
        assert(status == FDB_RESULT_SUCCESS);

        for (j=i*n_kvs; j < (i*n_kvs + n_kvs); ++j){
            sprintf(dbname, "db%d",j);
            status = fdb_kvs_open(dbfile[i], &db[j],
                                  dbname, &kvs_config);
            assert(status == FDB_RESULT_SUCCESS);
        }
    }

    for(i=0;i<10;++i){
        // generate initial commit headers
        for (i=0;i<n_kvs;i++){
            status = fdb_commit(dbfile[i], FDB_COMMIT_MANUAL_WAL_FLUSH);
            assert(status == FDB_RESULT_SUCCESS);
        }
    }

    for(j=0;j<n_loops;j++){

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
        for (i=0;i<n_kvs;++i){
            writer(db[i], i);
        }
        for (i=0;i<n_kvs;++i){
            deletes(db[i], i);
        }
        for (i=0;i<n_kvs;++i){
            ctx[i].handle = db[i];
            reader(&ctx[i]);
        }
        for (i=0;i<n_kvs;++i){
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
        for (i=0;i<n2_kvs;i+=n_kvs){ // every 16 kvs is new file
            writer(db[i], i);
        }
        for (i=0;i<n2_kvs;i+=n_kvs){
            deletes(db[i], i);
        }
        for (i=0;i<n2_kvs;i+=n_kvs){ // every 16 kvs is new file
            ctx[i].handle = db[i];
            reader(&ctx[i]);
        }
        for (i=0;i<n2_kvs;i+=n_kvs){
            status = fdb_snapshot_open(db[i], &snap_db[i],
                                       FDB_SNAPSHOT_INMEM);
            assert(status == FDB_RESULT_SUCCESS);
            ctx[i].handle = snap_db[i];
            reader(&ctx[i]);
        }

        // write to 16 files 16 kvs each
        for (i=0;i<n2_kvs;i++){
            writer(db[i], i);
        }
        for (i=0;i<n2_kvs;++i){
            deletes(db[i], i);
        }
        for (i=0;i<n2_kvs;i++){
            ctx[i].handle = db[i];
            reader(&ctx[i]);
        }
        for (i=0;i<n2_kvs;i++){
            status = fdb_snapshot_open(db[i], &snap_db[i],
                                       FDB_SNAPSHOT_INMEM);
            assert(status == FDB_RESULT_SUCCESS);
            ctx[i].handle = snap_db[i];
            reader(&ctx[i]);
        }

        // commit all
        for (i=0;i<n_kvs;i++){
            status = fdb_commit(dbfile[i], FDB_COMMIT_MANUAL_WAL_FLUSH);
            assert(status == FDB_RESULT_SUCCESS);
        }
    }
    // compact all
    for (i=0;i<n_kvs;i++){
        status = fdb_compact(dbfile[i], NULL);
        assert(status == FDB_RESULT_SUCCESS);
    }

    // print aggregated reader stats
    print_aggregate_stats_history(t_stats_itr_init, n2_kvs);
    print_aggregate_stats_history(t_stats_itr_next, n2_kvs);
    print_aggregate_stats_history(t_stats_itr_get, n2_kvs);
    print_aggregate_stats_history(t_stats_itr_close, n2_kvs);

    // print aggregated dbfile stats
    print_stats_aggregate(dbfile, n_kvs);

    // cleanup
    for(i=0;i<n2_kvs;i++){
        fdb_kvs_close(db[i]);
        fdb_kvs_close(snap_db[i]);
    }
    for(i=0;i<n_kvs;i++){
        fdb_close(dbfile[i]);
    }
    fdb_shutdown();
}

/*
 *  ===================
 *  FDB BENCH MARK TEST
 *  ===================
 *  Performs unit benchmarking with 16 dbfiles each with max 16 kvs
 */
int main(int argc, char* args[])
{
    do_bench();
}
