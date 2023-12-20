#include "DSM.h"
#include "GlobalAddress.h"
#include "Common.h"
#include "LocalLockTable.h"
#include "Timer.h"
#include "zipf.h"

#include <city.h>
#include <stdlib.h>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <random>


#define TEST_EPOCH 10
#define TIME_INTERVAL 1

#define MAX_THREAD_REQUEST 5000000
#define LOAD_HEARTBEAT 100000

// #define USE_YCSB_A

int kReadRatio;
int kSpanSize;
int kThreadCount;
int kNodeCount = 1;
bool kFineGrained = true;

uint64_t addressCnt = 1000000;
uint64_t msgSize = 16;  // need 8B-align
double zipfan = 0.99;

std::thread th[MAX_APP_THREAD];
uint64_t tp[MAX_APP_THREAD][MAX_CORO_NUM];

volatile bool need_stop = false;
extern volatile bool need_clear[MAX_APP_THREAD];
uint64_t latency[MAX_APP_THREAD][MAX_CORO_NUM][LATENCY_WINDOWS];
uint64_t latency_th_all[LATENCY_WINDOWS];

DSM *dsm;

struct OPAddress {
  GlobalAddress kv_addr;
  GlobalAddress lock_addr;
  uint64_t mask;

  OPAddress(const GlobalAddress& kv_addr, const GlobalAddress& lock_addr, uint64_t mask) : kv_addr(kv_addr), lock_addr(lock_addr), mask(mask) {}
};

inline OPAddress to_offset(uint64_t k) {
  uint64_t block_idx, kv_idx, mask;
  GlobalAddress block_addr, kv_addr, lock_addr;

  // [span-size-block 8B-lock] [span-size-block 8B-lock] ...
#ifdef USE_YCSB_A
  kv_idx = k % addressCnt;
#else
  kv_idx = (CityHash64((char *)&k, sizeof(k))) % addressCnt;
#endif
  block_idx = kv_idx / kSpanSize;
  block_addr = GlobalAddress{k % MEMORY_NODE_NUM, block_idx * (msgSize * kSpanSize + sizeof(uint64_t))};
  lock_addr = block_addr + msgSize * kSpanSize;
  kv_idx = kv_idx % kSpanSize;
  kv_addr = block_addr + msgSize * kv_idx;
  mask = kFineGrained ? (1ULL << (64 * kv_idx / kSpanSize)) : ~0ULL;

  return OPAddress{kv_addr, lock_addr, mask};
}

#ifdef USE_YCSB_A
struct Request {
  bool is_search;
  bool is_update;
  uint64_t k;
};

class RequestGenBench {
public:
  RequestGenBench(DSM *dsm, Request* req, int coro_id): local_thread_id(dsm->getMyThreadID()), coro_id(coro_id), req(req), cur(0) {}

  OPAddress next(bool &is_read) {
    // is_read = (rand_r(&seed) % 100) < kReadRatio;
    is_read = req[cur].is_search;
    tp[local_thread_id][coro_id]++;
    return to_offset(req[cur ++].k);
  }

private:
  int local_thread_id;
  int coro_id;
  Request* req;
  int cur;
};
#else
class AddressGenBench {
public:
  AddressGenBench(DSM *dsm, int coro_id): local_thread_id(dsm->getMyThreadID()), coro_id(coro_id), u(0, 100) {
    // seed = rdtsc();
    mehcached_zipf_init(&state, addressCnt, zipfan,
                        (rdtsc() & (0x0000ffffffffffffull)) ^ local_thread_id);
  }

  OPAddress next(bool &is_read) {
    // is_read = (rand_r(&seed) % 100) < kReadRatio;
    is_read = u(e) < kReadRatio;
    uint64_t dis = mehcached_zipf_next(&state);
    tp[local_thread_id][coro_id]++;
    return to_offset(dis);
  }

private:
  int local_thread_id;
  int coro_id;
  // unsigned int seed;
  std::uniform_int_distribution<int> u;
  std::default_random_engine e;
  struct zipf_gen_state state;
};
#endif

void do_read(DSM *dsm, const GlobalAddress& kv_addr) {
  Value v = 0;
  auto read_buffer = (dsm->get_rbuf(nullptr)).get_leaf_buffer();
  dsm->read_sync(read_buffer, kv_addr, msgSize);
  v = *(Value *)read_buffer;
  UNUSED(v);
  return;
}


uint64_t do_write(DSM *dsm, const GlobalAddress& lock_addr, const GlobalAddress& kv_addr, const uint64_t mask) {
  Value v = 0xffff;
  auto cas_buffer = (dsm->get_rbuf(nullptr)).get_cas_buffer();
  auto write_buffer = (dsm->get_rbuf(nullptr)).get_leaf_buffer();
  bool res;
  uint64_t cas_fail_cnt = 0;

  // unlock function
  auto unlock = [=](const GlobalAddress &lock_addr){
    dsm->cas_mask_sync(lock_addr, ~0ULL, 0ULL, cas_buffer, mask);
  };

  // write back function
  auto write_back = [=](const Value &v){
    *(Value *)write_buffer = v;
    dsm->write_sync(write_buffer, kv_addr, msgSize);
  };

  // acquire lock
re_acquire:
  res = dsm->cas_mask_sync(lock_addr, 0ULL, ~0ULL, cas_buffer, mask);
  if (!res) {
    ++ cas_fail_cnt;
    goto re_acquire;
  }
  write_back(v);
  unlock(lock_addr);
  return cas_fail_cnt;
}


std::atomic<int64_t> warmup_cnt{0};
std::atomic_bool ready{false};


void thread_run(int id) {

  bindCore(id * 2 + 1);
  // bindCore(id + 1);
  dsm->registerThread();
  uint64_t my_id = kThreadCount * dsm->getMyNodeID() + id;
  printf("I am %lu\n", my_id);

#ifdef USE_YCSB_A
  // insert ycsb_a
  Request* req = new Request[MAX_THREAD_REQUEST];
  int req_num = 0;
  std::ifstream trans_in("../ycsb/workloads/txn_randint_workloada" + std::to_string(my_id));
  if (!trans_in.is_open()) {
    printf("Error opening trans file\n");
    assert(false);
  }
  std::string op;
  int cnt = 0;
  uint64_t int_k;
  while(trans_in >> op >> int_k) {
    Request r;
    r.is_search = (op == "READ");
    r.is_update = (op == "UPDATE");
    r.k = int_k;
    req[req_num ++] = r;
    if (++ cnt % LOAD_HEARTBEAT == 0) {
      printf("thread %d: %d trans entries loaded.\n", id, cnt);
    }
  }
#endif

  warmup_cnt.fetch_add(1);

  if (id == 0) {
    while (warmup_cnt.load() != kThreadCount)
      ;
    dsm->barrier("warm_finish");
    ready = true;
    warmup_cnt.store(-1);
  }
  while (warmup_cnt.load() != -1)
    ;

  Timer timer;
#ifdef USE_YCSB_A
  auto gen = new RequestGenBench(dsm, req, 0);
#else
  auto gen = new AddressGenBench(dsm, 0);
#endif
  auto thread_id = dsm->getMyThreadID();

  while (!need_stop) {
    bool is_read;
    auto op_addrs = gen->next(is_read);
    timer.begin();
    if (is_read) do_read(dsm, op_addrs.kv_addr);
    else do_write(dsm, op_addrs.lock_addr, op_addrs.kv_addr, op_addrs.mask);
    auto us_10 = timer.end() / 100;

    if (us_10 >= LATENCY_WINDOWS) {
      us_10 = LATENCY_WINDOWS - 1;
    }
    latency[thread_id][0][us_10]++;
  }
}

void parse_args(int argc, char *argv[]) {
  if (argc != 7) {
    printf("Usage: ./conflict_test kNodeCount kThreadCount kReadRatio kSpanSize zipfan kFineGrained\n");
    exit(-1);
  }

  kNodeCount = atoi(argv[1]);
  kThreadCount = atoi(argv[2]);
  kReadRatio = atoi(argv[3]);
  kSpanSize = atoi(argv[4]);
  zipfan = atof(argv[5]);
  kFineGrained = atoi(argv[6]) == 1 ? true : false;

  printf("kNodeCount %d, kThreadCount %d, kReadRatio %d, kSpanSize %d, zipfan %lf, kFineGrained %s\n", kNodeCount, kThreadCount, kReadRatio,
         kSpanSize, zipfan, kFineGrained ? "true" : "false");
}

void save_latency(int epoch_id) {
  // sum up local latency cnt
  for (int i = 0; i < LATENCY_WINDOWS; ++i) {
    latency_th_all[i] = 0;
    for (int k = 0; k < MAX_APP_THREAD; ++k)
      for (int j = 0; j < MAX_CORO_NUM; ++j) {
        latency_th_all[i] += latency[k][j][i];
    }
  }
  // store in file
  std::ofstream f_out("../us_lat/epoch_" + std::to_string(epoch_id) + ".lat");
  f_out << std::setiosflags(std::ios::fixed) << std::setprecision(1);
  if (f_out.is_open()) {
    for (int i = 0; i < LATENCY_WINDOWS; ++i) {
      f_out << i / 10.0 << "\t" << latency_th_all[i] << std::endl;
    }
    f_out.close();
  }
  else {
    printf("Fail to write file!\n");
    assert(false);
  }
  memset(latency, 0, sizeof(uint64_t) * MAX_APP_THREAD * MAX_CORO_NUM * LATENCY_WINDOWS);
}

int main(int argc, char *argv[]) {

  parse_args(argc, argv);

  DSMConfig config;
  config.machineNR = kNodeCount;
  config.threadNR = kThreadCount;
  dsm = DSM::getInstance(config);
  dsm->registerThread();
  bindCore(kThreadCount * 2 + 1);
  // bindCore(kThreadCount + 1);

  dsm->barrier("benchmark");

  for (int i = 0; i < kThreadCount; ++ i) {
    th[i] = std::thread(thread_run, i);
  }

  while (!ready.load())
    ;
  timespec s, e;
  uint64_t pre_tp = 0;
  int count = 0;

  clock_gettime(CLOCK_REALTIME, &s);
  while(!need_stop) {

    sleep(TIME_INTERVAL);
    clock_gettime(CLOCK_REALTIME, &e);
    int microseconds = (e.tv_sec - s.tv_sec) * 1000000 +
                       (double)(e.tv_nsec - s.tv_nsec) / 1000;

    uint64_t all_tp = 0;
    for (int i = 0; i < MAX_APP_THREAD; ++i) {
      for (int j = 0; j < MAX_CORO_NUM; ++j)
        all_tp += tp[i][j];
    }
    clock_gettime(CLOCK_REALTIME, &s);

    uint64_t cap = all_tp - pre_tp;
    pre_tp = all_tp;

    save_latency(++ count);

    double per_node_tp = cap * 1.0 / microseconds;
    uint64_t cluster_tp = dsm->sum((uint64_t)(per_node_tp * 1000));  // only node 0 return the sum

    printf("%d, throughput %.4f\n", dsm->getMyNodeID(), per_node_tp);

    if (dsm->getMyNodeID() == 0) {
      printf("epoch %d passed!\n", count);
      printf("cluster throughput %.3f Mops\n", cluster_tp / 1000.0);
      printf("\n");
    }
    if (count >= TEST_EPOCH) {
      need_stop = true;
    }
  }
  printf("[END]\n");

  for (int i = 0; i < kThreadCount; ++ i) {
    th[i].join();
    printf("Thread %d joined.\n", i);
  }
  dsm->barrier("fin");

  return 0;
}
