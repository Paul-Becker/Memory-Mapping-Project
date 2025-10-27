/* 
 * Authors: Paul Becker, Zhan Su, William Waweru, Gan-Orgil Gantumur
 * Assignment Name: Multithreaded File IO Program - Fix 
 * Assignment Description: Make this slow program that writes
 * 			   to disk too much use mmap() so that
 * 			   it's faster
 * Due Date: 10/26/2025
 * Date Created: 10/17/2025
 * Date Last Modified: 10/26/2025
*/

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <iostream>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

using clock_type = std::chrono::steady_clock;

struct Record {
    uint64_t id;
    uint64_t payload; // arbitrary payload (e.g., some data to "process")
};

struct Result {
    uint64_t id;
    uint64_t checksum; // simple computed function of the payload
};

struct SharedState {
    std::atomic<uint64_t> write_idx{0}; // how many records producer has written
    std::atomic<uint64_t> read_idx{0};  // next record index to be read
    uint64_t total;                      // total records to produce
    std::mutex m_comm;                   // protects comm file writes (baseline)
    std::mutex m_results;                // protects results file appends
    std::mutex m_log;                    // protects logging
    std::condition_variable cv;         // notify workers when new data arrives
    std::atomic<bool> done{false};      // Changed to atomic for lock-free check
};

// simple workload: reversible 64-bit mix
static inline uint64_t mix(uint64_t x) {
    x ^= x >> 33; x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33; x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33; return x;
}

void log_line(std::ofstream& log, std::mutex& m, const std::string& s) {
    std::lock_guard<std::mutex> lock(m);
    log << s << '\n';
    //log.flush();
}

int main(int argc, char** argv) {
    const uint64_t N = (argc > 1) ? std::stoull(argv[1]) : 100000;
    const unsigned   W = (argc > 2) ? static_cast<unsigned>(std::stoul(argv[2])) : std::thread::hardware_concurrency();
    const size_t length = 100 * 1024 * 1024; // 100 MB
    
    // Use unique_ptr with array of atomics to avoid copy constructor issue
    std::unique_ptr<std::atomic<char>[]> ready(new std::atomic<char>[N]);
    for (size_t i = 0; i < N; ++i) {
        ready[i].store(0, std::memory_order_relaxed);
    }

    // kept log as an fstream so we could still log if we crash
    std::ofstream log("app.log", std::ios::out | std::ios::trunc);
    if (!log) { std::cerr << "Failed to open log\n"; return 1; }

    // using these and truncating later because I was crashing
    // from having the wrong length
    const size_t commLength = static_cast<size_t>(N) * sizeof(Record);
    const size_t resultsLength = static_cast<size_t>(N) * sizeof(Result);

    int commFD = ::open("comm.dat", O_RDWR | O_CREAT | O_TRUNC, 0644);
    int resultFD = ::open("results.dat", O_RDWR | O_CREAT | O_TRUNC, 0644);

    if (commFD == -1 || resultFD == -1) {
	perror("couldn't open comm or result\n");
	return 1;
    }

    // truncating here now
    if (ftruncate(commFD, static_cast<off_t>(commLength)) == -1) {
	perror("couldn't truncatre comm\n");
	return 1;
    }
    if (ftruncate(resultFD, static_cast<off_t>(resultsLength)) == -1) {
	perror("couldn't truncate results\n");
	return 1;
    }

    // set up the maps
    void* commMap = mmap(nullptr, commLength, PROT_READ | PROT_WRITE,
		         MAP_SHARED, commFD, 0); 
    void* resultsMap = mmap(nullptr, resultsLength, PROT_READ | PROT_WRITE,
		            MAP_SHARED, resultFD, 0);

    if (resultsMap == MAP_FAILED || commMap == MAP_FAILED) {
	perror("mmap broke\n");
	return 1;
    }

    Record* commData = static_cast<Record*>(commMap);
    Result* resultData = static_cast<Result*>(resultsMap);

    SharedState S;
    S.total = N;

    auto t0 = clock_type::now();

    // Producer: writes N records to comm.dat (binary), logs progress
    std::thread producer([&]{
        for (uint64_t i = 0; i < N; ++i) {
            Record r{ i, mix(i) };
            
            // Direct write to mmap - no mutex needed!
            commData[i] = r;
            
            // Mark as ready with memory barrier
            ready[i].store(1, std::memory_order_release);
            S.write_idx.store(i + 1, std::memory_order_release);
            
            // Batch notifications - reduce syscall overhead
            if ((i & 0x3FF) == 0x3FF) { // Every 1024 records
                S.cv.notify_all();
            }

            if ((i & 0xFFFF) == 0) {
                log_line(log, S.m_log, "Produced up to id=" + std::to_string(i));
            }
        }
        
        {
            std::lock_guard<std::mutex> lk(S.m_log);
            log << "Producer finished N=" << N << '\n';
            log.flush();
        }
        
        S.done.store(true, std::memory_order_release);
        S.cv.notify_all(); // Final notification
    });

    // Worker threads: consume records from comm.dat in-order and append results
    std::vector<std::thread> workers;
    workers.reserve(W);
    for (unsigned w = 0; w < W; ++w) {
        workers.emplace_back([&, w]{
            while (true) {
                // claim next index
                uint64_t my_idx = S.read_idx.fetch_add(1, std::memory_order_acq_rel);
                
                if (my_idx >= N) break;
                
                // Spin-wait for ready flag
                while (!ready[my_idx].load(std::memory_order_acquire)) {
                    if (S.done.load(std::memory_order_acquire) && 
                        S.write_idx.load(std::memory_order_acquire) <= my_idx) {
                        return;
                    }
                    // Yield to avoid burning CPU
                    std::this_thread::yield();
                }
                
                // Direct memory operations
                Record r = commData[my_idx];
                Result out {r.id, mix(r.payload)};
                resultData[my_idx] = out;

                if ((my_idx & 0xFFFF) == 0) {
                    log_line(log, S.m_log, "Worker " + std::to_string(w) + 
                             " processed id=" + std::to_string(my_idx));
                }
            }
        });
    }

    producer.join();
    for (auto& t : workers) t.join();

    // Sync to disk only once at the end
    msync(resultsMap, resultsLength, MS_SYNC);
    msync(commMap, commLength, MS_SYNC);

    munmap(resultsMap, resultsLength);
    munmap(commMap, commLength);

    // just flushing the log once now
    {
        std::lock_guard<std::mutex> lk(S.m_log);
        log.flush();
    }

    close(commFD);
    close(resultFD);

    auto t1 = clock_type::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

    // Report
    double rec_per_ms = (ms > 0) ? static_cast<double>(N) / static_cast<double>(ms) : 0.0;
    std::cout << "mmap IO processed " << N << " records in " << ms << " ms\n";
    std::cout << "Throughput: " << std::fixed << std::setprecision(3) << rec_per_ms << " records/ms\n";
    return 0;
}