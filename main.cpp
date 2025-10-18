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
    bool done{false};
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
    log.flush();
}

int main(int argc, char** argv) {
    const uint64_t N = (argc > 1) ? std::stoull(argv[1]) : 100000;
    const unsigned   W = (argc > 2) ? static_cast<unsigned>(std::stoul(argv[2])) : std::thread::hardware_concurrency();
    const size_t length = 100 * 1024 * 1024; // 100 MB


    // Open files (truncate)
    std::ofstream comm("comm.dat", std::ios::binary | std::ios::trunc);
    std::ofstream results("results.dat", std::ios::binary | std::ios::trunc);
    std::ofstream log("app.log", std::ios::out | std::ios::trunc);
    if (!comm || !results || !log) { std::cerr << "Failed to open output files.\n"; return 1; }

    int fd = ::open("comm.dat", O_RDWR | O_CREAT, 0644);
    if (fd == -1) {perror("open broke"); return 1; }

    SharedState S;
    S.total = N;

    void* map = mmap(nullptr, length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    char* data = static_cast<char*>(map);

    if (map == MAP_FAILED) { perror("mmap"); ::close(fd); return 1; }

    auto t0 = clock_type::now();

    // Producer: writes N records to comm.dat (binary), logs progress
    std::thread producer([&]{
        for (uint64_t i = 0; i < N; ++i) {
            Record r{ i, mix(i) };
            {
                // shared file for communication
                std::lock_guard<std::mutex> lk(S.m_comm);
                std:: memcpy(data + (i*sizeof(r.payload)), &r, sizeof(r.payload))
                //comm.write(reinterpret_cast<const char*>(&r), sizeof(r));
                comm.flush(); // intentional for baseline (costly)
            }
            S.write_idx.fetch_add(1, std::memory_order_release);

            if ((i & 0xFFFF) == 0) {
                log_line(log, S.m_log, "Produced up to id=" + std::to_string(i));
            }
            S.cv.notify_all();
        }
        {
            std::lock_guard<std::mutex> lk(S.m_log);
            log << "Producer finished N=" << N << '\n';
            log.flush();
        }
        {
            std::lock_guard<std::mutex> lk(S.m_comm);
            comm.flush();
        }
        S.done = true;
        S.cv.notify_all();
    });

    // Worker threads: consume records from comm.dat in-order and append results
    std::vector<std::thread> workers;
    workers.reserve(W);
    for (unsigned w = 0; w < W; ++w) {
        workers.emplace_back([&, w]{
            std::ifstream reader("comm.dat", std::ios::binary);
            if (!reader) return;

            while (true) {
                // claim next index
                uint64_t my_idx = S.read_idx.fetch_add(1, std::memory_order_acq_rel);
                // wait until producer has written it (or finished)
                std::unique_lock<std::mutex> ul(S.m_comm); // lock only to pair with cv wait
                S.cv.wait(ul, [&]{ return S.write_idx.load(std::memory_order_acquire) > my_idx || S.done; });
                ul.unlock();

                if (my_idx >= S.write_idx.load(std::memory_order_acquire)) {
                    if (S.done) break;
                    // someone else progressed; retry
                    continue;
                }

                // read the record at position my_idx
                std::streampos offset = static_cast<std::streampos>(my_idx) * static_cast<std::streampos>(sizeof(Record));
                reader.clear();
                reader.seekg(offset, std::ios::beg);
                Record r{};
                reader.read(reinterpret_cast<char*>(&r), sizeof(r));
                if (!reader) break;

                // "Process" the payload
                Result out{ r.id, mix(r.payload) };

                // append to results file
                {
                    std::lock_guard<std::mutex> lk(S.m_results);

                    results.write(reinterpret_cast<const char*>(&out), sizeof(out));
                    results.flush(); // baseline intentionally flushes
                }

                if ((my_idx & 0xFFFF) == 0) {
                    log_line(log, S.m_log, "Worker " + std::to_string(w) + " processed id=" + std::to_string(my_idx));
                }

                if (my_idx + 1 >= N) break;
            }
        });
    }

    producer.join();
    for (auto& t : workers) t.join();

    auto t1 = clock_type::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

    // Report
    double rec_per_ms = (ms > 0) ? static_cast<double>(N) / static_cast<double>(ms) : 0.0;
    std::cout << "Baseline (fstream) processed " << N << " records in " << ms << " ms\n";
    std::cout << "Throughput: " << std::fixed << std::setprecision(3) << rec_per_ms << " records/ms\n";
    return 0;
}
