// Keeps the process under a memory target by moving DuckDB's buffer pool limit with the memory that DuckDB doesn't
// track itself: fetched WARC records, parsed HTML documents, HTTP response buffers and so on.
//
// DuckDB's memory_limit only covers its buffer pool, so a fixed limit is either too low for queries which only scan
// parquet or too high for queries which also fetch and parse pages. Every 20ms the governor sets
//
//   buffer pool limit = target - (memory in use - buffer pool usage) - headroom
//
// The headroom follows how fast the untracked memory has recently grown, so bursts like many parallel fetches get
// more room and steady queries like a parquet scan without warc_record() get almost all of the memory. DuckDB evicts
// caches or fails the query with an out of memory error before the process is killed. Memory in use is the largest of
// the resident set and the cgroup usage, which Lambda also counts and which includes the page cache.
// warc_record() also waits before fetching while the process is close to the target.
//
//   SET memory_target_mb = 3008;   -- starts the governor, 0 disables it

#include "extract_extension.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <chrono>
#include <fstream>
#include <thread>

#if defined(__linux__) && defined(__GLIBC__)
#include <malloc.h>
#include <unistd.h>
#define EXTRACT_MEMORY_GOVERNOR 1
#endif

namespace duckdb {

namespace {

constexpr idx_t MB = 1024 * 1024;
constexpr idx_t MINIMUM_POOL = 128 * MB;
constexpr auto INTERVAL = std::chrono::milliseconds(20);

std::atomic<idx_t> target_bytes(0);
std::atomic<bool> running(false);
//! Last values of the governor for memory_governor()
std::atomic<idx_t> last_limit(0), last_resident(0), last_untracked(0), last_cgroup(0);
//! Bytes reserved by MemoryReservation for allocations which are about to happen
std::atomic<idx_t> reserved(0);

//! Room for memory which grows faster than the governor reacts, moved by the recent growth rate
std::atomic<idx_t> dynamic_headroom(64 * MB);

idx_t Headroom(idx_t target) {
	return MinValue<idx_t>(dynamic_headroom.load(), target / 4);
}

#ifdef EXTRACT_MEMORY_GOVERNOR
//! Resident set size of the process
idx_t ResidentBytes() {
	std::ifstream statm("/proc/self/statm");
	idx_t size = 0, resident = 0;
	if (!(statm >> size >> resident)) {
		return 0;
	}
	return resident * idx_t(sysconf(_SC_PAGESIZE));
}

//! Memory of the cgroup, which also counts the page cache of written files. 0 if there is no cgroup.
idx_t CgroupBytes() {
	for (auto path : {"/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory/memory.usage_in_bytes"}) {
		std::ifstream file(path);
		idx_t bytes = 0;
		if (file >> bytes) {
			return bytes;
		}
	}
	return 0;
}

//! Heap memory which is allocated and not freed
idx_t HeapInUseBytes() {
	auto info = mallinfo2();
	return idx_t(info.uordblks) + idx_t(info.hblkhd);
}

void Loop(weak_ptr<DatabaseInstance> weak_db) {
	idx_t current_limit = 0;
	idx_t previous_untracked = 0;
	// Peak growth of untracked memory per interval, decays so that one spike doesn't shrink the pool for long
	double peak_growth = 0;
	auto last_trim = std::chrono::steady_clock::now();
	auto last_heap_check = last_trim;
	while (true) {
		std::this_thread::sleep_for(INTERVAL);
		auto target = target_bytes.load();
		auto db = weak_db.lock();
		if (!db || target == 0) {
			running = false;
			return;
		}
		auto &buffer_manager = BufferManager::GetBufferManager(*db);
		auto resident = ResidentBytes();
		auto cgroup = CgroupBytes();
		auto pool = buffer_manager.GetUsedMemory();

		// Freed memory which malloc keeps would look like used memory to Lambda, give it back. mallinfo2() walks every
		// malloc arena while holding its lock, so with many fetch threads it's only called once a second.
		auto now = std::chrono::steady_clock::now();
		if (now - last_heap_check > std::chrono::seconds(1)) {
			last_heap_check = now;
			if (resident > HeapInUseBytes() + 256 * MB && now - last_trim > std::chrono::seconds(1)) {
				malloc_trim(0);
				last_trim = now;
				resident = ResidentBytes();
			}
		}

		// The buffer pool is allocated from the heap, everything else in use is outside of DuckDB's limit
		auto used = MaxValue<idx_t>(resident, cgroup);
		auto untracked = used > pool ? used - pool : 0;
		last_resident = resident;
		last_cgroup = cgroup;
		last_untracked = untracked;
		auto growth = untracked > previous_untracked ? double(untracked - previous_untracked) : 0.0;
		previous_untracked = untracked;
		peak_growth = MaxValue<double>(growth, peak_growth * 0.98);
		// Enough for four intervals at the recent peak growth before the next adjustment catches up
		dynamic_headroom = MaxValue<idx_t>(64 * MB, idx_t(peak_growth * 4));
		auto headroom = Headroom(target);
		auto limit = target > untracked + headroom + MINIMUM_POOL ? target - untracked - headroom : MINIMUM_POOL;
		if (limit + 16 * MB < current_limit || limit > current_limit + 16 * MB) {
			try {
				// Lowering the limit evicts unpinned buffers. If pinned buffers don't fit the limit stays as it was.
				buffer_manager.SetMemoryLimit(limit);
				current_limit = limit;
				last_limit = limit;
			} catch (std::exception &) {
			}
		}
	}
}
//! An exception out of the detached thread would terminate the process
void Run(weak_ptr<DatabaseInstance> weak_db) {
	while (true) {
		try {
			Loop(weak_db);
		} catch (...) {
		}
		running = false;
		// SET memory_target_mb may have run while this thread was stopping and seen it as running
		if (target_bytes.load() == 0 || weak_db.expired() || running.exchange(true)) {
			return;
		}
	}
}
#endif

void SetMemoryTarget(ClientContext &context, SetScope scope, Value &parameter) {
	auto megabytes = parameter.IsNull() ? 0 : parameter.GetValue<idx_t>();
	target_bytes = megabytes * MB;
#ifdef EXTRACT_MEMORY_GOVERNOR
	if (megabytes > 0 && !running.exchange(true)) {
		std::thread(Run, weak_ptr<DatabaseInstance>(context.db)).detach();
	}
#else
	if (megabytes > 0) {
		throw NotImplementedException("memory_target_mb is only supported on Linux with glibc");
	}
#endif
}

} // namespace

MemoryReservation::MemoryReservation(idx_t needed) {
#ifdef EXTRACT_MEMORY_GOVERNOR
	auto target = target_bytes.load();
	if (target == 0) {
		return;
	}
	auto limit = target - Headroom(target) / 2;
	// Gives other fetches and the query time to free memory. Gives up after 10 seconds and lets the allocation fail.
	for (int attempt = 0; attempt < 1000; attempt++) {
		// The governor's last sample is at most 20ms old, the reservations cover what was allocated since then
		auto used = MaxValue<idx_t>(last_resident.load(), last_cgroup.load());
		auto current = reserved.load();
		if (used + current + needed <= limit || (attempt == 999)) {
			if (reserved.compare_exchange_weak(current, current + needed)) {
				bytes = needed;
				return;
			}
			continue;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}
#endif
}

namespace {
std::mutex budget_lock;
std::condition_variable budget_released;
idx_t budget_in_use = 0;
} // namespace

FetchBudget::FetchBudget(idx_t needed) {
	auto target = target_bytes.load();
	if (target == 0) {
		return;
	}
	auto limit = target / 2;
	bytes = MinValue<idx_t>(needed, limit);
	std::unique_lock<std::mutex> guard(budget_lock);
	budget_released.wait(guard, [&]() { return budget_in_use == 0 || budget_in_use + bytes <= limit; });
	budget_in_use += bytes;
}

FetchBudget::~FetchBudget() {
	if (bytes == 0) {
		return;
	}
	{
		std::lock_guard<std::mutex> guard(budget_lock);
		budget_in_use -= bytes;
	}
	budget_released.notify_all();
}

unique_ptr<FetchMemory> FetchMemory::TryReserve(idx_t needed, bool force) {
#ifdef EXTRACT_MEMORY_GOVERNOR
	auto target = target_bytes.load();
	if (target == 0) {
		return unique_ptr<FetchMemory>(new FetchMemory(0));
	}
	auto limit = target - Headroom(target) / 2;
	while (true) {
		auto used = MaxValue<idx_t>(last_resident.load(), last_cgroup.load());
		auto current = reserved.load();
		if (!force && used + current + needed > limit) {
			return nullptr;
		}
		if (reserved.compare_exchange_weak(current, current + needed)) {
			return unique_ptr<FetchMemory>(new FetchMemory(needed));
		}
	}
#else
	return unique_ptr<FetchMemory>(new FetchMemory(0));
#endif
}

FetchMemory::~FetchMemory() {
	if (bytes > 0) {
		reserved -= bytes;
	}
}

MemoryReservation::~MemoryReservation() {
	if (bytes > 0) {
		reserved -= bytes;
	}
}

namespace {

void MemoryGovernorFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	child_list_t<Value> values = {{"target_mb", Value::UBIGINT(target_bytes / MB)},
	                              {"buffer_pool_limit_mb", Value::UBIGINT(last_limit / MB)},
	                              {"resident_mb", Value::UBIGINT(last_resident / MB)},
	                              {"untracked_mb", Value::UBIGINT(last_untracked / MB)},
	                              {"cgroup_mb", Value::UBIGINT(last_cgroup / MB)}};
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	result.SetValue(0, Value::STRUCT(std::move(values)));
}

} // namespace

void RegisterMemoryGovernor(ExtensionLoader &loader) {
	child_list_t<LogicalType> fields = {{"target_mb", LogicalType::UBIGINT},
	                                    {"buffer_pool_limit_mb", LogicalType::UBIGINT},
	                                    {"resident_mb", LogicalType::UBIGINT},
	                                    {"untracked_mb", LogicalType::UBIGINT},
	                                    {"cgroup_mb", LogicalType::UBIGINT}};
	ScalarFunction memory_governor("memory_governor", {}, LogicalType::STRUCT(fields), MemoryGovernorFunction);
	memory_governor.SetVolatile();
	loader.RegisterFunction(memory_governor);

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption("memory_target_mb",
	                          "Keep the process under this many megabytes by moving the buffer pool limit with the "
	                          "memory used outside of DuckDB, 0 disables it",
	                          LogicalType::UBIGINT, Value::UBIGINT(0), SetMemoryTarget);
}

} // namespace duckdb
