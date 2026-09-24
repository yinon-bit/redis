#include "server.h"
#include "forkless.h"
#include "listpack.h"
#include "stream.h"
#include "endianconv.h"
#include "cluster.h"
#include "cluster_asm.h"
#include <stdbool.h>
#include <fcntl.h>
#include <poll.h>

/*
 * Hybrid BGSAVE has one owner for each part of the snapshot:
 *
 * 1. The child owns the fork-time key list.
 * 2. The child sends DONTFORK key references to the parent.
 * 3. The parent serializes those values.
 * 4. A mutation pins the fork-time value until it is serialized.
 * 5. The child writes both sources to one RDB; the parent publishes it.
 *
 * Full forkless uses the separate nofork lifecycle below: main scans live
 * dictionaries incrementally and supplies owned copies to a parent worker.
 * It never calls fork and works without jemalloc arenas.
 */

#if defined(USE_JEMALLOC) && defined(__linux__)
#include <sys/mman.h>
#endif

/* ---------- Fork-visibility arenas ----------
 *
 * Jemalloc builds create two arenas at startup, including in fork mode:
 *
 *   A (forkless_arena_fork)   always inherited by the child.
 *   B (forkless_arena_nofork) fork visibility is flipped at runtime.
 *
 * A value's initial arena is chosen by type. Encoding changes may migrate it.
 * Switching bgsave-mode itself moves no objects: it flips B's
 * pages between MADV_DONTFORK and MADV_DOFORK, which is O(extents) instead of
 * O(keys). That keeps the dict entry tag permanently meaningful ("this value
 * lives in B") and lets the mode be modified at runtime.
 *
 * Page granularity is why the split has to be by placement rather than by
 * flipping alone: many small values share a page, so fork visibility cannot be
 * decided per object inside one arena. */
static unsigned int forkless_arena_fork = UINT_MAX;
static unsigned int forkless_arena_nofork = UINT_MAX;

/* Extents handed out by arena B, needed to flip their advice later. Fixed and
 * statically sized because extent hooks run with jemalloc's arena locks held:
 * allocating here could reenter the allocator and deadlock. */
#if defined(USE_JEMALLOC)
#define FORKLESS_MAX_EXTENTS 8192
typedef struct forklessExtent {
    void *addr;
    size_t size;
} forklessExtent;
static forklessExtent forkless_extents[FORKLESS_MAX_EXTENTS];
static size_t forkless_extent_count;
static int forkless_extents_overflowed;
static pthread_mutex_t forkless_extent_mutex = PTHREAD_MUTEX_INITIALIZER;
#endif
/* 1 while arena B is inherited by the child. */
static int forkless_nofork_forkable = 1;

static int forkless_save_active;
/* Set at save begin: does this save use the split child/parent protocol?
 * The child inherits it, so it never has to re-read live config. */
static int forkless_split_save;
static char forkless_rdb_tmp[256];       /* Complete RDB, pending publication. */
/* Set while the serializer can create pins. */
static redisAtomic int forkless_capture_enabled;
/* Frozen before fork/thread creation; CONFIG SET only affects the live DB.
 * Ordinary lookups already suppress LRU/LFU updates while the child exists.
 * A future truly no-fork implementation must protect that read path too. */
static struct {
    int policy;
    int lfu_decay_time;
    unsigned int lru_clock;
    unsigned long lfu_clock;
} forkless_eviction;

/* Pin-table states for keys mutated during the save. Untouched keys never get
 * a pin. Trusted-round pins may be emitted before their child work record. */
enum {
    FORKLESS_PIN_OBJECT = 1,     /* retained object, waiting to be written */
    FORKLESS_PIN_BYTES,          /* bytes captured before an in-place change */
    FORKLESS_PIN_WRITTEN
};

/* All fields are protected by forkless_mutex. */
typedef struct forklessPin {
    kvobj *kv;
    kvobj *snapshot;             /* Private copy; released by the main thread. */
    long long expire;
    uint8_t held;                /* Main thread must release this reference. */
    uint8_t evicted;             /* Prefer this snapshot once the child confirms it. */
    uint8_t retained_encoding;   /* Frozen on main; do not read live refcount word. */
    uint8_t early_pending;       /* Identity is still in the bounded early FIFO. */
    uint8_t awaiting_child;      /* Keep the tombstone until its child record arrives. */
    int state;
    sds encoded;                 /* Encoded key-value pair. */
    sds retained_key;            /* Borrowed while snapshot holds the original string. */
    size_t accounted_bytes;     /* Preserved-value estimate, including retained refs. */
} forklessPin;

/* Independent hash shards avoid rehashing every captured key in one critical
 * section. Only changed keys allocate slots; this is not an all-keys index. */
#define FORKLESS_PIN_SHARDS 4096
#define FORKLESS_PIN_SHARD_BITS 12
#define FORKLESS_DONE_CHUNK_SIZE 64
#define FORKLESS_CLEANUP_SLOTS 1024
#define FORKLESS_CLEANUP_USEC 1000
typedef struct forklessPinShard {
    forklessPin *slots;
    size_t count, cap;
} forklessPinShard;

typedef struct forklessDoneChunk {
    struct forklessDoneChunk *next;
    size_t count, released;
    kvobj *values[FORKLESS_DONE_CHUNK_SIZE];
    size_t bytes[FORKLESS_DONE_CHUNK_SIZE];
} forklessDoneChunk;

typedef struct forklessPinTable {
    forklessPinShard shards[FORKLESS_PIN_SHARDS];
    forklessDoneChunk *done_head, *done_tail;
    struct forklessPinTable *next;
    size_t cleanup_shard, cleanup_slot;
    size_t count, slot_bytes;
} forklessPinTable;

static forklessPinTable *forkless_pins;
/* Main-thread-only generations awaiting bounded cleanup. Never reused by a
 * later save; workers have exited before a table is detached here. */
static forklessPinTable *forkless_retired_pins;
/* Main-thread-only release work and a budget shared by all cleanup calls in
 * one event-loop turn, including calls made from eviction. */
static forklessDoneChunk *forkless_releasing;
static monotime forkless_cleanup_used;
static size_t forkless_cleanup_slots;

void forklessResetCleanupBudget(void) {
    forkless_cleanup_used = 0;
    forkless_cleanup_slots = 0;
}

typedef struct forklessWorkRec {
    kvobj *kv;
    intptr_t dbid;
} forklessWorkRec;

#define FORKLESS_WORK_BATCH_SIZE 256
static forklessWorkRec forkless_work_batch[FORKLESS_WORK_BATCH_SIZE];
static size_t forkless_work_batch_count;

/* Only trusted snapshot-start pins enter this bounded queue. Queue entries
 * contain identities, never pointers into a reallocatable pin-table shard. */
static forklessWorkRec forkless_early[FORKLESS_WORK_BATCH_SIZE];
static size_t forkless_early_head, forkless_early_count;
static int forkless_early_wake[2] = {-1, -1};
static kvstore **forkless_snapshot_dbs;
static int forkless_snapshot_dbnum;
static unsigned char forkless_snapshot_trimmed[CLUSTER_SLOTS / 8];
static unsigned long long forkless_early_keys;

/* Bounded scheduling window. Only child-confirmed records can enter it: a
 * mutation pin alone may refer to a key created after the fork. All fields
 * below are protected by forkless_mutex. Indices are stored as index + 1. */
typedef struct forklessReadyKey {
    forklessWorkRec work;
    unsigned int next_priority;
    int pending;
    int prioritized;
} forklessReadyKey;
static forklessReadyKey forkless_ready[FORKLESS_WORK_BATCH_SIZE];
static unsigned int forkless_ready_index[FORKLESS_WORK_BATCH_SIZE * 2];
static size_t forkless_ready_count;
static size_t forkless_ready_remaining;
static size_t forkless_ready_cursor;
/* Work-conserving turns: normally 32 records per source. A nearly full
 * early queue gets up to 64, but can never starve child confirmations. */
#define FORKLESS_SCHEDULE_QUANTUM 32
static int forkless_schedule_early;
static unsigned int forkless_schedule_run;
static unsigned int forkless_priority_head;
static unsigned int forkless_priority_tail;
static unsigned long long forkless_priority_keys;

static pthread_t forkless_serializer_tid;
static int forkless_serializer_started;
static redisAtomic int forkless_serializer_exited;
static int forkless_cleanup_pending;
static redisAtomic int forkless_serializer_error;
static int forkless_serializer_pipe[2] = {-1, -1}; /* Parent status → child. */
static int forkless_data_pipe[2] = {-1, -1};     /* Parent RDB bytes → child. */
static int forkless_work_pipe[2] = {-1, -1};       /* Child key refs → parent. */
/* Single output owner for both disk and diskless saves. */
static rio *forkless_child_rio;

#define FORKLESS_STATUS_OK  1
#define FORKLESS_STATUS_ERR 2

/* Protects the pin table and both active-encoder pointers. */
static pthread_mutex_t forkless_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t forkless_read_done = PTHREAD_COND_INITIALIZER;

/* Each thread publishes the object it reads outside forkless_mutex. */
static kvobj *forkless_serializer_kv;
static kvobj *forkless_capture_kv;
/* The current round uses 1 or 2; zero is an uninitialized mark. A successful
 * split save normalizes every helper-owned live object: read objects are
 * marked by the helper, captured objects and new values by the main thread.
 * Failed saves (or changed routing) disable skips until a complete recovery
 * save. No pre-save keyspace scan and no per-object generation counter. */
static unsigned forkless_round = 1;
static int forkless_marks_valid;
static int forkless_round_succeeded;
/* -1 means no split round has been issued in this process, not merely that
 * the last save failed. It is deliberately never reset by cleanup. */
static int forkless_marks_mode = -1;
static size_t forkless_marks_cap;
static unsigned long long forkless_capture_skipped;
static redisAtomic size_t forkless_retained_bytes;
static size_t forkless_retained_peak;
static unsigned long long forkless_limit_aborts;
static unsigned long long forkless_removed_pins;
/* Main-thread counters, cumulative for the process lifetime. */
static unsigned long long forkless_retained_strings, forkless_copied_values;
static unsigned long long forkless_copy_usec, forkless_reader_wait_usec;

/* Opt-in, cumulative elapsed microseconds. Helper counters are shared with
 * INFO; child counters live only in the fork and are reported in its log.
 * These are wall times, not CPU times, and must not be summed across processes. */
static redisAtomic unsigned long long forkless_helper_encode_usec;
static redisAtomic unsigned long long forkless_helper_work_wait_usec;
static redisAtomic unsigned long long forkless_helper_output_usec;
static unsigned long long forkless_child_encode_usec, forkless_child_input_usec;
static unsigned long long forkless_child_output_usec;
static unsigned long long forkless_child_work_wait_usec;
static size_t (*forkless_child_write)(rio *, const void *, size_t);

static size_t forklessTimedChildWrite(rio *rdb, const void *buf, size_t len) {
    unsigned long long started = getMonotonicUs();
    size_t result = forkless_child_write(rdb, buf, len);
    forkless_child_output_usec += getMonotonicUs() - started;
    return result;
}

unsigned long long forklessTimingStart(void) {
    return server.forkless_timing ? getMonotonicUs() : 0;
}

void forklessTimingChildEncode(unsigned long long start) {
    if (start) forkless_child_encode_usec += getMonotonicUs() - start;
}

static void forklessTimingAdd(redisAtomic unsigned long long *counter,
                             unsigned long long start) {
    if (start) atomicIncr(*counter, getMonotonicUs() - start);
}

/* Separate from forkless_mutex: buffer admission also runs while encoding
 * outside that lock. Never acquire forkless_mutex while holding this mutex.
 * Count requested tracking allocations, conservatively charged SDS capacity,
 * and the existing preserved-value estimates. This is not an RSS limit. */
static pthread_mutex_t forkless_memory_mutex = PTHREAD_MUTEX_INITIALIZER;
static size_t forkless_memory_bytes, forkless_memory_peak, forkless_memory_limit;
static unsigned long long forkless_memory_denials;

static int forklessMemoryReserve(size_t bytes) {
    pthread_mutex_lock(&forkless_memory_mutex);
    int fits = bytes <= SIZE_MAX - forkless_memory_bytes &&
        (!forkless_memory_limit || (bytes <= forkless_memory_limit &&
         forkless_memory_bytes <= forkless_memory_limit - bytes));
    if (fits) {
        forkless_memory_bytes += bytes;
        if (forkless_memory_bytes > forkless_memory_peak)
            forkless_memory_peak = forkless_memory_bytes;
    } else {
        forkless_memory_denials++;
    }
    pthread_mutex_unlock(&forkless_memory_mutex);
    return fits;
}

static void forklessMemoryRelease(size_t bytes) {
    pthread_mutex_lock(&forkless_memory_mutex);
    serverAssert(bytes <= forkless_memory_bytes);
    forkless_memory_bytes -= bytes;
    pthread_mutex_unlock(&forkless_memory_mutex);
}

static void forklessReleaseRetained(size_t bytes) {
    atomicDecr(forkless_retained_bytes, bytes);
    forklessMemoryRelease(bytes);
}

static void *forklessTrackingAlloc(size_t bytes) {
    if (!forklessMemoryReserve(bytes)) return NULL;
    return zcalloc(bytes);
}

static void forklessTrackingFree(void *ptr, size_t bytes) {
    if (!ptr) return;
    zfree(ptr);
    forklessMemoryRelease(bytes);
}

/* Charge a conservative header allowance and all allocated SDS capacity.
 * During growth both old and replacement buffers remain charged. */
static size_t forklessBufferCharge(sds s) {
    return sdsalloc(s) + sizeof(struct sdshdr64) + 1;
}

static void forklessBufferFree(sds s) {
    if (!s) return;
    size_t bytes = forklessBufferCharge(s);
    sdsfree(s);
    forklessMemoryRelease(bytes);
}

static size_t forklessBufferWrite(rio *r, const void *data, size_t len) {
    sds old = r->io.buffer.ptr;
    size_t used = sdslen(old);
    if (len > SIZE_MAX - used) return 0;
    size_t needed = used + len;
    if (sdsavail(old) < len) {
        /* Reserve allocator-rounding headroom before allocation. Shrink the
         * reservation to the actual SDS capacity immediately afterwards. */
        size_t overhead = sizeof(struct sdshdr64) + 1;
        if (needed > (SIZE_MAX - overhead) / 4) return 0;
        size_t capacity = needed < SDS_MAX_PREALLOC ? needed * 2 : needed + SDS_MAX_PREALLOC;
        /* Type 5 has no independent capacity field: changing length would
         * change the amount later released from our accounting. */
        if (capacity < 32) capacity = 32;
        size_t reserved = capacity * 2 + overhead;
        if (!forklessMemoryReserve(reserved)) return 0;
        sds next = sdstrynewlen(SDS_NOINIT, capacity);
        if (!next) {
            forklessMemoryRelease(reserved);
            return 0;
        }
        size_t actual = forklessBufferCharge(next);
        if (actual > reserved) {
            sdsfree(next);
            forklessMemoryRelease(reserved);
            return 0;
        }
        forklessMemoryRelease(reserved - actual);
        memcpy(next, old, used);
        sdssetlen(next, used);
        next[used] = '\0';
        forklessBufferFree(old);
        r->io.buffer.ptr = old = next;
    }
    memcpy(old + used, data, len);
    sdssetlen(old, needed);
    old[needed] = '\0';
    r->io.buffer.pos += len;
    return 1;
}

static int forklessInitBuffer(rio *r) {
    size_t charge = 128;
    if (!forklessMemoryReserve(charge)) return C_ERR;
    sds empty = sdsempty();
    serverAssert(forklessBufferCharge(empty) <= charge);
    forklessMemoryRelease(charge - forklessBufferCharge(empty));
    rioInitWithBuffer(r, empty);
    r->write = forklessBufferWrite;
    return C_OK;
}

sds forklessAppendInfo(sds info) {
    unsigned long long encode, work_wait, output;
    atomicGet(forkless_helper_encode_usec, encode);
    atomicGet(forkless_helper_work_wait_usec, work_wait);
    atomicGet(forkless_helper_output_usec, output);
    info = sdscatprintf(info,
        "forkless_timing_enabled:%d\r\n"
        "forkless_helper_encode_usec:%llu\r\n"
        "forkless_helper_work_wait_usec:%llu\r\n"
        "forkless_helper_output_usec:%llu\r\n",
        server.forkless_timing, encode, work_wait, output);
    pthread_mutex_lock(&forkless_memory_mutex);
    size_t snapshot_bytes = forkless_memory_bytes, snapshot_peak = forkless_memory_peak;
    unsigned long long denials = forkless_memory_denials;
    pthread_mutex_unlock(&forkless_memory_mutex);
    size_t retained;
    atomicGet(forkless_retained_bytes, retained);
    pthread_mutex_lock(&forkless_mutex);
    unsigned long long removed = forkless_removed_pins;
    unsigned long long early = forkless_early_keys;
    size_t records = 0, table_bytes = 0;
    if (forkless_pins) {
        records = forkless_pins->count;
        table_bytes = sizeof(*forkless_pins) + forkless_pins->slot_bytes;
    }
    pthread_mutex_unlock(&forkless_mutex);
    return sdscatprintf(info,
        "forkless_retained_strings:%llu\r\n"
        "forkless_copied_values:%llu\r\n"
        "forkless_copy_usec:%llu\r\n"
        "forkless_reader_wait_usec:%llu\r\n"
        "forkless_capture_skipped:%llu\r\n"
        "forkless_progress_recovery:%d\r\n"
        "forkless_retained_bytes:%zu\r\n"
        "forkless_retained_peak:%zu\r\n"
        "forkless_limit_aborts:%llu\r\n"
        "forkless_removed_pins:%llu\r\n"
        "forkless_pin_records:%zu\r\n"
        "forkless_pin_table_bytes:%zu\r\n"
        "forkless_snapshot_bytes:%zu\r\n"
        "forkless_snapshot_peak:%zu\r\n"
        "forkless_snapshot_denials:%llu\r\n"
        "forkless_early_keys:%llu\r\n",
        forkless_retained_strings, forkless_copied_values,
        forkless_copy_usec, forkless_reader_wait_usec,
        forkless_capture_skipped, !forkless_marks_valid,
        retained, forkless_retained_peak, forkless_limit_aborts,
        removed, records, table_bytes, snapshot_bytes, snapshot_peak, denials, early);
}

static int forklessChildRelayFrames(void);
static int forklessPtrInArena(void *ptr);
static void forklessCloseFd(int *fd);
static int forklessSerializerFailed(void);
static int forklessCaptureEnabled(void);
static void forklessNoForkCapture(redisDb *db, kvobj *kv);

static int forklessStreamActive(void) {
    return forkless_data_pipe[0] != -1 ||
           forkless_data_pipe[1] != -1;
}

/* ---------- DONTFORK arena ---------- */

#if defined(USE_JEMALLOC)
static extent_hooks_t forkless_hooks;
static extent_hooks_t *forkless_default_hooks;

/* MADV_DONTFORK/MADV_DOFORK only exist on Linux. Everywhere else the split
 * protocol still runs end to end, but the child can read these pages, so the
 * feature provides no CoW benefit and proves nothing about fault avoidance. */
static int forklessApplyAdvice(void *addr, size_t size, int forkable) {
#if defined(__linux__)
    if (madvise(addr, size, forkable ? MADV_DOFORK : MADV_DONTFORK) != 0) {
        int saved_errno = errno;
        serverLog(LL_WARNING, "forkless: madvise(%s) failed: %s",
                  forkable ? "MADV_DOFORK" : "MADV_DONTFORK", strerror(errno));
        errno = saved_errno;
        return C_ERR;
    }
#else
    UNUSED(addr);
    UNUSED(size);
    UNUSED(forkable);
#endif
    return C_OK;
}

/* Called from the extent hook, so it must not allocate. */
static void forklessExtentRegister(void *addr, size_t size) {
    pthread_mutex_lock(&forkless_extent_mutex);
    if (forkless_extent_count < FORKLESS_MAX_EXTENTS) {
        forkless_extents[forkless_extent_count].addr = addr;
        forkless_extents[forkless_extent_count].size = size;
        forkless_extent_count++;
    } else if (!forkless_extents_overflowed) {
        /* Without a complete extent list a flip back to forkable would leave
         * unrecorded DONTFORK pages behind, so pin the arena as non-forkable
         * and let CONFIG SET refuse the switch rather than corrupt a save. */
        forkless_extents_overflowed = 1;
        serverLog(LL_WARNING,
                  "forkless: extent registry full (%d); bgsave-mode can no "
                  "longer be switched to fork without a restart",
                  FORKLESS_MAX_EXTENTS);
    }
    /* Publish before the memory escapes so no free path can miss it. */
    zmallocNoTcacheWindowAdd(addr, size);
    /* Failure here only lets the child inherit extra pages: tagged values
     * still use the parent serializer. Keep the extent registered so a
     * later DOFORK transition covers even partially advised ranges. */
    if (!forkless_nofork_forkable) (void)forklessApplyAdvice(addr, size, 0);
    pthread_mutex_unlock(&forkless_extent_mutex);
}

static void *forklessExtentAlloc(extent_hooks_t *extent_hooks, void *new_addr,
                                 size_t size, size_t alignment, bool *zero,
                                 bool *commit, unsigned arena_ind)
{
    void *p = forkless_default_hooks->alloc(forkless_default_hooks, new_addr,
                                            size, alignment, zero, commit,
                                            arena_ind);
    if (p) forklessExtentRegister(p, size);
    UNUSED(extent_hooks);
    return p;
}

/* Flip every extent of arena B. O(extents), not O(keys): MADV_DONTFORK is a
 * property of the mapping, so no object has to move. Safe across jemalloc
 * purging, which uses MADV_DONTNEED and leaves VM_DONTCOPY intact. */
static int forklessSetNoforkForkable(int forkable) {
    pthread_mutex_lock(&forkless_extent_mutex);
    if (forkable != forkless_nofork_forkable && forkless_extents_overflowed) {
        pthread_mutex_unlock(&forkless_extent_mutex);
        errno = ENOSPC;
        return C_ERR;
    }
    if (forkless_nofork_forkable != forkable) {
        for (size_t i = 0; i < forkless_extent_count; i++) {
            if (forklessApplyAdvice(forkless_extents[i].addr,
                                   forkless_extents[i].size, forkable) == C_OK)
                continue;
            int saved_errno = errno;
            /* madvise may change part of the failing range. Restore it too,
             * before CONFIG SET restores the old configuration value. */
            for (size_t j = i + 1; j > 0; j--) {
                if (forklessApplyAdvice(forkless_extents[j-1].addr,
                                       forkless_extents[j-1].size,
                                       forkless_nofork_forkable) != C_OK)
                    serverPanic("forkless: arena advice rollback failed; fork visibility is inconsistent");
            }
            pthread_mutex_unlock(&forkless_extent_mutex);
            errno = saved_errno;
            return C_ERR;
        }
        forkless_nofork_forkable = forkable;
    }
    pthread_mutex_unlock(&forkless_extent_mutex);
    return C_OK;
}

static int forklessInstallDontforkHooks(unsigned int arena) {
    extent_hooks_t *cur = NULL;
    size_t sz = sizeof(cur);
    char mib[64];

    snprintf(mib, sizeof(mib), "arena.%u.extent_hooks", 0);
    if (je_mallctl(mib, (void *)&cur, &sz, NULL, 0) != 0 || cur == NULL) {
        serverLog(LL_WARNING, "Unable to read default jemalloc extent hooks");
        return C_ERR;
    }
    forkless_default_hooks = cur;
    if (cur->alloc == NULL) {
        serverLog(LL_NOTICE, "jemalloc default extent alloc is implicit; skip DONTFORK hooks");
        return C_ERR;
    }
    forkless_hooks = *cur;
    forkless_hooks.alloc = forklessExtentAlloc;
    extent_hooks_t *hooks = &forkless_hooks;
    snprintf(mib, sizeof(mib), "arena.%u.extent_hooks", arena);
    sz = sizeof(hooks);
    if (je_mallctl(mib, NULL, NULL, (void *)&hooks, sz) != 0) {
        serverLog(LL_WARNING, "Unable to install forkless-arena extent hooks");
        return C_ERR;
    }
    return C_OK;
}
#else
/* Without jemalloc only hybrid is unavailable; full forkless needs no arenas. */
static int forklessSetNoforkForkable(int forkable) {
    UNUSED(forkable);
    return C_OK;
}
#endif

#if defined(USE_JEMALLOC)
static int forklessCreateArena(unsigned int *out) {
    unsigned int arena;
    size_t sz = sizeof(arena);
    if (je_mallctl("arenas.create", (void *)&arena, &sz, NULL, 0) != 0)
        return 0;
    *out = arena;
    return 1;
}
#endif

static int forklessEnsureArena(void) {
#if defined(USE_JEMALLOC)
    if (forkless_arena_nofork != UINT_MAX) return 1;
    if (!forklessCreateArena(&forkless_arena_fork) ||
        !forklessCreateArena(&forkless_arena_nofork))
    {
        serverLog(LL_WARNING, "Failed creating the forkless jemalloc arenas");
        forkless_arena_fork = UINT_MAX;
        forkless_arena_nofork = UINT_MAX;
        return 0;
    }
    /* Only B gets the tracking hooks; A behaves like any ordinary arena. */
    if (forklessInstallDontforkHooks(forkless_arena_nofork) != C_OK) {
        forkless_arena_fork = UINT_MAX;
        forkless_arena_nofork = UINT_MAX;
        return 0;
    }
    serverLog(LL_NOTICE,
              "Forkless jemalloc arenas created (forkable %u, flippable %u)",
              forkless_arena_fork, forkless_arena_nofork);
    return 1;
#else
    return 0;
#endif
}

int forklessInit(void) {
    forkless_arena_fork = UINT_MAX;
    forkless_arena_nofork = UINT_MAX;
    forkless_save_active = 0;
    forkless_rdb_tmp[0] = '\0';

    if (server.bgsave_mode != BGSAVE_MODE_FORK &&
        (server.aof_enabled || server.aof_state != AOF_OFF)) {
        serverLog(LL_WARNING,
                  "bgsave-mode %s cannot be used with appendonly yes",
                  bgsaveModeName());
        return C_ERR;
    }
    if (forklessEnsureArena()) {
#if defined(USE_JEMALLOC)
        if (server.forkless_arena_tcache && !zmallocArenaCacheInit(forkless_arena_nofork)) {
            serverLog(LL_WARNING, "Unable to initialize experimental forkless arena cache");
            return C_ERR;
        }
#endif
        /* Arm arena B now. From here the mode may be changed at runtime and
         * only flips visibility; placement decisions are permanent. */
        return forklessSetNoforkForkable(server.bgsave_mode == BGSAVE_MODE_FORK);
    }
    if (server.forkless_arena_tcache) {
        serverLog(LL_WARNING, "forkless-arena-tcache requires initialized jemalloc arenas");
        return C_ERR;
    }
    if (server.bgsave_mode == BGSAVE_MODE_FORK || server.bgsave_mode == BGSAVE_MODE_FORKLESS) return C_OK;
    serverLog(LL_WARNING, "bgsave-mode %s requires jemalloc",
              bgsaveModeName());
    return C_ERR;
}

/* Jemalloc builds create both arenas before loading the dataset, including
 * when starting in fork mode. Non-jemalloc builds have no dedicated arenas. */
int forklessArenasReady(void) {
    return forkless_arena_nofork != UINT_MAX;
}

int forklessSplitSaveActive(void) {
    return forkless_split_save;
}

int forklessHybridCleanupPending(void) {
    return forkless_cleanup_pending;
}

/* Apply a runtime bgsave-mode change. Only the visibility of arena B changes;
 * every object stays where it was allocated. */
int forklessApplyModeChange(int newmode, const char **err) {
    if (forkless_save_active || hasActiveChildProcess()) {
        *err = "bgsave-mode cannot be changed during a background save.";
        return C_ERR;
    }
    if (!forklessArenasReady()) {
        if (newmode == BGSAVE_MODE_FORK || newmode == BGSAVE_MODE_FORKLESS) {
            forkless_marks_valid = 0;
            return C_OK;
        }
        *err = "Dedicated snapshot arenas are unavailable in this build.";
        return C_ERR;
    }
    if (forklessSetNoforkForkable(newmode == BGSAVE_MODE_FORK) != C_OK) {
        *err = errno == ENOSPC ?
               "bgsave-mode cannot change: the forkless extent registry overflowed." :
               "bgsave-mode cannot change: arena advice failed; previous visibility restored.";
        return C_ERR;
    }
    forkless_marks_valid = 0;
    return C_OK;
}

const char *bgsaveModeName(void) {
    if (server.bgsave_mode == BGSAVE_MODE_FORKLESS) return "forkless";
    if (server.bgsave_mode == BGSAVE_MODE_HYBRID) return "hybrid";
    return "fork";
}

int forklessEnabled(void) {
    return (server.bgsave_mode == BGSAVE_MODE_FORKLESS ||
            server.bgsave_mode == BGSAVE_MODE_HYBRID) &&
           forklessArenasReady();
}

unsigned int forklessArenaIndex(void) {
    return forkless_arena_nofork;
}

unsigned int forklessForkArenaIndex(void) {
    return forkless_arena_fork;
}

int forklessNoforkIsForkable(void) {
    return forkless_nofork_forkable;
}

int forklessNonRdbForkAllowed(void) {
#if defined(USE_JEMALLOC) && defined(__linux__)
    /* Module and Lua-debugger children dereference the live keyspace without
     * the RDB child's split serializer protocol. Check applied visibility,
     * not the config value, which CONFIG SET changes before its callbacks. */
    return !forklessArenasReady() || forklessNoforkIsForkable();
#else
    return 1;
#endif
}

const char *forklessChildRdbTmpfile(void) {
    return forkless_rdb_tmp[0] ? forkless_rdb_tmp : NULL;
}

/* ---------- Values that live outside the child ---------- */

/* Should this value be placed in the flippable arena? Placement is permanent,
 * so `fork` and `forkless` deliberately share the widest rule: switching
 * between them is then a pure page flip with no object left behind. Hybrid is
 * the only mode that narrows placement, which is why hybrid conversions
 * converge lazily as keys are rewritten rather than instantly. */
static int forklessValueIsEligible(const robj *val) {
    if (!forklessArenasReady() || val == NULL) return 0;

    /* Module values keep classic handling: their RDB callbacks are not
     * guaranteed to be safe on the serializer thread. */
    if (val->type == OBJ_MODULE) return 0;

    if (server.bgsave_mode != BGSAVE_MODE_HYBRID) return 1;

    /* Strings are self-contained after routing their SDS. Hybrid keeps large
     * strings forkable so a write cannot stall on a huge background encode. */
    if (val->type == OBJ_STRING) {
        if (val->encoding == OBJ_ENCODING_INT) return 1;
        if (!sdsEncodedObject(val)) return 0;
        return sdslen(val->ptr) <= server.forkless_string_max;
    }

    /* Listpack hashes are a blob plus, for listpack-ex, a small header that
     * holds field-TTL metadata. */
    return val->type == OBJ_HASH &&
           (val->encoding == OBJ_ENCODING_LISTPACK ||
            val->encoding == OBJ_ENCODING_LISTPACK_EX);
}

/* Ordinary mutation may pin only immutable encodings. RAW strings need a copy
 * before APPEND/SETRANGE; unlink-only capture can retain them until needed. */
static int forklessPinObjectEligible(const kvobj *kv) {
    if (!kv || getModuleMetaBits(kvobjMetaBits(kv))) return 0;
    return kv->type == OBJ_STRING && kv->encoding != OBJ_ENCODING_RAW;
}

/* Automatic tcaches mix arenas. Only the opt-in, dedicated main-thread cache
 * may be used here; other arenas and threads still bypass caching. */
static int forklessAllocEnterArena(unsigned int arena) {
#if defined(USE_JEMALLOC)
    int prev = zmalloc_get_thread_flags();
    if (arena != UINT_MAX)
        zmalloc_set_thread_flags(zmallocArenaFlags(arena));
    return prev;
#else
    UNUSED(arena);
    return 0;
#endif
}

#if defined(USE_JEMALLOC)
static int forklessAllocEnter(void) {
    return forklessAllocEnterArena(forklessArenasReady() ?
                                   forkless_arena_nofork : UINT_MAX);
}
#endif

static void forklessAllocLeave(int prev) {
#if defined(USE_JEMALLOC)
    zmalloc_set_thread_flags(prev);
#else
    UNUSED(prev);
#endif
}

static int forklessPayloadNeedsDup(const robj *val) {
    if (!val || val->ptr == NULL) return 0;
    if (val->type == OBJ_STRING && val->encoding == OBJ_ENCODING_RAW)
        return !forklessPtrInArena(sdsAllocPtr(val->ptr));
    if (val->type == OBJ_HASH && val->encoding == OBJ_ENCODING_LISTPACK)
        return !forklessPtrInArena(val->ptr);
    if (val->type == OBJ_HASH && val->encoding == OBJ_ENCODING_LISTPACK_EX) {
        listpackEx *lpt = val->ptr;
        return !forklessPtrInArena(lpt) ||
               (lpt->lp && !forklessPtrInArena(lpt->lp));
    }
    return 0;
}

static void forklessDupPayloadInPlace(robj *val) {
    if (val->refcount != 1 || val->ptr == NULL) return;
    if (val->type == OBJ_STRING && val->encoding == OBJ_ENCODING_RAW) {
        sds copy = sdsdup(val->ptr);
        sdsfree(val->ptr);
        val->ptr = copy;
    } else if (val->type == OBJ_HASH &&
               val->encoding == OBJ_ENCODING_LISTPACK)
    {
        unsigned char *copy = lpDup(val->ptr);
        lpFree(val->ptr);
        val->ptr = copy;
    } else if (val->type == OBJ_HASH &&
               val->encoding == OBJ_ENCODING_LISTPACK_EX)
    {
        /* ExpireMeta lives in this header. Callers must detach HFE first. */
        listpackEx *old = val->ptr;
        listpackEx *copy = zcalloc(sizeof(*copy));
        copy->meta.trash = 1;
        copy->lp = old->lp ? lpDup(old->lp) : NULL;
        if (old->lp) lpFree(old->lp);
        zfree(old);
        val->ptr = copy;
    }
}

#if defined(USE_JEMALLOC)
static kvobj *forklessMoveKvobjToArena(kvobj *kv) {
    if (!forklessArenasReady() || kv == NULL) return kv;
    void *old = kvobjGetAllocPtr(kv);
    if (!forklessPtrInArena(old)) {
        size_t sz = zmalloc_usable_size(old);
        void *n = zmalloc_with_flags(sz, zmallocArenaFlags(forkless_arena_nofork));
        memcpy(n, old, sz);
        kvobj *nkv = (kvobj *)((char *)n + ((char *)kv - (char *)old));
        if (nkv->encoding == OBJ_ENCODING_EMBSTR && nkv->ptr)
            nkv->ptr = (char *)nkv->ptr + ((char *)nkv - (char *)kv);
        zfree(old);
        kv = nkv;
    }
    int prev = forklessAllocEnter();
    if (forklessPayloadNeedsDup(kv))
        forklessDupPayloadInPlace(kv);
    forklessAllocLeave(prev);
    return kv;
}
#else
static kvobj *forklessMoveKvobjToArena(kvobj *kv) {
    return kv;
}
#endif

kvobj *forklessCreateKvobj(sds key, robj *val, uint32_t metabits) {
    if (getModuleMetaBits(metabits) || !forklessArenasReady()) {
        kvobj *kv = kvobjSet(key, val, metabits);
        kvobjSetSnapshot(kv, forkless_round);
        return kv;
    }

    int nofork = forklessValueIsEligible(val);
    int prev = forklessAllocEnterArena(nofork ? forkless_arena_nofork :
                                                forkless_arena_fork);
    /* Only the flippable arena needs its payload relocated: kvobjSet reuses
     * val->ptr, which the command allocated elsewhere, and the child must not
     * be able to reach it. Values bound for the forkable arena keep their
     * payload where it is, so routing them costs nothing extra. */
    if (nofork) forklessDupPayloadInPlace(val);
    kvobj *kv = kvobjSet(key, val, metabits);
    kvobjSetSnapshot(kv, forkless_round);
    forklessAllocLeave(prev);
    return kv;
}

kvobj *forklessMoveKvobjToDefault(kvobj *kv) {
#if defined(USE_JEMALLOC)
    if (!kv || !forklessPtrInArena(kvobjGetAllocPtr(kv))) return kv;
    long long expire = kvobjGetExpire(kv);
    /* Out of the flippable arena and into the one the child always inherits. */
    int prev = forklessAllocEnterArena(forkless_arena_fork);
    forklessDupPayloadInPlace(kv);
    kv = kvobjSet(kvobjGetKey(kv), kv, kvobjMetaBits(kv));
    if (expire != -1) kv = kvobjSetExpire(kv, expire);
    zmalloc_set_thread_flags(prev);
#endif
    return kv;
}

void forklessRetagLink(dictEntryLink link, kvobj *kv) {
    if (!link || !kv) return;
    if (!getModuleMetaBits(kvobjMetaBits(kv)) &&
        forklessValueIsEligible(kv) &&
        forklessPtrInArena(kvobjGetAllocPtr(kv)))
        dictSetEntryForkless(link, 1);
    else
        dictSetEntryForkless(link, 0);
}

int forklessMigrateIfNeeded(redisDb *db, robj *key, kvobj **kvp, dictEntryLink link) {
    kvobj *kv = kvp ? *kvp : NULL;
    if (!forklessEnabled() || !link || !kv || !key) return 0;
    if (server.bgsave_mode != BGSAVE_MODE_HYBRID) return 0;
    if (kv->type != OBJ_STRING || !sdsEncodedObject(kv)) return 0;
    if (sdslen(kv->ptr) <= server.forkless_string_max) return 0;
    if (!dictEntryIsForkless(*link) && !forklessPtrInArena(kvobjGetAllocPtr(kv)))
        return 0;

    forklessCaptureFromLink(db, link);
    int slot = getKeySlot(key->ptr);
    long long expire = kvobjGetExpire(kv);
    uint32_t bits = kvobjMetaBits(kv);
    if (expire == -1)
        bits &= ~KEY_META_MASK_EXPIRE;
    robj *raw = createRawStringObject(kv->ptr, sdslen(kv->ptr));
    kvobj *nkv = kvobjSet(key->ptr, raw, bits);
    nkv->lru = kv->lru;
    if (expire != -1)
        kvobjSetExpire(nkv, expire);
    kvstoreDictSetAtLink(db->keys, slot, nkv, &link, 0);
    if (expire != -1) {
        dictEntryLink ex = kvstoreDictFindLink(db->expires, slot, key->ptr, NULL);
        if (ex) kvstoreDictSetAtLink(db->expires, slot, nkv, &ex, 0);
    }
    decrRefCount(kv);
    dictSetEntryForkless(link, 0);
    *kvp = nkv;
    return 1;
}

/* Complete value routing after dbSetValue has either allocated a new kvobj or
 * reused the old one. This keeps arena moves and dict retagging out of db.c. */
void forklessFinalizeDbValue(redisDb *db, robj *key, kvobj **kvp,
                             dictEntryLink link)
{
    kvobj *kv = *kvp;
    int slot = getKeySlot(key->ptr);
    int route = !getModuleMetaBits(kvobjMetaBits(kv)) &&
                forklessValueIsEligible(kv);
    int touch = route ?
                (!forklessPtrInArena(kvobjGetAllocPtr(kv)) ||
                 forklessPayloadNeedsDup(kv)) :
                forklessPtrInArena(kvobjGetAllocPtr(kv));
    uint64_t subexpiry = EB_EXPIRE_TIME_INVALID;
    if (touch && kv->type == OBJ_HASH)
        subexpiry = estoreRemove(db->subexpires, slot, kv);

    kvobj *moved = kv;
    if (route) {
        moved = forklessMoveKvobjToArena(kv);
    } else if (touch) {
        moved = forklessMoveKvobjToDefault(kv);
    }

    if (moved != kv) {
        kvstoreDictSetAtLink(db->keys, slot, moved, &link, 0);
        long long expire = kvobjGetExpire(moved);
        if (expire != -1) {
            dictEntryLink ex = kvstoreDictFindLink(db->expires, slot,
                                                   key->ptr, NULL);
            if (ex) kvstoreDictSetAtLink(db->expires, slot, moved, &ex, 0);
        }
        kv = moved;
    }
    forklessMigrateIfNeeded(db, key, &kv, link);
    forklessRetagLink(link, kv);
    /* This is a new/post-mutation value. Its old version was captured before
     * the mutation, if needed. Between saves use the last completed round. */
    kvobjSetSnapshot(kv, forkless_round);
    if (subexpiry != EB_EXPIRE_TIME_INVALID)
        estoreAdd(db->subexpires, slot, kv, subexpiry);
    *kvp = kv;
}

/* Hash commands mutate the value in place, including listpack-to-hashtable
 * conversions. Re-evaluate ownership after the command has chosen its final
 * encoding, and return the possibly relocated kvobj. */
kvobj *forklessFinalizeHashValue(redisDb *db, robj *key, kvobj *kv) {
    int slot = getKeySlot(key->ptr);
    dictEntryLink link = kvstoreDictFindLink(db->keys, slot, key->ptr, NULL);
    if (!link || dictGetKV(*link) != kv) return kv;
    forklessFinalizeDbValue(db, key, &kv, link);
    return kv;
}

static int forklessPtrInArena(void *ptr) {
    return forkless_arena_nofork != UINT_MAX &&
           zmallocPtrArenaIndex(ptr) == (int)forkless_arena_nofork;
}

/* ---------- Fork-time values changed by the parent ---------- */

static inline uint64_t forklessPtrHash(const void *p) {
    uint64_t x = (uint64_t)(uintptr_t)p;
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;
    return x;
}

/* Pin helpers — caller must hold forkless_mutex. A pin pointer must not
 * survive unlocking: another mutation may grow that pin's shard. */
static forklessPin *forklessPinFindLocked(kvobj *kv) {
    if (!forkless_pins || !kv) return NULL;
    uint64_t hash = forklessPtrHash(kv);
    forklessPinShard *shard = &forkless_pins->shards[hash & (FORKLESS_PIN_SHARDS - 1)];
    if (!shard->cap) return NULL;
    size_t mask = shard->cap - 1;
    size_t slot = (hash >> FORKLESS_PIN_SHARD_BITS) & mask;
    while (1) {
        forklessPin *p = &shard->slots[slot];
        if (p->kv == NULL) return NULL;
        if (p->kv == kv) return p;
        slot = (slot + 1) & mask;
    }
}

/* Main thread only. Growth is local to one shard, not the whole snapshot. */
static int forklessPinGrowLocked(forklessPinShard *shard) {
    mstime_t latency;
    latencyStartMonitor(latency);
    size_t newcap = shard->cap ? shard->cap * 2 : 16;
    /* Only main grows tables. The helper may delete entries while allocation
     * runs; rehash the latest contents after reacquiring the lock. */
    pthread_mutex_unlock(&forkless_mutex);
    forklessPin *slots = forklessTrackingAlloc(newcap * sizeof(*slots));
    pthread_mutex_lock(&forkless_mutex);
    if (!slots) return C_ERR;
    size_t mask = newcap - 1;
    for (size_t i = 0; i < shard->cap; i++) {
        if (!shard->slots[i].kv) continue;
        size_t slot = (forklessPtrHash(shard->slots[i].kv) >>
                       FORKLESS_PIN_SHARD_BITS) & mask;
        while (slots[slot].kv) slot = (slot + 1) & mask;
        slots[slot] = shard->slots[i];
    }
    forklessTrackingFree(shard->slots, shard->cap * sizeof(*slots));
    forkless_pins->slot_bytes += (newcap - shard->cap) * sizeof(*slots);
    shard->slots = slots;
    shard->cap = newcap;
    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("forkless-pin-grow", latency);
    return C_OK;
}

static forklessPin *forklessPinGetOrAddLocked(kvobj *kv) {
    forklessPin *existing = forklessPinFindLocked(kv);
    if (existing) return existing;
    if (!forkless_pins) {
        forkless_pins = forklessTrackingAlloc(sizeof(*forkless_pins));
        if (!forkless_pins) return NULL;
    }
    uint64_t hash = forklessPtrHash(kv);
    forklessPinShard *shard = &forkless_pins->shards[hash & (FORKLESS_PIN_SHARDS - 1)];
    if (shard->count + 1 > shard->cap * 3 / 4 &&
        forklessPinGrowLocked(shard) != C_OK) return NULL;
    size_t mask = shard->cap - 1;
    size_t slot = (hash >> FORKLESS_PIN_SHARD_BITS) & mask;
    while (shard->slots[slot].kv) slot = (slot + 1) & mask;
    shard->slots[slot].kv = kv;
    shard->count++;
    forkless_pins->count++;
    return &shard->slots[slot];
}

/* A work identity occurs once in the child's frozen dict. After consuming it
 * and any queued early reference,
 * normal-round marks prevent recapture of the live/new object. Recovery keeps
 * tombstones because it intentionally ignores those marks. No dangling object
 * dereference is needed to retire an identity. */
static void forklessPinRemoveWrittenLocked(kvobj *kv) {
    if (!forkless_marks_valid) return;
    forklessPin *pin = forklessPinFindLocked(kv);
    if (!pin || pin->state != FORKLESS_PIN_WRITTEN ||
        pin->early_pending || pin->awaiting_child) return;
    serverAssert(!pin->held && !pin->encoded);
    uint64_t hash = forklessPtrHash(kv);
    forklessPinShard *shard = &forkless_pins->shards[hash & (FORKLESS_PIN_SHARDS - 1)];
    size_t mask = shard->cap - 1;
    size_t hole = pin - shard->slots;
    for (size_t next = (hole + 1) & mask; shard->slots[next].kv; next = (next + 1) & mask) {
        size_t home = (forklessPtrHash(shard->slots[next].kv) >> FORKLESS_PIN_SHARD_BITS) & mask;
        if (((next - home) & mask) >= ((next - hole) & mask)) {
            shard->slots[hole] = shard->slots[next];
            hole = next;
        }
    }
    memset(&shard->slots[hole], 0, sizeof(*pin));
    shard->count--;
    forkless_pins->count--;
    forkless_removed_pins++;
    /* Keep capacity until cleanup: reallocating an empty shard on each key
     * would exchange table memory savings for allocator/lock churn. */
}

/* Main only, before stamping a captured object with this round. Recovering
 * saves cannot prove membership from marks and must await the child's record.
 * Follow dictionary identity rather than db->id across SWAPDB. */
static int forklessEarlyDb(redisDb *db, kvobj *kv) {
    if (!db || !forkless_snapshot_dbs || !forkless_marks_valid ||
        kvobjSnapshot(kv) == forkless_round) return -1;
    if (server.cluster_enabled) {
        int slot = getKeySlot(kvobjGetKey(kv));
        if (forkless_snapshot_trimmed[slot / 8] & (1 << (slot % 8))) return -1;
    }
    if (db->id < forkless_snapshot_dbnum && forkless_snapshot_dbs[db->id] == db->keys)
        return db->id;
    for (int i = 0; i < forkless_snapshot_dbnum; i++)
        if (forkless_snapshot_dbs[i] == db->keys) return i;
    return -1;
}

/* Caller holds forkless_mutex. A full queue falls back to child-driven work;
 * it never allocates or blocks a client. A wake byte only signals empty->nonempty. */
static void forklessQueueEarlyLocked(forklessPin *pin, int dbid) {
    if (dbid < 0 || forkless_early_wake[1] == -1 ||
        forkless_early_count == FORKLESS_WORK_BATCH_SIZE) return;
    size_t tail = (forkless_early_head + forkless_early_count) % FORKLESS_WORK_BATCH_SIZE;
    forkless_early[tail] = (forklessWorkRec){pin->kv, dbid};
    pin->early_pending = pin->awaiting_child = 1;
    if (forkless_early_count++ == 0) {
        ssize_t n;
        do { n = write(forkless_early_wake[1], "w", 1); } while (n == -1 && errno == EINTR);
        /* EAGAIN means a previous wake is already readable. */
        if (n == -1 && errno != EAGAIN && errno != EWOULDBLOCK)
            atomicSet(forkless_serializer_error, 1);
    }
}

static int forklessTakeEarlyLocked(forklessWorkRec *work) {
    while (forkless_early_count) {
        *work = forkless_early[forkless_early_head];
        forkless_early_head = (forkless_early_head + 1) % FORKLESS_WORK_BATCH_SIZE;
        forkless_early_count--;
        forklessPin *pin = forklessPinFindLocked(work->kv);
        serverAssert(pin && pin->early_pending);
        pin->early_pending = 0;
        if (pin->state == FORKLESS_PIN_WRITTEN) {
            /* Child-driven work overtook this queue entry. */
            forklessPinRemoveWrittenLocked(work->kv);
            continue;
        }
        return 1;
    }
    return 0;
}

/* O(1) expected lookup, with no allocation on the eviction path. A window is
 * never refilled until drained, so priority traffic cannot starve its tail. */
static void forklessPrioritizeReadyLocked(kvobj *kv) {
    size_t mask = FORKLESS_WORK_BATCH_SIZE * 2 - 1;
    size_t slot = forklessPtrHash(kv) & mask;
    while (forkless_ready_index[slot]) {
        unsigned int index = forkless_ready_index[slot];
        forklessReadyKey *ready = &forkless_ready[index - 1];
        if (ready->work.kv == kv) {
            if (!ready->pending || ready->prioritized) return;
            ready->prioritized = 1;
            if (forkless_priority_tail)
                forkless_ready[forkless_priority_tail - 1].next_priority = index;
            else
                forkless_priority_head = index;
            forkless_priority_tail = index;
            return;
        }
        slot = (slot + 1) & mask;
    }
}

/* Called after capture, before eviction unlinks the live object. Never encode
 * or wait for output here. Pins outside the window stay on the normal path. */
void forklessPrioritizeEviction(kvobj *kv) {
    if (!forkless_save_active || !forklessCaptureEnabled()) return;
    pthread_mutex_lock(&forkless_mutex);
    forklessPin *pin = forklessPinFindLocked(kv);
    if (pin && pin->state != FORKLESS_PIN_WRITTEN) {
        pin->evicted = 1;
        forklessPrioritizeReadyLocked(kv);
    }
    pthread_mutex_unlock(&forkless_mutex);
}

unsigned long long forklessPriorityKeys(void) {
    pthread_mutex_lock(&forkless_mutex);
    unsigned long long count = forkless_priority_keys;
    pthread_mutex_unlock(&forkless_mutex);
    return count;
}

static void forklessPublishReadyLocked(forklessWorkRec *batch, size_t count) {
    memset(forkless_ready, 0, sizeof(forkless_ready));
    memset(forkless_ready_index, 0, sizeof(forkless_ready_index));
    forkless_ready_count = count;
    forkless_ready_remaining = 0;
    forkless_ready_cursor = 0;
    forkless_priority_head = forkless_priority_tail = 0;
    for (size_t i = 0; i < count; i++) {
        forkless_ready[i].work = batch[i];
        if (!batch[i].kv) continue;
        forkless_ready_remaining++;
        forkless_ready[i].pending = 1;
        size_t mask = FORKLESS_WORK_BATCH_SIZE * 2 - 1;
        size_t slot = forklessPtrHash(batch[i].kv) & mask;
        while (forkless_ready_index[slot]) slot = (slot + 1) & mask;
        forkless_ready_index[slot] = i + 1;
        forklessPin *pin = forklessPinFindLocked(batch[i].kv);
        if (pin && pin->evicted && pin->state != FORKLESS_PIN_WRITTEN)
            forklessPrioritizeReadyLocked(batch[i].kv);
    }
}

static int forklessTakeReadyLocked(forklessWorkRec *work) {
    size_t index;
    if (forkless_priority_head) {
        index = forkless_priority_head - 1;
        forkless_priority_head = forkless_ready[index].next_priority;
        if (!forkless_priority_head) forkless_priority_tail = 0;
        forkless_priority_keys++;
    } else {
        while (forkless_ready_cursor < forkless_ready_count &&
               !forkless_ready[forkless_ready_cursor].pending)
            forkless_ready_cursor++;
        if (forkless_ready_cursor == forkless_ready_count) return 0;
        index = forkless_ready_cursor++;
    }
    forkless_ready[index].pending = 0;
    forkless_ready_remaining--;
    *work = forkless_ready[index].work;
    return 1;
}

/* Caller already holds the read-guard mutex: scheduling adds no lock or
 * allocation. Empty sources donate their turn immediately. */
static int forklessTakeScheduledLocked(forklessWorkRec *work, int *from_child) {
    unsigned int quantum = FORKLESS_SCHEDULE_QUANTUM;
    if (forkless_schedule_early && forkless_early_count >= FORKLESS_WORK_BATCH_SIZE * 3 / 4)
        quantum *= 2;
    int early = forkless_schedule_early;
    if (forkless_schedule_run >= quantum) early = !early;
    int found = early ? forklessTakeEarlyLocked(work) : forklessTakeReadyLocked(work);
    if (!found) {
        early = !early;
        found = early ? forklessTakeEarlyLocked(work) : forklessTakeReadyLocked(work);
    }
    if (!found) return 0;
    if (early != forkless_schedule_early) forkless_schedule_run = 0;
    forkless_schedule_early = early;
    if (forkless_schedule_run < FORKLESS_SCHEDULE_QUANTUM * 2) forkless_schedule_run++;
    *from_child = !early;
    return 1;
}

/* No worker may be running here. Detach in O(1); reference decrements and
 * table scans happen in bounded main-loop batches, even after save completion. */
static void forklessRetirePins(void) {
    if (forkless_pins) {
        forkless_pins->next = forkless_retired_pins;
        forkless_retired_pins = forkless_pins;
        forkless_pins = NULL;
    }
    forkless_serializer_kv = NULL;
    forkless_capture_kv = NULL;
    forkless_early_head = forkless_early_count = 0;
    forkless_schedule_early = 0;
    forkless_schedule_run = 0;
    forklessPublishReadyLocked(NULL, 0);
}

/* Chunked queue: no database-sized realloc/memmove while holding the mutex. */
static int forklessQueueDoneLocked(kvobj *kv, size_t bytes) {
    serverAssert(forkless_pins != NULL);
    forklessDoneChunk *tail = forkless_pins->done_tail;
    if (!tail || tail->count == FORKLESS_DONE_CHUNK_SIZE) {
        forklessDoneChunk *chunk = forklessTrackingAlloc(sizeof(*chunk));
        if (!chunk) return C_ERR;
        if (tail) tail->next = chunk;
        else forkless_pins->done_head = chunk;
        forkless_pins->done_tail = chunk;
        tail = chunk;
    }
    tail->bytes[tail->count] = bytes;
    tail->values[tail->count++] = kv;
    return C_OK;
}

static void forklessReleaseSnapshot(kvobj *kv) {
    /* Snapshot objects have no module metadata/callbacks. Hand expensive final
     * destruction to lazyfree; shared references are decremented on main. */
    serverAssert(kv->type != OBJ_MODULE && !getModuleMetaBits(kvobjMetaBits(kv)));
    robj key;
    initStaticStringObject(key, kvobjGetKey(kv));
    freeObjAsync(&key, kv, 0);
}

int forklessReleaseDonePins(void) {
    if (!forkless_save_active && !forkless_retired_pins && !forkless_releasing) return 0;
    /* BIO may own the DB's reference to these same strings after async FLUSH
     * or slot trimming. Refcounts are not atomic: keep our references until
     * its dictionary destruction finishes. Main alone schedules those jobs. */
    if (lazyfreeHasPendingDbJobs()) return 0;
    if (forkless_cleanup_used >= FORKLESS_CLEANUP_USEC ||
        forkless_cleanup_slots >= FORKLESS_CLEANUP_SLOTS)
        return forkless_retired_pins != NULL || forkless_releasing != NULL;
    mstime_t latency;
    latencyStartMonitor(latency);
    monotime started = getMonotonicUs();
    monotime deadline = started + FORKLESS_CLEANUP_USEC - forkless_cleanup_used;
    size_t available = FORKLESS_CLEANUP_SLOTS - forkless_cleanup_slots;
    size_t remaining = available;

    /* Active references first. Object destruction stays on the main thread,
     * outside the lock. Check the budget after each object, not each chunk. */
    while (remaining) {
        if (!forkless_releasing) {
            pthread_mutex_lock(&forkless_mutex);
            forkless_releasing = forkless_pins ? forkless_pins->done_head : NULL;
            if (forkless_releasing) {
                forkless_pins->done_head = forkless_releasing->next;
                if (!forkless_releasing->next) forkless_pins->done_tail = NULL;
            }
            pthread_mutex_unlock(&forkless_mutex);
        }
        if (!forkless_releasing) break;
        size_t index = forkless_releasing->released++;
        forklessReleaseSnapshot(forkless_releasing->values[index]);
        forklessReleaseRetained(forkless_releasing->bytes[index]);
        if (forkless_releasing->released == forkless_releasing->count) {
            forklessTrackingFree(forkless_releasing, sizeof(*forkless_releasing));
            forkless_releasing = NULL;
        }
        remaining--;
        if (getMonotonicUs() >= deadline) goto done;
    }

    while (forkless_retired_pins && remaining) {
        forklessPinTable *table = forkless_retired_pins;
        if (table->done_head) {
            forklessDoneChunk *chunk = table->done_head;
            size_t index = chunk->released++;
            forklessReleaseSnapshot(chunk->values[index]);
            forklessReleaseRetained(chunk->bytes[index]);
            if (chunk->released == chunk->count) {
                table->done_head = chunk->next;
                forklessTrackingFree(chunk, sizeof(*chunk));
            }
            remaining--;
        } else if (table->cleanup_shard == FORKLESS_PIN_SHARDS) {
            forkless_retired_pins = table->next;
            forklessTrackingFree(table, sizeof(*table));
            remaining--;
        } else {
            forklessPinShard *shard = &table->shards[table->cleanup_shard];
            if (table->cleanup_slot == shard->cap) {
                forklessTrackingFree(shard->slots, shard->cap * sizeof(*shard->slots));
                table->cleanup_shard++;
                table->cleanup_slot = 0;
            } else {
                forklessPin *pin = &shard->slots[table->cleanup_slot++];
                forklessBufferFree(pin->encoded);
                if (pin->held) forklessReleaseSnapshot(pin->snapshot);
                forklessReleaseRetained(pin->accounted_bytes);
            }
            remaining--;
        }
        /* Soft budget: a single object destruction can still exceed it. */
        if (getMonotonicUs() >= deadline) break;
    }
done:
    forkless_cleanup_used += getMonotonicUs() - started;
    forkless_cleanup_slots += available - remaining;
    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("forkless-release", latency);
    return forkless_retired_pins != NULL || forkless_releasing != NULL;
}

static sds forklessCaptureBytes(kvobj *kv, int dbid) {
    /* Module metadata can make RDB encoding depend on dbid, so such values
     * never enter the forkless arena. The child work record remains the
     * authority for the SELECTDB emitted by the serializer after SWAPDB. */
    serverAssert(!getModuleMetaBits(kvobjMetaBits(kv)));
    rio rdb;
    if (forklessInitBuffer(&rdb) != C_OK) return NULL;
    robj keyobj;
    initStaticStringObject(keyobj, kvobjGetKey(kv));
    if (rdbSaveKeyValuePair(&rdb, &keyobj, kv, kvobjGetExpire(kv), dbid) == -1) {
        forklessBufferFree(rdb.io.buffer.ptr);
        return NULL;
    }
    return rdb.io.buffer.ptr;
}

static int forklessCaptureEnabled(void) {
    int enabled;
    atomicGet(forkless_capture_enabled, enabled);
    return enabled;
}

/* COPY's built-in duplication routines run on the main thread. The worker
 * owns only serialization; object destruction also stays on the main thread. */
static kvobj *forklessCopyValue(kvobj *kv) {
    robj *copy = NULL;
    uint64_t minexpire = EB_EXPIRE_TIME_INVALID;
    switch (kv->type) {
    case OBJ_STRING: copy = dupStringObject(kv); break;
    case OBJ_LIST: copy = listTypeDup(kv); break;
    case OBJ_SET: copy = setTypeDup(kv); break;
    case OBJ_ZSET: copy = zsetDup(kv); break;
    case OBJ_HASH: copy = hashTypeDup(kv, &minexpire); break;
    case OBJ_STREAM: copy = streamDup(kv); break;
#ifdef ENABLE_GCRA
    case OBJ_GCRA: copy = gcraDup(kv); break;
#endif
    case OBJ_ARRAY: copy = arrayTypeDup(kv); break;
    default: return NULL;
    }
    /* The lru field holds either LRU recency or packed LFU time/frequency.
     * Duplication creates fresh metadata; preserve the source's history.
     * kvobjSet below transfers this field to the snapshot object. */
    copy->lru = kv->lru;
    long long expire = kvobjGetExpire(kv);
    kvobj *snapshot = kvobjSet(kvobjGetKey(kv), copy,
                              expire == -1 ? 0 : KEY_META_MASK_EXPIRE);
    if (expire != -1) snapshot = kvobjSetExpire(snapshot, expire);
    return snapshot;
}

/* Main holds forkless_mutex. Stop the worker before allowing a mutation whose
 * old value could not be preserved. Never leave an unprotected work pointer. */
static void forklessCaptureAbortLocked(void) {
    atomicSet(forkless_serializer_error, 1);
    forkless_capture_kv = NULL;
    pthread_cond_broadcast(&forkless_read_done);
    pthread_mutex_unlock(&forkless_mutex);
    forkless_limit_aborts++;
    forklessAbortBeforeFlush();
    server.lastbgsave_status = C_ERR;
    server.stat_rdb_consecutive_failures++;
}

static void forklessCaptureKvInternal(kvobj *kv, int copy_metadata, int retain_string, redisDb *db) {
    /* Main owns refcounts. A retained original has an extra reference and
     * must still take the lock: its current mark does not permit mutation.
     * With a single reference, a trusted current mark means either a private
     * snapshot exists, this is a new value, or the helper has finished reading
     * it. The acquire load pairs with the helper's release publication.
     * Recovery must always inspect pins, regardless of the object's mark. */
    if (forkless_marks_valid && kvobjSnapshot(kv) == forkless_round &&
        kv->refcount == 1) {
        forkless_capture_skipped++;
        return;
    }
    monotime waiting_since = 0;
    for (;;) {
        pthread_mutex_lock(&forkless_mutex);
        while (forkless_serializer_kv == kv) {
            if (!waiting_since) waiting_since = getMonotonicUs();
            pthread_cond_wait(&forkless_read_done, &forkless_mutex);
        }
        if (waiting_since)
            forkless_reader_wait_usec += getMonotonicUs() - waiting_since;

        forklessPin *pin = forklessPinFindLocked(kv);
        /* Unlink can retain a RAW string. A later in-place write (or
         * a module unlink callback) must detach it before changing its bytes. */
        int detach = pin && pin->state == FORKLESS_PIN_OBJECT &&
                     pin->snapshot == kv &&
                     (copy_metadata || (!retain_string && !forklessPinObjectEligible(kv)));
        if (pin && !detach) {
            /* Already captured for this fork-time object. */
            pthread_mutex_unlock(&forkless_mutex);
            return;
        }
        /* Check pins first: a retained original may still need detaching
         * before an in-place mutation, even though its mark is current. */
        if (!pin && forkless_marks_valid && kvobjSnapshot(kv) == forkless_round) {
            forkless_capture_skipped++;
            pthread_mutex_unlock(&forkless_mutex);
            return;
        }
        int early_dbid = pin ? -1 : forklessEarlyDb(db, kv);

        /* Includes retired generations until main releases their references.
         * Admission uses the existing O(1) value allocation estimate. This is
         * not a process-RSS cap: output buffers, allocator slack and kernel
         * CoW are intentionally outside this preserved-value budget. */
        size_t bytes = kvobjAllocSize(kv);
        size_t retained;
        atomicGet(forkless_retained_bytes, retained);
        size_t limit = server.forkless_retained_max_memory;
        if ((limit && (bytes > limit || retained > limit - bytes)) ||
            !forklessMemoryReserve(bytes)) {
            serverLog(LL_WARNING, "forkless: snapshot admission limit reached; aborting snapshot");
            forklessCaptureAbortLocked();
            return;
        }
        atomicIncr(forkless_retained_bytes, bytes);
        if (retained + bytes > forkless_retained_peak)
            forkless_retained_peak = retained + bytes;

        if (!copy_metadata && (forklessPinObjectEligible(kv) ||
            (retain_string && kv->type == OBJ_STRING && !getModuleMetaBits(kvobjMetaBits(kv))))) {
            /* Shard growth temporarily unlocks for allocation. Keep this
             * object protected until its retained reference is published. */
            forkless_capture_kv = kv;
            pin = forklessPinGetOrAddLocked(kv);
            if (!pin) {
                forklessReleaseRetained(bytes);
                forklessCaptureAbortLocked();
                return;
            }
            pin->expire = kvobjGetExpire(kv);
            pin->held = 1;
            pin->encoded = NULL;
            pin->state = FORKLESS_PIN_OBJECT;
            pin->snapshot = kv;
            pin->accounted_bytes = bytes;
            pin->retained_key = kvobjGetKey(kv);
            pin->retained_encoding = kv->encoding;
            incrRefCount(kv);
            kvobjSetSnapshot(kv, forkless_round);
            forklessQueueEarlyLocked(pin, early_dbid);
            forkless_retained_strings++;
            forkless_capture_kv = NULL;
            pthread_cond_broadcast(&forkless_read_done);
            pthread_mutex_unlock(&forkless_mutex);
            return;
        }

        /* Publish the main-thread read before encoding outside the lock. */
        forkless_capture_kv = kv;
        pthread_mutex_unlock(&forkless_mutex);

        monotime copy_start = getMonotonicUs();
        kvobj *snapshot = forklessCopyValue(kv);
        sds encoded = snapshot ? NULL : forklessCaptureBytes(kv, 0);
        forkless_copy_usec += getMonotonicUs() - copy_start;
        if (snapshot || encoded) forkless_copied_values++;

        pthread_mutex_lock(&forkless_mutex);
        if (!snapshot && !encoded) {
            forklessReleaseRetained(bytes);
            forklessCaptureAbortLocked();
            return;
        }
        pin = forklessPinFindLocked(kv);
        if (pin) {
            /* A prior ordinary write may have pinned an immutable string.
             * Its value is immutable, but its LRU/LFU header is not. Detach
             * the snapshot before allowing a metadata-only mutation. */
            if (detach && pin->state == FORKLESS_PIN_OBJECT &&
                pin->snapshot == kv && snapshot) {
                decrRefCount(pin->snapshot);
                forklessReleaseRetained(pin->accounted_bytes);
                pin->snapshot = snapshot;
                pin->accounted_bytes = bytes;
                bytes = 0;
                snapshot = NULL;
            }
            if (snapshot) decrRefCount(snapshot);
            forklessBufferFree(encoded);
            forklessReleaseRetained(bytes);
            forkless_capture_kv = NULL;
            pthread_cond_broadcast(&forkless_read_done);
            pthread_mutex_unlock(&forkless_mutex);
            return;
        }
        pin = forklessPinGetOrAddLocked(kv);
        if (!pin) {
            if (snapshot) decrRefCount(snapshot);
            forklessBufferFree(encoded);
            forklessReleaseRetained(bytes);
            forklessCaptureAbortLocked();
            return;
        }
        pin->expire = kvobjGetExpire(kv);
        pin->held = snapshot != NULL;
        pin->snapshot = snapshot;
        pin->accounted_bytes = bytes;
        pin->encoded = encoded;
        pin->state = snapshot ? FORKLESS_PIN_OBJECT : FORKLESS_PIN_BYTES;
        kvobjSetSnapshot(kv, forkless_round);
        forklessQueueEarlyLocked(pin, early_dbid);
        forkless_capture_kv = NULL;
        pthread_cond_broadcast(&forkless_read_done);
        pthread_mutex_unlock(&forkless_mutex);
        return;
    }
}

static void forklessCaptureKv(redisDb *db, kvobj *kv, int copy_metadata, int retain_string) {
    mstime_t latency;
    latencyStartMonitor(latency);
    forklessCaptureKvInternal(kv, copy_metadata, retain_string, db);
    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("forkless-capture", latency);
}

void forklessCaptureFromLink(redisDb *db, dictEntryLink link) {
    if (forklessNoForkActive()) {
        if (link) forklessNoForkCapture(db, dictGetKV(*link));
        return;
    }
    if (!forkless_save_active || !forklessCaptureEnabled() || !link ||
        !dictEntryIsForkless(*link)) return;
    kvobj *kv = dictGetKV(*link);
    if (kv) forklessCaptureKv(db, kv, 0, 0);
}

void forklessCaptureForUnlink(redisDb *db, dictEntryLink link) {
    if (forklessNoForkActive()) {
        if (link) forklessNoForkCapture(db, dictGetKV(*link));
        return;
    }
    if (!forkless_save_active || !forklessCaptureEnabled() || !link ||
        !dictEntryIsForkless(*link)) return;
    kvobj *kv = dictGetKV(*link);
    if (kv) forklessCaptureKv(db, kv, 0, 1);
}

/* Module APIs can change metadata even on keys opened read-only. A private
 * object also prevents header-bitfield races with the background encoder. */
void forklessCaptureMetadata(redisDb *db, kvobj *kv) {
    if (forklessNoForkActive()) {
        if (kv && kv->iskvobj) forklessNoForkCapture(db, kv);
        return;
    }
    if (!forkless_save_active || !forklessCaptureEnabled() || !kv ||
        !kv->iskvobj) return;
    /* Expiry metadata can relocate an object without changing its ownership
     * tag. Use the same authority as the child, not its current arena address. */
    dictEntryLink link;
    if (dbFindByLink(db, kvobjGetKey(kv), &link) != kv ||
        !dictEntryIsForkless(*link)) return;
    forklessCaptureKv(db, kv, 1, 0);
}

/* ---------- Start one snapshot ---------- */

static void forklessCloseFd(int *fd) {
    if (*fd == -1) return;
    close(*fd);
    *fd = -1;
}

static void forklessClosePipePair(int p[2]) {
    forklessCloseFd(&p[0]);
    forklessCloseFd(&p[1]);
}

/* Main thread, before the child/helper starts. Constructors initialize marks
 * to zero; database insertion/routing stamps the current round (initially 1).
 * Thus label 2 cannot appear before the first split save, even after RDB load,
 * classic fork saves or configuration changes. No normalization scan is needed
 * to trust that first round. Once issued, a label is never "fresh" again:
 * failure and routing changes retain the existing conservative recovery path. */
static void forklessBeginRound(int mode, size_t cap) {
    if (forkless_marks_mode == -1) {
        serverAssert(forkless_round == 1);
        forkless_marks_valid = 1;
    } else if (forkless_marks_mode != mode || forkless_marks_cap != cap) {
        forkless_marks_valid = 0;
    }
    forkless_marks_mode = mode;
    forkless_marks_cap = cap;
    forkless_round = forkless_round == 1 ? 2 : 1;
    forkless_round_succeeded = 0;
}

int forklessSaveBegin(const char *rdb_tmpfile) {
    if (!forklessEnabled()) return C_ERR;
    if (forkless_save_active) return C_ERR;
    pthread_mutex_lock(&forkless_memory_mutex);
    forkless_memory_limit = server.forkless_snapshot_max_memory;
    /* Retired generations remain charged until cleanup actually frees them. */
    int over_budget = forkless_memory_limit && forkless_memory_bytes > forkless_memory_limit;
    if (over_budget) forkless_memory_denials++;
    pthread_mutex_unlock(&forkless_memory_mutex);
    if (over_budget) return C_ERR;
    if (rdb_tmpfile)
        snprintf(forkless_rdb_tmp, sizeof(forkless_rdb_tmp), "%s", rdb_tmpfile);
    else
        forkless_rdb_tmp[0] = '\0';
    atomicSet(forkless_capture_enabled, 0);
    atomicSet(forkless_serializer_error, 0);
    forkless_serializer_started = 0;
    atomicSet(forkless_serializer_exited, 0);
    forkless_cleanup_pending = 0;
    forkless_work_batch_count = 0;
    forklessRetirePins();
    forkless_priority_keys = 0;
    forkless_eviction.policy = server.maxmemory_policy;
    forkless_eviction.lfu_decay_time = server.lfu_decay_time;
    forkless_eviction.lru_clock = LRU_CLOCK();
    forkless_eviction.lfu_clock = LFUGetTimeInMinutes();
    forkless_child_rio = NULL;

    forklessClosePipePair(forkless_serializer_pipe);
    forklessClosePipePair(forkless_data_pipe);
    forklessClosePipePair(forkless_work_pipe);
    forklessClosePipePair(forkless_early_wake);
    forkless_snapshot_dbnum = server.dbnum;
    forkless_snapshot_dbs = forklessTrackingAlloc(sizeof(*forkless_snapshot_dbs) * forkless_snapshot_dbnum);
    if (!forkless_snapshot_dbs) goto fail;
    for (int i = 0; i < forkless_snapshot_dbnum; i++)
        forkless_snapshot_dbs[i] = server.db[i].keys;
    memset(forkless_snapshot_trimmed, 0, sizeof(forkless_snapshot_trimmed));
    if (server.cluster_enabled) {
        for (int slot = 0; slot < CLUSTER_SLOTS; slot++)
            if (isSlotInTrimJob(slot)) forkless_snapshot_trimmed[slot / 8] |= 1 << (slot % 8);
    }
    if (anetPipe(forkless_early_wake, O_NONBLOCK, O_NONBLOCK) == -1) goto fail;
    if (pipe(forkless_serializer_pipe) == -1) {
        serverLog(LL_WARNING,
                  "forkless: cannot create serializer pipe: %s",
                  strerror(errno));
        goto fail;
    }
    if (pipe(forkless_work_pipe) == -1) {
        serverLog(LL_WARNING, "forkless: cannot create work pipe: %s",
                  strerror(errno));
        goto fail;
    }
    if (pipe(forkless_data_pipe) == -1) {
        serverLog(LL_WARNING, "forkless: cannot create data pipe: %s",
                  strerror(errno));
        goto fail;
    }

    server.active_defrag_paused++;
    forklessBeginRound(server.bgsave_mode, server.forkless_string_max);
    forkless_save_active = 1;
    /* Frozen for the whole save, and inherited by the child, so a concurrent
     * mode change can never make the two writers disagree about who owns a
     * key. CONFIG SET refuses to run while this is set. */
    forkless_split_save = 1;
    return C_OK;

fail:
    forklessClosePipePair(forkless_serializer_pipe);
    forklessClosePipePair(forkless_data_pipe);
    forklessClosePipePair(forkless_work_pipe);
    forklessClosePipePair(forkless_early_wake);
    forklessTrackingFree(forkless_snapshot_dbs, sizeof(*forkless_snapshot_dbs) * forkless_snapshot_dbnum);
    forkless_snapshot_dbs = NULL;
    forkless_rdb_tmp[0] = '\0';
    return C_ERR;
}

static void forklessWriteStatus(char status) {
    if (forkless_serializer_pipe[1] != -1) {
        while (write(forkless_serializer_pipe[1], &status, 1) == -1 && errno == EINTR) {}
        forklessCloseFd(&forkless_serializer_pipe[1]);
    }
}

/* ---------- Parent serializer ---------- */

/* Used by both halves of a split save, including main-thread fallback capture.
 * Match the existing eviction arithmetic, but never consult live server state. */
void forklessEvictionMetadata(unsigned int lru, long long *idle_seconds, int *frequency) {
    *idle_seconds = -1;
    *frequency = -1;
    if (forkless_eviction.policy & MAXMEMORY_FLAG_LRU) {
        unsigned int clock = forkless_eviction.lru_clock;
        unsigned int elapsed = clock >= lru ? clock - lru :
                               clock + (LRU_CLOCK_MAX - lru);
        *idle_seconds = (unsigned long long)elapsed * LRU_CLOCK_RESOLUTION / 1000;
    }
    if (forkless_eviction.policy & MAXMEMORY_FLAG_LFU) {
        unsigned long clock = forkless_eviction.lfu_clock;
        unsigned long last = lru >> 8;
        unsigned long elapsed = clock >= last ? clock - last : 65535 - last + clock;
        unsigned long periods = forkless_eviction.lfu_decay_time ?
                                elapsed / forkless_eviction.lfu_decay_time : 0;
        unsigned int counter = lru & 255;
        *frequency = periods > counter ? 0 : counter - periods;
    }
}

/* Only the hybrid serializer opts in. Main-thread capture buffers must retain
 * independent ownership, including when capture runs while IPC is blocked. */
static _Thread_local int forkless_reuse_buffer;
static _Thread_local sds forkless_encode_scratch;

static void forklessRecycleEncoded(sds encoded) {
    if (forkless_reuse_buffer && !forkless_encode_scratch && encoded && sdsalloc(encoded) <= 64 * 1024) {
        sdsclear(encoded);
        forkless_encode_scratch = encoded; /* Remains charged until released. */
    } else {
        forklessBufferFree(encoded);
    }
}

static sds forklessEncodeValue(robj *value, sds name, long long expire, int dbid)
{
    rio buffer;
    if (forkless_reuse_buffer && forkless_encode_scratch) {
        rioInitWithBuffer(&buffer, forkless_encode_scratch);
        buffer.write = forklessBufferWrite;
        forkless_encode_scratch = NULL;
    } else if (forklessInitBuffer(&buffer) != C_OK) {
        return NULL;
    }
    robj key;
    initStaticStringObject(key, name);
    long long idle;
    int freq;
    forklessEvictionMetadata(value->lru, &idle, &freq);
    unsigned long long started = forklessTimingStart();
    int result = rdbSaveKeyValuePairWithMetadata(&buffer, &key, value, expire, dbid, idle, freq);
    forklessTimingAdd(&forkless_helper_encode_usec, started);
    if (result == -1) {
        forklessBufferFree(buffer.io.buffer.ptr);
        return NULL;
    }
    return buffer.io.buffer.ptr;
}

static sds forklessEncodeKv(kvobj *kv, long long expire, int dbid) {
    return forklessEncodeValue(kv, kvobjGetKey(kv), expire, dbid);
}

/* IPC is framed; framing never appears in the disk or replica RDB stream.
 * Publish only complete records, after releasing the live-object read guard. */
static int forklessWriteEncoded(rio *rdb, sds encoded, int dbid) {
    if (!encoded) return C_ERR;
    int status = C_OK;
    uint64_t header[2] = {sdslen(encoded), (uint64_t)dbid};
    unsigned long long started = forklessTimingStart();
    if (!rioWrite(rdb, header, sizeof(header))) status = C_ERR;
    if (status == C_OK && !rioWrite(rdb, encoded, sdslen(encoded))) status = C_ERR;
    forklessTimingAdd(&forkless_helper_output_usec, started);
    /* Flush the tail at the end of the bounded work window, before waiting
     * for more child work. Per-key flushing causes millions of tiny writes. */
    forklessRecycleEncoded(encoded);
    return status;
}

static void forklessSerializerReadDone(void) {
    pthread_mutex_lock(&forkless_mutex);
    /* The read guard still keeps this live object from being changed/freed.
     * Do not mark pin identities here: those addresses may already be reused. */
    kvobjSetSnapshot(forkless_serializer_kv, forkless_round);
    forkless_serializer_kv = NULL;
    pthread_cond_broadcast(&forkless_read_done);
    pthread_mutex_unlock(&forkless_mutex);
}

/* Emit a child work record or a proven snapshot-start pin. Both use the same
 * encoder and output framing. Early completion keeps an identity tombstone
 * until the child's frozen dictionary confirms that record. */
static int forklessSerializerWriteKey(rio *rdb, int *ready)
{
    for (;;) {
        pthread_mutex_lock(&forkless_mutex);
        /* Select work and acquire its read guard under one lock. Do not
         * pre-claim a batch of live objects: writers should wait only for
         * the one value actually being encoded. Eviction priority remains
         * observable between every pair of records. */
        forklessWorkRec work;
        int from_child;
        *ready = forklessTakeScheduledLocked(&work, &from_child);
        if (!*ready) {
            pthread_mutex_unlock(&forkless_mutex);
            return C_OK;
        }
        kvobj *kv = work.kv;
        int dbid = work.dbid;
        while (forkless_capture_kv == kv)
            pthread_cond_wait(&forkless_read_done, &forkless_mutex);

        /* Cancellation and acquiring a live-value read guard must be ordered
         * by the same mutex. A buffered child pointer may already be freed
         * once main has cancelled the save. Never dereference it afterward. */
        if (forklessSerializerFailed()) {
            pthread_mutex_unlock(&forkless_mutex);
            return C_ERR;
        }

        forklessPin *pin = forklessPinFindLocked(kv);
        if (from_child && pin) pin->awaiting_child = 0;
        if (!from_child) {
            serverAssert(pin && pin->awaiting_child);
            forkless_early_keys++;
        }
        if (pin) {
            if (pin->state == FORKLESS_PIN_WRITTEN) {
                forklessPinRemoveWrittenLocked(kv);
                pthread_mutex_unlock(&forkless_mutex);
                return C_OK;
            }
            if (pin->state == FORKLESS_PIN_BYTES) {
                sds encoded = pin->encoded;
                size_t bytes = pin->accounted_bytes;
                pin->accounted_bytes = 0;
                pin->encoded = NULL;
                pin->state = FORKLESS_PIN_WRITTEN;
                forklessPinRemoveWrittenLocked(kv);
                pthread_mutex_unlock(&forkless_mutex);

                int result = forklessWriteEncoded(rdb, encoded, dbid);
                forklessReleaseRetained(bytes);
                return result;
            }
            if (pin->state == FORKLESS_PIN_OBJECT) {
                kvobj *snapshot = pin->snapshot;
                long long expire = pin->expire;
                int retained = snapshot == kv;
                sds retained_key = pin->retained_key;
                unsigned int encoding = pin->retained_encoding;
                forkless_serializer_kv = snapshot;
                pthread_mutex_unlock(&forkless_mutex);

                sds encoded;
                if (retained) {
                    /* Main may adjust the original refcount while unlinking.
                     * Its type/encoding share that word: encode with a private
                     * header. Payload, key and LRU are protected by the read
                     * guard and the pin's reference. */
                    robj value;
                    initStaticStringObject(value, snapshot->ptr);
                    value.encoding = encoding;
                    value.lru = snapshot->lru;
                    encoded = forklessEncodeValue(&value, retained_key, expire, dbid);
                } else {
                    encoded = forklessEncodeKv(snapshot, expire, dbid);
                }

                pthread_mutex_lock(&forkless_mutex);
                forkless_serializer_kv = NULL;
                pthread_cond_broadcast(&forkless_read_done);
                pin = forklessPinFindLocked(kv);
                if (pin) {
                    pin->state = FORKLESS_PIN_WRITTEN;
                    if (pin->held) {
                        if (forklessQueueDoneLocked(pin->snapshot, pin->accounted_bytes) != C_OK) {
                            /* Leave ownership in the pin for failure cleanup. */
                            pthread_mutex_unlock(&forkless_mutex);
                            forklessBufferFree(encoded);
                            return C_ERR;
                        }
                        pin->held = 0;
                        pin->accounted_bytes = 0;
                    }
                }
                forklessPinRemoveWrittenLocked(kv);
                pthread_mutex_unlock(&forkless_mutex);
                return forklessWriteEncoded(rdb, encoded, dbid);
            }
        }

        /* Untouched: publish the read without allocating a pin. */
        forkless_serializer_kv = kv;
        pthread_mutex_unlock(&forkless_mutex);

        sds encoded = forklessEncodeKv(kv, kvobjGetExpire(kv), dbid);
        forklessSerializerReadDone();
        return forklessWriteEncoded(rdb, encoded, dbid);
    }
}

static int forklessConsumeWorkStream(rio *rdb) {
    forklessWorkRec batch[FORKLESS_WORK_BATCH_SIZE];
    size_t buffered = 0;

    while (1) {
        /* Bound each turn so early traffic cannot starve child pipe reads. */
        for (size_t i = 0; i < FORKLESS_WORK_BATCH_SIZE; i++) {
            if (forklessSerializerFailed()) return C_ERR;
            int ready;
            if (forklessSerializerWriteKey(rdb, &ready) == C_ERR) return C_ERR;
            if (!ready) break;
        }
        /* The child may be waiting for a frame tail before sending more work.
         * Flush before sleeping, not once per key. */
        unsigned long long started = forklessTimingStart();
        int flush_error = fflush(rdb->io.file.fp);
        forklessTimingAdd(&forkless_helper_output_usec, started);
        if (flush_error) return C_ERR;
        pthread_mutex_lock(&forkless_mutex);
        int pending = forkless_early_count != 0;
        int child_pending = forkless_ready_remaining != 0;
        pthread_mutex_unlock(&forkless_mutex);
        /* Interleaving can leave child records in the current window. Never
         * replace that window or accept EOF until all of them are consumed. */
        if (child_pending) continue;
        struct pollfd fds[2] = {
            {.fd = forkless_work_pipe[0], .events = POLLIN},
            {.fd = forkless_early_wake[0], .events = POLLIN}
        };
        int polled;
        started = pending ? 0 : forklessTimingStart();
        do { polled = poll(fds, 2, pending ? 0 : -1); } while (polled == -1 && errno == EINTR);
        forklessTimingAdd(&forkless_helper_work_wait_usec, started);
        if (polled == -1) return C_ERR;
        if (fds[1].revents & POLLIN) {
            char wake[128];
            while (read(forkless_early_wake[0], wake, sizeof(wake)) > 0) {}
        }
        if (!(fds[0].revents & (POLLIN | POLLHUP | POLLERR))) continue;
        /* Only an incomplete record may carry over between reads. */
        if (buffered >= sizeof(*batch)) return C_ERR;
        char *next = (char *)batch + buffered;
        ssize_t n = read(forkless_work_pipe[0], next,
                         sizeof(batch) - buffered);
        if (n == 0) {
            return buffered == 0 ? C_OK : C_ERR;
        }
        if (n == -1 && errno == EINTR) continue;
        if (n == -1) return C_ERR;
        buffered += (size_t)n;

        size_t count = buffered / sizeof(*batch);
        pthread_mutex_lock(&forkless_mutex);
        forklessPublishReadyLocked(batch, count);
        pthread_mutex_unlock(&forkless_mutex);

        buffered %= sizeof(*batch);
        if (buffered)
            memmove(batch, (char *)batch + count * sizeof(*batch), buffered);
    }
}

static int forklessSerializerFailed(void) {
    int error;
    atomicGet(forkless_serializer_error, error);
    return error;
}

static void forklessSignalSerializerResult(void) {
    forklessWriteStatus(forklessSerializerFailed() ?
                        FORKLESS_STATUS_ERR :
                        FORKLESS_STATUS_OK);
}

static void *forklessSerializerMain(void *arg) {
    UNUSED(arg);
    FILE *fp = fdopen(forkless_data_pipe[1], "wb");
    if (!fp) {
        forklessCloseFd(&forkless_data_pipe[1]);
        forklessCloseFd(&forkless_work_pipe[0]);
        atomicSet(forkless_serializer_error, 1);
        forklessSignalSerializerResult();
        atomicSet(forkless_serializer_exited, 1);
        return NULL;
    }

    rio rdb;
    rioInitWithFile(&rdb, fp);
    forkless_reuse_buffer = 1;
    /* Only the child owns output/checksum/fsync. The helper sends complete
     * frames after releasing the live-object read guard. */
    if (forklessConsumeWorkStream(&rdb) == C_ERR)
        atomicSet(forkless_serializer_error, 1);
    forklessBufferFree(forkless_encode_scratch);
    forkless_encode_scratch = NULL;
    forkless_reuse_buffer = 0;

    unsigned long long output_started = forklessTimingStart();
    if (fflush(fp))
        atomicSet(forkless_serializer_error, 1);
    if (fclose(fp)) atomicSet(forkless_serializer_error, 1);
    forklessTimingAdd(&forkless_helper_output_usec, output_started);
    forkless_data_pipe[1] = -1;

    forklessCloseFd(&forkless_work_pipe[0]);
    atomicSet(forkless_capture_enabled, 0);
    forklessSignalSerializerResult();
    atomicSet(forkless_serializer_exited, 1);
    return NULL;
}

static int forklessSerializerCanStart(void) {
    return forkless_work_pipe[0] != -1;
}

static int forklessStartSerializer(void) {
    if (!forkless_save_active) return C_ERR;
    if (forkless_data_pipe[1] == -1) return C_ERR;

    forklessCloseFd(&forkless_data_pipe[0]);
    forklessCloseFd(&forkless_serializer_pipe[0]);
    forklessCloseFd(&forkless_work_pipe[1]);

    if (!forklessSerializerCanStart() ||
        pthread_create(&forkless_serializer_tid, NULL,
                       forklessSerializerMain,
                       NULL) != 0)
    {
        forklessCloseFd(&forkless_data_pipe[1]);
        atomicSet(forkless_serializer_error, 1);
        forklessCloseFd(&forkless_work_pipe[0]);
        forklessSignalSerializerResult();
        return C_ERR;
    }
    forkless_serializer_started = 1;
    atomicSet(forkless_capture_enabled, 1);
    return C_OK;
}

int forklessStartDiskSerializer(void) {
    return forklessStartSerializer();
}

int forklessStartSocketSerializer(void) {
    return forklessStartSerializer();
}

/* ---------- Child work stream ---------- */

typedef struct forklessFrameReader {
    unsigned char buf[64*1024];
    size_t pos;
    size_t end;
} forklessFrameReader;

/* Child-owned: preserve prefetched bytes when yielding at a frame boundary. */
static forklessFrameReader forkless_child_reader;

/* Read whatever is available, not an entire buffer's worth. Waiting for a
 * full buffer could deadlock with the helper waiting for more work from us.
 * Only wait inside a frame: returning mid-frame would let child-owned RDB
 * records interrupt its bytes. At frame boundaries EAGAIN yields to work
 * production. Returns bytes available, zero for EOF, or -1 with errno set. */
static ssize_t forklessFrameReaderFill(forklessFrameReader *reader, int fd, int wait) {
    if (reader->pos < reader->end) return reader->end - reader->pos;
    while (1) {
        unsigned long long started = forklessTimingStart();
        ssize_t n = read(fd, reader->buf, sizeof(reader->buf));
        if (started) forkless_child_input_usec += getMonotonicUs() - started;
        if (n >= 0) {
            reader->pos = 0;
            reader->end = n;
            return n;
        }
        if (n == -1 && errno == EINTR) continue;
        if (wait && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            struct pollfd pfd = {.fd = fd, .events = POLLIN};
            started = forklessTimingStart();
            int polled = poll(&pfd, 1, -1);
            if (started) forkless_child_input_usec += getMonotonicUs() - started;
            if (polled == -1 && errno != EINTR) return -1;
            continue;
        }
        return -1;
    }
}

static int forklessChildRelayFramesLimited(size_t frame_limit) {
    if (forkless_data_pipe[0] == -1) return C_OK;
    if (!forkless_child_rio) return C_OK;

    forklessFrameReader *reader = &forkless_child_reader;
    size_t frames = 0;
    while (1) {
        /* Yield only between complete RDB records; keep prefetched bytes. */
        if (frame_limit && frames >= frame_limit)
            return C_OK;
        uint64_t header[2];
        ssize_t n = forklessFrameReaderFill(reader, forkless_data_pipe[0], 0);
        if (n > 0) {
            size_t copied = 0;
            while (copied < sizeof(header)) {
                n = forklessFrameReaderFill(reader, forkless_data_pipe[0], 1);
                if (n <= 0) return C_ERR; /* Truncated header. */
                size_t take = sizeof(header) - copied;
                if (take > (size_t)n) take = n;
                memcpy((char *)header + copied, reader->buf + reader->pos, take);
                reader->pos += take;
                copied += take;
            }
            if (header[1] >= (uint64_t)server.dbnum) return C_ERR;
            if (rdbSaveType(forkless_child_rio, RDB_OPCODE_SELECTDB) == -1 ||
                rdbSaveLen(forkless_child_rio, header[1]) == -1) return C_ERR;
            uint64_t left = header[0];
            while (left) {
                n = forklessFrameReaderFill(reader, forkless_data_pipe[0], 1);
                if (n <= 0) return C_ERR; /* Truncated payload. */
                size_t take = left > (uint64_t)n ? (size_t)n : (size_t)left;
                if (!rioWrite(forkless_child_rio, reader->buf + reader->pos, take)) return C_ERR;
                reader->pos += take;
                left -= take;
            }
            frames++;
            continue;
        }
        if (n == -1 && errno == EINTR) continue;
        if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) return C_OK;
        if (n == 0) {
            forklessCloseFd(&forkless_data_pipe[0]);
            return C_OK;
        }
        return C_ERR;
    }
}

static int forklessChildRelayFrames(void) {
    return forklessChildRelayFramesLimited(0);
}

static void forklessChildPreparePipes(void) {
    if (forkless_work_pipe[0] == -1) return;

    forklessCloseFd(&forkless_work_pipe[0]);
    forklessClosePipePair(forkless_early_wake);

    if (forklessStreamActive()) {
        int flags = fcntl(forkless_work_pipe[1], F_GETFL, 0);
        if (flags != -1)
            fcntl(forkless_work_pipe[1], F_SETFL, flags | O_NONBLOCK);
        if (forkless_data_pipe[0] != -1) {
            flags = fcntl(forkless_data_pipe[0], F_GETFL, 0);
            if (flags != -1)
                fcntl(forkless_data_pipe[0], F_SETFL, flags | O_NONBLOCK);
        }
        forklessCloseFd(&forkless_data_pipe[1]);
    }
}

static int forklessChildWriteWork(const void *buf, size_t len) {
    const char *p = buf;
    size_t left = len;

    forklessChildPreparePipes();
    while (left) {
        ssize_t n = write(forkless_work_pipe[1], p, left);
        if (n > 0) {
            p += n;
            left -= (size_t)n;
            continue;
        }
        if (n == -1 && errno == EINTR) continue;
        if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            if (forklessStreamActive()) {
                if (forklessChildRelayFramesLimited(32) == C_ERR) return C_ERR;
                struct pollfd pfd[2];
                int nfds = 1;
                pfd[0].fd = forkless_work_pipe[1];
                pfd[0].events = POLLOUT;
                if (forkless_data_pipe[0] != -1) {
                    pfd[1].fd = forkless_data_pipe[0];
                    pfd[1].events = POLLIN;
                    nfds = 2;
                }
                unsigned long long started = forklessTimingStart();
                poll(pfd, nfds, 10);
                if (started) forkless_child_work_wait_usec += getMonotonicUs() - started;
                continue;
            }
            struct pollfd pfd = { .fd = forkless_work_pipe[1], .events = POLLOUT };
            unsigned long long started = forklessTimingStart();
            int polled = poll(&pfd, 1, -1);
            if (started) forkless_child_work_wait_usec += getMonotonicUs() - started;
            if (polled == -1 && errno != EINTR) return C_ERR;
            continue;
        }
        return C_ERR;
    }
    return C_OK;
}

static int forklessChildFlushWork(void) {
    size_t bytes = forkless_work_batch_count * sizeof(*forkless_work_batch);
    if (bytes == 0) return C_OK;
    int status = forklessChildWriteWork(forkless_work_batch, bytes);
    forkless_work_batch_count = 0;
    return status;
}

/* Send even a partial batch before encoding a child-owned key. Service the
 * return pipe too, but give child-owned keys a turn under continuous output. */
int forklessChildPumpWork(void) {
    forklessChildPreparePipes();
    if (forklessChildFlushWork() == C_ERR) return C_ERR;
    return forklessChildRelayFramesLimited(32);
}

int forklessEmitWork(kvobj *kv, int dbid) {
    if (!server.in_fork_child || !forkless_split_save || !kv) return C_ERR;
    if (forkless_work_pipe[1] == -1) return C_ERR;

    forklessWorkRec *rec = &forkless_work_batch[forkless_work_batch_count++];
    rec->kv = kv;
    rec->dbid = dbid;

    if (forkless_work_batch_count == FORKLESS_WORK_BATCH_SIZE)
        return forklessChildFlushWork();
    return C_OK;
}

static int forklessChildCloseWorkWriter(void) {
    int status = forklessChildFlushWork();
    forklessCloseFd(&forkless_work_pipe[1]);
    forklessCloseFd(&forkless_work_pipe[0]);
    return status;
}


static char forklessChildReadStatus(char error_status) {
    forklessCloseFd(&forkless_serializer_pipe[1]);
    char status = error_status;
    if (forkless_serializer_pipe[0] != -1) {
        ssize_t n;
        while ((n = read(forkless_serializer_pipe[0], &status, 1)) == -1 &&
               errno == EINTR) {}
        if (n != 1) status = error_status;
        forklessCloseFd(&forkless_serializer_pipe[0]);
    }
    return status;
}
void forklessChildSetOutput(rio *rdb) {
    forkless_child_rio = rdb;
    if (server.forkless_timing && rdb->write != forklessTimedChildWrite) {
        forkless_child_write = rdb->write;
        rdb->write = forklessTimedChildWrite;
    }
    forkless_child_reader.pos = forkless_child_reader.end = 0;
}

int forklessChildFinishStream(rio *rdb) {
    forkless_child_rio = rdb;
    if (!forklessEnabled() || forkless_data_pipe[0] == -1) return C_ERR;
    if (forklessChildCloseWorkWriter() != C_OK) return C_ERR;
    forklessCloseFd(&forkless_data_pipe[1]);
    forklessCloseFd(&forkless_serializer_pipe[1]);

    /* Remaining bytes can block until the serializer closes. */
    int flags = fcntl(forkless_data_pipe[0], F_GETFL, 0);
    if (flags != -1)
        fcntl(forkless_data_pipe[0], F_SETFL, flags & ~O_NONBLOCK);

    int retval = forklessChildRelayFrames();
    forklessCloseFd(&forkless_data_pipe[0]);
    forkless_child_rio = NULL;

    char status = forklessChildReadStatus(FORKLESS_STATUS_ERR);
    if (server.forkless_timing)
        serverLog(LL_NOTICE, "Hybrid timing: child_encode_with_output_usec=%llu child_helper_input_usec=%llu child_output_usec=%llu child_work_wait_usec=%llu status=%c",
                  forkless_child_encode_usec, forkless_child_input_usec,
                  forkless_child_output_usec, forkless_child_work_wait_usec, status);
    if (rdb->write == forklessTimedChildWrite) rdb->write = forkless_child_write;
    return retval == C_OK && status == FORKLESS_STATUS_OK ? C_OK : C_ERR;
}

/* ---------- Finish or abort the snapshot ---------- */

static void forklessJoinSerializer(void) {
    if (!forkless_serializer_started) return;
    mstime_t latency;
    latencyStartMonitor(latency);
    pthread_join(forkless_serializer_tid, NULL);
    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("forkless-join", latency);
    forkless_serializer_started = 0;
}

void forklessSaveCleanup(void) {
    if (forkless_serializer_started) {
        int exited;
        atomicGet(forkless_serializer_exited, exited);
        if (!exited) {
            pthread_mutex_lock(&forkless_mutex);
            atomicSet(forkless_serializer_error, 1);
            /* Stop acquiring guards before allowing uncaptured writes. Only
             * an existing value read must finish, never the helper's I/O. */
            while (forkless_serializer_kv)
                pthread_cond_wait(&forkless_read_done, &forkless_mutex);
            atomicSet(forkless_capture_enabled, 0);
            pthread_mutex_unlock(&forkless_mutex);
            /* The child is exiting, so EOF/EPIPE will wake the helper. Keep
             * its descriptors, pins and defrag pause until it reports exit.
             * Gate new saves while this deferred cleanup still owns state. */
            forkless_cleanup_pending = 1;
            return;
        }
        forklessJoinSerializer();
    }
    atomicSet(forkless_capture_enabled, 0);
    forkless_cleanup_pending = 0;
    if (forkless_save_active)
        forkless_marks_valid = forkless_round_succeeded;
    forklessClosePipePair(forkless_serializer_pipe);
    forklessClosePipePair(forkless_data_pipe);
    forklessClosePipePair(forkless_work_pipe);

    forklessRetirePins();
    forklessClosePipePair(forkless_early_wake);
    forklessTrackingFree(forkless_snapshot_dbs, sizeof(*forkless_snapshot_dbs) * forkless_snapshot_dbnum);
    forkless_snapshot_dbs = NULL;

    if (forkless_save_active && server.active_defrag_paused > 0)
        server.active_defrag_paused--;
    forkless_save_active = 0;
    forkless_split_save = 0;
    forkless_child_rio = NULL;
    if (forkless_rdb_tmp[0]) unlink(forkless_rdb_tmp);
    forkless_rdb_tmp[0] = '\0';
}

void forklessHybridCron(void) {
    if (!forkless_cleanup_pending) return;
    int exited;
    atomicGet(forkless_serializer_exited, exited);
    if (exited) {
        forklessSaveCleanup();
        updateDictResizePolicy();
    }
}

/* Only call after the child and final output have succeeded. Worker completion
 * alone is insufficient: a later pipe, disk or publication error fails the save. */
void forklessSaveComplete(void) {
    forklessJoinSerializer();
    if (forkless_save_active && !forklessSerializerFailed() &&
        forkless_marks_cap == server.forkless_string_max)
        forkless_round_succeeded = 1;
    forklessSaveCleanup();
}

void forklessAbortBeforeFlush(void) {
    if (forklessNoForkActive()) {
        killRDBChild();
        return;
    }
    if (!forkless_save_active) return;
    atomicSet(forkless_serializer_error, 1);
    killRDBChild();
    forklessSaveCleanup();
}


int forklessParentInstallRdb(const char *child_tmpfile, const char *filename) {
    forklessJoinSerializer();

    if (forklessSerializerFailed()) {
        serverLog(LL_WARNING, "forkless: background serializer failed");
        unlink(child_tmpfile);
        forklessSaveCleanup();
        return C_ERR;
    }

    if (rename(child_tmpfile, filename) == -1) {
        serverLog(LL_WARNING, "forkless: rename %s -> %s failed: %s",
                  child_tmpfile, filename, strerror(errno));
        unlink(child_tmpfile);
        forklessSaveCleanup();
        return C_ERR;
    }
    mstime_t latency;
    latencyStartMonitor(latency);
    int sync_status = fsyncFileDir(filename);
    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("forkless-directory-sync", latency);
    if (sync_status == -1) {
        serverLog(LL_WARNING,
                  "forkless: failed to sync RDB directory: %s",
                  strerror(errno));
        forklessSaveCleanup();
        return C_ERR;
    }
    forklessSaveComplete();
    return C_OK;
}

/* ---------- Full forkless snapshot: main owns dictionaries and object lifetime.
 * The worker receives private built-in copies or frozen string views. A mark means that the
 * snapshot version has been queued (or the key was born after the boundary).
 * dictScan tolerates live rehashing and duplicates; marks suppress duplicates.
 * No dictionary pointer, module callback, or refcount is used by the worker. */
typedef struct noforkRecord {
    struct noforkRecord *next;
    kvobj *value;
    kvobj *retained; /* Main-only lifetime reference; worker never dereferences it. */
    robj string_view; /* Frozen header, including encoding, ptr and LRU. */
    sds string_key; /* Borrowed from retained; key bytes never mutate in place. */
    long long string_expire;
    hashTemplate *tmpl; /* Borrowed from value's owned template reference. */
    sds bytes;
    size_t charge;
    int dbid;
} noforkRecord;

static struct {
    int active, started, fd, socket, dbid, scanning_done;
    int compression, checksum, data, autosync, reclaim_cache;
    long long now, delay;
    unsigned long long cursor;
    kvstore **databases; /* O(number of databases), follows SWAPDB identities. */
    unsigned char *trimmed_slots;
    int dbnum;
    size_t outstanding, done_count;
    noforkRecord *head, *tail, *done, *done_tail;
    int wake[2];
    sds prefix, suffix;
    char *output;
    size_t output_used;
    size_t unsynced;
    char tmp[256], filename[PATH_MAX], eofmark[CONFIG_RUN_ID_SIZE];
    pthread_t tid;
    redisAtomic int cancelled;
    redisAtomic int exited;
} nofork;
static pthread_cond_t nofork_work = PTHREAD_COND_INITIALIZER;
static noforkRecord *nofork_retired;

static void noforkWakeMain(void) {
    char byte = 1;
    /* Nonblocking: a full pipe already guarantees an event-loop wakeup. */
    while (write(nofork.wake[1], &byte, 1) == -1 && errno == EINTR) {}
}

static void noforkWakeHandler(aeEventLoop *el, int fd, void *priv, int mask) {
    UNUSED(el); UNUSED(priv); UNUSED(mask);
    char bytes[256];
    while (read(fd, bytes, sizeof(bytes)) > 0) {}
}

int forklessNoForkActive(void) { return nofork.active; }

void forklessNoForkCancel(void) {
    if (!nofork.active) return;
    atomicSet(nofork.cancelled, 1);
    pthread_mutex_lock(&forkless_mutex);
    pthread_cond_broadcast(&nofork_work);
    pthread_mutex_unlock(&forkless_mutex);
}

static int noforkCancelled(void) {
    int cancelled;
    atomicGet(nofork.cancelled, cancelled);
    return cancelled;
}

/* Can block on a large value, as explicitly permitted for full forkless.
 * All budget checks precede publication and all failures cancel the save. */
static void noforkCaptureKey(kvobj *kv, int dbid) {
    serverAssert(pthread_equal(pthread_self(), server.main_thread_id));
    if (noforkCancelled()) return;
    if (kvobjSnapshot(kv) == forkless_round) {
        forkless_capture_skipped++;
        return;
    }
    if (nofork.trimmed_slots) {
        int slot = getKeySlot(kvobjGetKey(kv));
        if (nofork.trimmed_slots[slot / 8] & (1 << (slot % 8))) {
            kvobjSetSnapshot(kv, forkless_round);
            return;
        }
    }
    noforkRecord *rec = forklessTrackingAlloc(sizeof(*rec));
    if (!rec) goto fail;
    size_t bytes = kvobjAllocSize(kv), retained;
    atomicGet(forkless_retained_bytes, retained);
    size_t limit = server.forkless_retained_max_memory;
    if ((limit && (bytes > limit || retained > limit - bytes)) || !forklessMemoryReserve(bytes)) {
        forklessTrackingFree(rec, sizeof(*rec));
        goto fail;
    }
    rec->charge = bytes;
    rec->dbid = dbid;
    atomicIncr(forkless_retained_bytes, bytes);
    if (retained + bytes > forkless_retained_peak) forkless_retained_peak = retained + bytes;
    monotime begin = getMonotonicUs();
    if (kv->type == OBJ_MODULE || getModuleMetaBits(kvobjMetaBits(kv))) {
        /* MAIN only: module metadata and module values are never copied into
         * objects the worker might serialize or destroy. */
        rec->bytes = forklessEncodeKv(kv, kvobjGetExpire(kv), dbid);
    } else if (kv->type == OBJ_STRING) {
        incrRefCount(kv);
        rec->retained = kv;
        initStaticStringObject(rec->string_view, kv->ptr);
        rec->string_view.encoding = kv->encoding;
        rec->string_view.lru = kv->lru;
        rec->string_key = kvobjGetKey(kv);
        rec->string_expire = kvobjGetExpire(kv);
        forkless_retained_strings++;
    } else {
        rec->value = forklessCopyValue(kv);
        if (rec->value && rec->value->type == OBJ_HASH &&
            (rec->value->encoding == OBJ_ENCODING_TMPL_LP || rec->value->encoding == OBJ_ENCODING_TMPL_ARRAY))
            rec->tmpl = hashTypeGetTemplate(rec->value);
    }
    forkless_copy_usec += getMonotonicUs() - begin;
    if (!rec->bytes && !rec->value && !rec->retained) {
        forklessReleaseRetained(bytes);
        forklessTrackingFree(rec, sizeof(*rec));
        goto fail;
    }
    if (!rec->retained) forkless_copied_values++;
    kvobjSetSnapshot(kv, forkless_round);
    pthread_mutex_lock(&forkless_mutex);
    if (nofork.tail) nofork.tail->next = rec;
    else nofork.head = rec;
    nofork.tail = rec;
    nofork.outstanding++;
    pthread_cond_signal(&nofork_work);
    pthread_mutex_unlock(&forkless_mutex);
    return;
fail:
    forkless_limit_aborts++;
    forklessNoForkCancel();
}

static void forklessNoForkCapture(redisDb *db, kvobj *kv) {
    if (nofork.scanning_done || noforkCancelled()) return;
    /* Keep original DB numbers after SWAPDB, including mutation captures. */
    for (int i = 0; i < nofork.dbnum; i++) {
        if (nofork.databases[i] == db->keys) {
            noforkCaptureKey(kv, i);
            return;
        }
    }
}

static void noforkScan(void *priv, const dictEntry *de, dictEntry **link) {
    UNUSED(priv);
    UNUSED(link);
    noforkCaptureKey(dictGetKV((dictEntry *)de), nofork.dbid);
}

/* Nonblocking pipe writes make cancellation independent of replica progress.
 * Disk writes are ordinary bounded writes; their descriptors have one owner. */
static size_t noforkWriteRaw(const void *buf, size_t len) {
    const char *p = buf;
    while (len) {
        if (noforkCancelled()) return 0;
        size_t amount = len > 65536 ? 65536 : len;
        ssize_t n = write(nofork.fd, p, amount);
        if (n > 0) { p += n; len -= n; continue; }
        if (n == -1 && errno == EINTR) continue;
        if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            struct pollfd fd = {.fd = nofork.fd, .events = POLLOUT};
            if (poll(&fd, 1, 20) == -1 && errno != EINTR) return 0;
            continue;
        }
        return 0;
    }
    return 1;
}

#define NOFORK_OUTPUT_SIZE (64 * 1024)
static int noforkFlush(void) {
    if (!noforkWriteRaw(nofork.output, nofork.output_used)) return 0;
    nofork.unsynced += nofork.output_used;
    nofork.output_used = 0;
    if (!nofork.socket && nofork.autosync && nofork.unsynced >= REDIS_AUTOSYNC_BYTES) {
        if (redis_fsync(nofork.fd) == -1) return 0;
        if (nofork.reclaim_cache) reclaimFilePageCache(nofork.fd, 0, 0);
        nofork.unsynced = 0;
    }
    return 1;
}

static size_t noforkWrite(rio *r, const void *buf, size_t len) {
    UNUSED(r);
    const char *p = buf;
    while (len) {
        if (noforkCancelled()) return 0;
        size_t available = NOFORK_OUTPUT_SIZE - nofork.output_used;
        size_t n = len < available ? len : available;
        memcpy(nofork.output + nofork.output_used, p, n);
        nofork.output_used += n;
        p += n;
        len -= n;
        if (nofork.output_used == NOFORK_OUTPUT_SIZE && !noforkFlush()) return 0;
    }
    return 1;
}

static void *noforkWorker(void *arg) {
    UNUSED(arg);
    rdbSetThreadSnapshot(nofork.compression, nofork.now, nofork.delay);
    rio rdb = {0};
    rdb.write = noforkWrite;
    int ok = 1;
    int last_dbid = -1;
    if (nofork.socket) {
        char header[CONFIG_RUN_ID_SIZE + 8];
        memcpy(header, "$EOF:", 5);
        memcpy(header + 5, nofork.eofmark, CONFIG_RUN_ID_SIZE);
        memcpy(header + 5 + CONFIG_RUN_ID_SIZE, "\r\n", 2);
        ok = rioWrite(&rdb, header, CONFIG_RUN_ID_SIZE + 7);
    }
    if (nofork.checksum) rdb.update_cksum = rioGenericUpdateChecksum;
    if (ok) ok = rioWrite(&rdb, nofork.prefix, sdslen(nofork.prefix));
    while (ok && !noforkCancelled()) {
        pthread_mutex_lock(&forkless_mutex);
        while (!nofork.head && !nofork.scanning_done && !noforkCancelled())
            pthread_cond_wait(&nofork_work, &forkless_mutex);
        noforkRecord *rec = nofork.head;
        if (rec) {
            nofork.head = rec->next;
            if (!nofork.head) nofork.tail = NULL;
        }
        int finished = nofork.scanning_done;
        pthread_mutex_unlock(&forkless_mutex);
        if (!rec) {
            if (finished || noforkCancelled()) break;
            continue;
        }
        if (last_dbid != rec->dbid) {
            ok = rdbSaveType(&rdb, RDB_OPCODE_SELECTDB) != -1 && rdbSaveLen(&rdb, rec->dbid) != -1;
            if (ok) last_dbid = rec->dbid;
        }
        if (ok && rec->bytes) ok = rioWrite(&rdb, rec->bytes, sdslen(rec->bytes));
        if (ok && rec->retained) {
            robj key;
            initStaticStringObject(key, rec->string_key);
            long long idle;
            int freq;
            forklessEvictionMetadata(rec->string_view.lru, &idle, &freq);
            ok = rdbSaveKeyValuePairWithMetadata(&rdb, &key, &rec->string_view,
                     rec->string_expire, rec->dbid, idle, freq) != -1;
        }
        if (ok && rec->value) {
            rdbSetThreadSnapshotTemplate(rec->tmpl);
            robj key;
            initStaticStringObject(key, kvobjGetKey(rec->value));
            long long idle;
            int freq;
            forklessEvictionMetadata(rec->value->lru, &idle, &freq);
            ok = rdbSaveKeyValuePairWithMetadata(&rdb, &key, rec->value,
                     kvobjGetExpire(rec->value), rec->dbid, idle, freq) != -1;
            rdbSetThreadSnapshotTemplate(NULL);
        }
        pthread_mutex_lock(&forkless_mutex);
        rec->next = nofork.done;
        int wake = nofork.done == NULL;
        if (wake) nofork.done_tail = rec;
        nofork.done = rec;
        nofork.done_count++;
        pthread_mutex_unlock(&forkless_mutex);
        if (wake) noforkWakeMain();
    }
    if (ok && !noforkCancelled()) {
        ok = rioWrite(&rdb, nofork.suffix, sdslen(nofork.suffix)) &&
             rdbSaveType(&rdb, RDB_OPCODE_EOF) != -1;
        uint64_t checksum = rdb.cksum;
        memrev64ifbe(&checksum);
        if (ok) ok = rioWrite(&rdb, &checksum, sizeof(checksum));
        rdb.update_cksum = NULL;
        if (ok && nofork.socket) ok = rioWrite(&rdb, nofork.eofmark, CONFIG_RUN_ID_SIZE);
        if (ok) ok = noforkFlush();
        if (ok && !nofork.socket) ok = fsync(nofork.fd) == 0;
        if (ok && !nofork.socket && nofork.reclaim_cache) reclaimFilePageCache(nofork.fd, 0, 0);
    }
    if (close(nofork.fd)) ok = 0;
    if (!ok) atomicSet(nofork.cancelled, 1);
    atomicSet(nofork.exited, 1);
    noforkWakeMain();
    return NULL;
}

static void noforkRetireRecords(noforkRecord *head, noforkRecord *tail) {
    if (!head) return;
    tail->next = nofork_retired;
    nofork_retired = head;
}

static void noforkReleaseRetired(void) {
    if (lazyfreeHasPendingDbJobs()) return;
    monotime start = getMonotonicUs();
    while (nofork_retired && forkless_cleanup_slots < FORKLESS_CLEANUP_SLOTS &&
           forkless_cleanup_used + getMonotonicUs() - start < FORKLESS_CLEANUP_USEC) {
        noforkRecord *rec = nofork_retired;
        nofork_retired = rec->next;
        if (rec->value) forklessReleaseSnapshot(rec->value);
        if (rec->retained) decrRefCount(rec->retained);
        forklessBufferFree(rec->bytes);
        forklessReleaseRetained(rec->charge);
        forklessTrackingFree(rec, sizeof(*rec));
        forkless_cleanup_slots++;
    }
    forkless_cleanup_used += getMonotonicUs() - start;
}

static void noforkFreeState(void) {
    noforkRetireRecords(nofork.head, nofork.tail);
    noforkRetireRecords(nofork.done, nofork.done_tail);
    forklessBufferFree(nofork.prefix);
    forklessBufferFree(nofork.suffix);
    forklessTrackingFree(nofork.output, NOFORK_OUTPUT_SIZE);
    forklessTrackingFree(nofork.databases, sizeof(*nofork.databases) * nofork.dbnum);
    forklessTrackingFree(nofork.trimmed_slots, CLUSTER_SLOTS / 8);
    nofork.head = nofork.tail = nofork.done = NULL;
    nofork.prefix = nofork.suffix = NULL;
    nofork.output = NULL;
    nofork.databases = NULL;
    nofork.trimmed_slots = NULL;
    if (nofork.wake[0] != -1) {
        aeDeleteFileEvent(server.el, nofork.wake[0], AE_READABLE);
        close(nofork.wake[0]);
    }
    if (nofork.wake[1] != -1) close(nofork.wake[1]);
    if (nofork.tmp[0]) unlink(nofork.tmp);
    nofork.active = 0;
}

int forklessNoForkStart(int req, const char *filename, rdbSaveInfo *rsi, int flags, int fd) {
    serverAssert(pthread_equal(pthread_self(), server.main_thread_id));
    if (nofork.active || hasActiveChildProcess()) return C_ERR;
    /* Partial slot snapshots have their own dictionary/metadata protocol. */
    if (req & SLAVE_REQ_SLOTS_SNAPSHOT) return C_ERR;
    memset(&nofork, 0, sizeof(nofork));
    nofork.wake[0] = nofork.wake[1] = -1;
    nofork.fd = fd;
    nofork.socket = fd != -1;
    nofork.dbnum = server.dbnum;
    nofork.data = !(req & SLAVE_REQ_RDB_EXCLUDE_DATA);
    nofork.compression = !(req & SLAVE_REQ_RDB_NO_COMPRESS) && server.rdb_compression;
    nofork.checksum = !(req & SLAVE_REQ_RDB_NO_CHECKSUM) && server.rdb_checksum;
    nofork.now = server.mstime;
    nofork.delay = server.rdb_key_save_delay;
    nofork.autosync = server.rdb_save_incremental_fsync;
    nofork.reclaim_cache = !(flags & RDBFLAGS_KEEP_CACHE);
    pthread_mutex_lock(&forkless_memory_mutex);
    forkless_memory_limit = server.forkless_snapshot_max_memory;
    int over = forkless_memory_limit && forkless_memory_bytes > forkless_memory_limit;
    if (over) forkless_memory_denials++;
    pthread_mutex_unlock(&forkless_memory_mutex);
    if (over) return C_ERR;
    nofork.databases = forklessTrackingAlloc(sizeof(*nofork.databases) * nofork.dbnum);
    if (!nofork.databases) return C_ERR;

    /* Failed live scans cannot reuse their marks: an unvisited key can have
     * either label. Normalize without an all-keys allocation before publishing
     * another snapshot boundary. Also covers switching from hybrid/fork. */
    int normalize = forkless_marks_mode != -1 &&
                    (!forkless_marks_valid || forkless_marks_mode != BGSAVE_MODE_FORKLESS);
    for (int i = 0; i < nofork.dbnum; i++) {
        nofork.databases[i] = server.db[i].keys;
        if (!normalize) continue;
        kvstoreIterator it;
        kvstoreIteratorInit(&it, nofork.databases[i]);
        dictEntry *de;
        while ((de = kvstoreIteratorNext(&it))) kvobjSetSnapshot(dictGetKV(de), forkless_round);
        kvstoreIteratorReset(&it);
    }
    forklessBeginRound(BGSAVE_MODE_FORKLESS, server.forkless_string_max);
    forkless_marks_valid = 1;
    forkless_eviction.policy = server.maxmemory_policy;
    forkless_eviction.lfu_decay_time = server.lfu_decay_time;
    forkless_eviction.lru_clock = LRU_CLOCK();
    forkless_eviction.lfu_clock = LFUGetTimeInMinutes();
    nofork.active = forkless_save_active = forkless_split_save = 1;
    server.active_defrag_paused++;
    startSaving(flags);

    nofork.output = forklessTrackingAlloc(NOFORK_OUTPUT_SIZE);
    if (!nofork.output) goto fail;
    if (server.cluster_enabled) {
        nofork.trimmed_slots = forklessTrackingAlloc(CLUSTER_SLOTS / 8);
        if (!nofork.trimmed_slots) goto fail;
        for (int slot = 0; slot < CLUSTER_SLOTS; slot++)
            if (isSlotInTrimJob(slot)) nofork.trimmed_slots[slot / 8] |= 1 << (slot % 8);
    }

    rio prefix, suffix;
    if (forklessInitBuffer(&prefix) != C_OK) goto fail;
    int ok = rdbSaveSnapshotPreamble(&prefix, req, flags, rsi) == C_OK;
    nofork.prefix = prefix.io.buffer.ptr;
    if (!ok) goto fail;

    /* Modules may have AUX state shared with key callbacks. Capture their
     * keys between BEFORE and AFTER, all on main at the snapshot boundary.
     * Built-ins remain incremental; this pass builds no all-keys index.
     * Even callbacks without AUX may read mutable module-global state.
     * Skip the pass only when no module payloads or metadata classes exist;
     * registration of the built-in vector-set module alone is insufficient.
     * Detached payloads and unused/released classes conservatively scan. */
    if (!(req & SLAVE_REQ_RDB_EXCLUDE_DATA) &&
        (moduleObjectCount() || keyMetaHasModuleClasses())) {
        for (int i = 0; i < nofork.dbnum; i++) {
            kvstoreIterator it;
            kvstoreIteratorInit(&it, nofork.databases[i]);
            dictEntry *de;
            while ((de = kvstoreIteratorNext(&it))) {
                kvobj *kv = dictGetKV(de);
                if (kv->type == OBJ_MODULE || getModuleMetaBits(kvobjMetaBits(kv))) noforkCaptureKey(kv, i);
                if (noforkCancelled()) break;
            }
            kvstoreIteratorReset(&it);
            if (noforkCancelled()) goto fail;
        }
    }
    if (forklessInitBuffer(&suffix) != C_OK) goto fail;
    ok = (req & SLAVE_REQ_RDB_EXCLUDE_DATA) || rdbSaveModulesAux(&suffix, REDISMODULE_AUX_AFTER_RDB) != -1;
    nofork.suffix = suffix.io.buffer.ptr;
    if (!ok) goto fail;
    if (req & SLAVE_REQ_RDB_EXCLUDE_DATA) nofork.scanning_done = 1;
    if (anetPipe(nofork.wake, O_NONBLOCK, O_NONBLOCK) == -1) goto fail;
    if (aeCreateFileEvent(server.el, nofork.wake[0], AE_READABLE, noforkWakeHandler, NULL) == AE_ERR) goto fail;
    if (nofork.socket) {
        getRandomHexChars(nofork.eofmark, sizeof(nofork.eofmark));
        int current = fcntl(fd, F_GETFL);
        if (current == -1 || fcntl(fd, F_SETFL, current | O_NONBLOCK) == -1) goto fail;
    } else {
        snprintf(nofork.filename, sizeof(nofork.filename), "%s", filename);
        snprintf(nofork.tmp, sizeof(nofork.tmp), "temp-nofork-%d.rdb", (int)getpid());
        nofork.fd = open(nofork.tmp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (nofork.fd == -1) goto fail;
    }
    if (pthread_create(&nofork.tid, NULL, noforkWorker, NULL)) {
        if (!nofork.socket) close(nofork.fd);
        goto fail;
    }
    nofork.started = 1;
    /* Real PID remains -1. child_type is the existing persistence-operation
     * discriminator used by replication, INFO, SAVE and shutdown. */
    server.child_type = CHILD_TYPE_RDB;
    server.rdb_child_type = nofork.socket ? RDB_CHILD_TYPE_SOCKET : RDB_CHILD_TYPE_DISK;
    server.rdb_save_time_start = time(NULL);
    server.stat_rdb_cow_bytes = 0;
    server.stat_current_save_keys_processed = 0;
    server.stat_current_save_keys_total = nofork.data ? dbTotalServerKeyCount() : 0;
    serverLog(LL_NOTICE, "Background saving started without fork (parent serializer)");
    return C_OK;
fail:
    noforkFreeState();
    forkless_marks_valid = 0;
    forkless_save_active = forkless_split_save = 0;
    server.active_defrag_paused--;
    server.lastbgsave_status = C_ERR;
    stopSaving(0);
    return C_ERR;
}

/* Main-thread completion, after worker exit. Never free worker references on
 * cancellation. Diskless success also waits for the parent relay's EOF. */
int forklessNoForkCron(void) {
    noforkReleaseRetired();
    if (!nofork.active) return nofork_retired != NULL && !lazyfreeHasPendingDbJobs();
    pthread_mutex_lock(&forkless_mutex);
    noforkRecord *done = nofork.done;
    noforkRecord *done_tail = nofork.done_tail;
    nofork.done = nofork.done_tail = NULL;
    nofork.outstanding -= nofork.done_count;
    server.stat_current_save_keys_processed += nofork.done_count;
    nofork.done_count = 0;
    size_t outstanding = nofork.outstanding;
    pthread_mutex_unlock(&forkless_mutex);
    noforkRetireRecords(done, done_tail);
    if (!noforkCancelled() && !nofork.scanning_done && outstanding < FORKLESS_WORK_BATCH_SIZE) {
        monotime start = getMonotonicUs();
        for (int n = 0; n < 64; n++) {
            nofork.cursor = kvstoreScan(nofork.databases[nofork.dbid], nofork.cursor, -1, noforkScan, NULL, NULL);
            if (!nofork.cursor && ++nofork.dbid == nofork.dbnum) {
                pthread_mutex_lock(&forkless_mutex);
                nofork.scanning_done = 1;
                pthread_cond_broadcast(&nofork_work);
                pthread_mutex_unlock(&forkless_mutex);
                break;
            }
            if (noforkCancelled() || getMonotonicUs() - start >= 1000) break;
        }
    }
    int exited;
    atomicGet(nofork.exited, exited);
    if (!exited) {
        pthread_mutex_lock(&forkless_mutex);
        int runnable = !noforkCancelled() && !nofork.scanning_done && nofork.outstanding < FORKLESS_WORK_BATCH_SIZE;
        pthread_mutex_unlock(&forkless_mutex);
        return runnable || (nofork_retired != NULL && !lazyfreeHasPendingDbJobs());
    }
    int ok = !noforkCancelled();
    if (ok && nofork.socket && server.rdb_child_exit_pipe != -1)
        return nofork_retired != NULL && !lazyfreeHasPendingDbJobs();
    pthread_join(nofork.tid, NULL);
    if (ok && !nofork.socket)
        ok = rename(nofork.tmp, nofork.filename) == 0 && fsyncFileDir(nofork.filename) == 0;
    /* Remove pending socket write callbacks before the standard completion
     * handler frees the relay buffers on cancellation. */
    if (!ok && nofork.socket) {
        for (int i = 0; i < server.rdb_pipe_numconns; i++)
            if (server.rdb_pipe_conns[i]) connSetWriteHandler(server.rdb_pipe_conns[i], NULL);
    }
    noforkFreeState();
    forkless_round_succeeded = ok && nofork.data;
    forklessSaveCleanup();
    server.child_type = CHILD_TYPE_NONE;
    server.stat_current_save_keys_processed = 0;
    server.stat_current_save_keys_total = 0;
    stopSaving(ok);
    backgroundSaveDoneHandler(ok ? 0 : 1, 0);
    updateDictResizePolicy();
    return nofork_retired != NULL && !lazyfreeHasPendingDbJobs();
}

#ifdef REDIS_TEST
#include "testhelp.h"

typedef struct forklessTestFrameWriter {
    int fd;
    const char *bytes;
    size_t len;
    size_t chunk;
} forklessTestFrameWriter;

static void *forklessTestWriteFrames(void *arg) {
    forklessTestFrameWriter *writer = arg;
    size_t pos = 0;
    while (pos < writer->len) {
        size_t take = writer->len - pos;
        if (take > writer->chunk) take = writer->chunk;
        ssize_t n = write(writer->fd, writer->bytes + pos, take);
        if (n > 0) pos += n;
        else if (n == -1 && errno == EINTR) continue;
        else break;
    }
    close(writer->fd);
    return NULL;
}

/* Exercise the real relay, including frames split across pipe reads. */
static int forklessTestRelay(const void *bytes, size_t len, size_t chunk, sds expected, int status) {
    serverAssert(pipe(forkless_data_pipe) == 0);
    rio output;
    rioInitWithBuffer(&output, sdsempty());
    forklessChildSetOutput(&output);
    forklessTestFrameWriter writer = {forkless_data_pipe[1], bytes, len, chunk};
    pthread_t tid;
    serverAssert(pthread_create(&tid, NULL, forklessTestWriteFrames, &writer) == 0);
    int result;
    do {
        result = forklessChildRelayFramesLimited(32);
    } while (result == C_OK && forkless_data_pipe[0] != -1);
    pthread_join(tid, NULL);
    forklessCloseFd(&forkless_data_pipe[0]);
    forkless_data_pipe[1] = -1; /* Closed by writer. */
    forkless_child_rio = NULL;
    int ok = result == status && (!expected ||
        (sdslen(expected) == sdslen(output.io.buffer.ptr) &&
         !memcmp(expected, output.io.buffer.ptr, sdslen(expected))));
    sdsfree(output.io.buffer.ptr);
    return ok;
}

static void forklessTestBufferedFrames(void) {
    int saved_dbnum = server.dbnum;
    server.dbnum = 16;
    sds frames = sdsempty();
    rio expected;
    rioInitWithBuffer(&expected, sdsempty());
    char payload[100000];
    memset(payload, 'x', sizeof(payload));
    for (int i = 0; i < 10000; i++) {
        uint64_t header[2] = {i == 5000 ? sizeof(payload) : 7, i % 16};
        frames = sdscatlen(frames, header, sizeof(header));
        frames = sdscatlen(frames, payload, header[0]);
        rdbSaveType(&expected, RDB_OPCODE_SELECTDB);
        rdbSaveLen(&expected, header[1]);
        rioWrite(&expected, payload, header[0]);
    }
    test_cond("Bounded relay resumes prefetched frames, large payloads and DB selection",
              forklessTestRelay(frames, sdslen(frames), 65536, expected.io.buffer.ptr, C_OK));
    test_cond("Buffered relay preserves fragmented headers and payloads",
              forklessTestRelay(frames, sdslen(frames), 7, expected.io.buffer.ptr, C_OK));
    serverAssert(pipe(forkless_data_pipe) == 0);
    rio bounded;
    rioInitWithBuffer(&bounded, sdsempty());
    forklessChildSetOutput(&bounded);
    /* Three 23-byte frames fit in one read. Stop after one even though the
     * reader has already prefetched the other two. */
    serverAssert(write(forkless_data_pipe[1], frames, 69) == 69);
    test_cond("Relay yields exactly at its frame budget with buffered input",
              forklessChildRelayFramesLimited(1) == C_OK &&
              sdslen(bounded.io.buffer.ptr) == 9 &&
              forkless_child_reader.pos < forkless_child_reader.end);
    forklessCloseFd(&forkless_data_pipe[1]);
    test_cond("Relay resumes buffered input without losing or duplicating records",
              forklessChildRelayFrames() == C_OK &&
              sdslen(bounded.io.buffer.ptr) == 27 &&
              !memcmp(bounded.io.buffer.ptr, expected.io.buffer.ptr, 27));
    forkless_child_rio = NULL;
    sdsfree(bounded.io.buffer.ptr);
    sdsfree(frames);
    sdsfree(expected.io.buffer.ptr);
    uint64_t header[2] = {10, 0};
    test_cond("Buffered relay rejects EOF inside a header",
              forklessTestRelay(header, 3, 3, NULL, C_ERR));
    test_cond("Buffered relay rejects EOF inside a payload",
              forklessTestRelay(header, sizeof(header), sizeof(header), NULL, C_ERR));
    header[1] = server.dbnum;
    test_cond("Buffered relay rejects invalid database IDs",
              forklessTestRelay(header, sizeof(header), sizeof(header), NULL, C_ERR));
    serverAssert(pipe(forkless_data_pipe) == 0);
    int flags = fcntl(forkless_data_pipe[0], F_GETFL, 0);
    serverAssert(fcntl(forkless_data_pipe[0], F_SETFL, flags | O_NONBLOCK) == 0);
    rio output;
    rioInitWithBuffer(&output, sdsempty());
    forklessChildSetOutput(&output);
    test_cond("Buffered relay yields on an empty nonblocking pipe without closing it",
              forklessChildRelayFrames() == C_OK && forkless_data_pipe[0] != -1 &&
              sdslen(output.io.buffer.ptr) == 0);
    forklessClosePipePair(forkless_data_pipe);
    forkless_child_rio = NULL;
    sdsfree(output.io.buffer.ptr);
    server.dbnum = saved_dbnum;
}

/* Single-threaded tests still honor the insertion helper's lock contract. */
static forklessPin *forklessTestPinAdd(kvobj *kv) {
    pthread_mutex_lock(&forkless_mutex);
    forklessPin *pin = forklessPinGetOrAddLocked(kv);
    pthread_mutex_unlock(&forkless_mutex);
    return pin;
}

static void forklessTestEarlyPins(void) {
    kvobj keys[FORKLESS_WORK_BATCH_SIZE + 1] = {{0}};
    forklessWorkRec work;
    int saved_marks = forkless_marks_valid;
    forkless_marks_valid = 1;
    serverAssert(anetPipe(forkless_early_wake, O_NONBLOCK, O_NONBLOCK) == 0);
    for (size_t i = 0; i <= FORKLESS_WORK_BATCH_SIZE; i++) {
        forklessPin *p = forklessTestPinAdd(&keys[i]);
        p->state = FORKLESS_PIN_OBJECT;
        pthread_mutex_lock(&forkless_mutex);
        forklessQueueEarlyLocked(p, 7);
        pthread_mutex_unlock(&forkless_mutex);
    }
    test_cond("Early queue stays bounded and overflow falls back to child work",
              forkless_early_count == FORKLESS_WORK_BATCH_SIZE &&
              !forklessPinFindLocked(&keys[FORKLESS_WORK_BATCH_SIZE])->early_pending);
    pthread_mutex_lock(&forkless_mutex);
    test_cond("Early dequeue preserves identity and original database",
              forklessTakeEarlyLocked(&work) && work.kv == &keys[0] && work.dbid == 7);
    forklessPin *p = forklessPinFindLocked(&keys[0]);
    p->state = FORKLESS_PIN_WRITTEN;
    forklessPinRemoveWrittenLocked(&keys[0]);
    test_cond("Early tombstone survives until child confirmation",
              forklessPinFindLocked(&keys[0]) == p);
    p->awaiting_child = 0;
    forklessPinRemoveWrittenLocked(&keys[0]);
    test_cond("Child confirmation retires an early tombstone",
              !forklessPinFindLocked(&keys[0]));
    p = forklessPinFindLocked(&keys[1]);
    p->state = FORKLESS_PIN_WRITTEN;
    p->awaiting_child = 0;
    forklessPinRemoveWrittenLocked(&keys[1]);
    test_cond("Child-first completion keeps identity while still queued",
              forklessPinFindLocked(&keys[1]) == p);
    test_cond("Dequeuing child-first completion skips output and retires it",
              forklessTakeEarlyLocked(&work) && work.kv == &keys[2] &&
              !forklessPinFindLocked(&keys[1]));
    pthread_mutex_unlock(&forkless_mutex);
    forklessRetirePins();
    forklessClosePipePair(forkless_early_wake);
    while (forkless_retired_pins) {
        forklessResetCleanupBudget();
        forklessReleaseDonePins();
    }
    forkless_marks_valid = saved_marks;
    test_cond("Early queue cleanup releases all tracking charges", forkless_memory_bytes == 0);
}

static void forklessTestScheduling(void) {
    kvobj keys[FORKLESS_WORK_BATCH_SIZE * 2] = {{0}};
    forklessWorkRec batch[FORKLESS_WORK_BATCH_SIZE], work;
    serverAssert(anetPipe(forkless_early_wake, O_NONBLOCK, O_NONBLOCK) == 0);
    for (size_t i = 0; i < FORKLESS_WORK_BATCH_SIZE; i++) {
        forklessPin *pin = forklessTestPinAdd(&keys[i]);
        pin->state = FORKLESS_PIN_OBJECT;
        pthread_mutex_lock(&forkless_mutex);
        forklessQueueEarlyLocked(pin, 0);
        pthread_mutex_unlock(&forkless_mutex);
        batch[i] = (forklessWorkRec){&keys[FORKLESS_WORK_BATCH_SIZE + i], 0};
    }
    pthread_mutex_lock(&forkless_mutex);
    forklessPublishReadyLocked(batch, FORKLESS_WORK_BATCH_SIZE);
    int from_child, correct = 1;
    size_t children = 0, early = 0;
    for (size_t i = 0; i < FORKLESS_WORK_BATCH_SIZE * 2; i++) {
        if (!forklessTakeScheduledLocked(&work, &from_child)) {correct = 0; break;}
        if (from_child) {
            if (work.kv != &keys[FORKLESS_WORK_BATCH_SIZE + children++]) correct = 0;
        } else {
            if (work.kv != &keys[early++]) correct = 0;
        }
        if (i == 31)
            test_cond("Scheduler starts with 32 child records", children == 32 && early == 0);
        if (i == 95)
            test_cond("Full early queue receives 64 records before returning to child",
                      children == 32 && early == 64);
        if (i == 255)
            test_cond("Mixed turn retains unfinished child window", forkless_ready_remaining > 0);
    }
    test_cond("Scheduler drains both sources exactly once without reordering them",
              correct && children == FORKLESS_WORK_BATCH_SIZE && early == FORKLESS_WORK_BATCH_SIZE);
    test_cond("Scheduler reports no work after both queues drain",
              !forklessTakeScheduledLocked(&work, &from_child) && !forkless_ready_remaining);
    forklessPublishReadyLocked(batch, 2);
    test_cond("An empty early queue immediately donates its turn to child work",
              forklessTakeScheduledLocked(&work, &from_child) && from_child &&
              forklessTakeScheduledLocked(&work, &from_child) && from_child);
    pthread_mutex_unlock(&forkless_mutex);
    forklessRetirePins();
    forklessClosePipePair(forkless_early_wake);
    while (forkless_retired_pins) {
        forklessResetCleanupBudget();
        forklessReleaseDonePins();
    }
    test_cond("Scheduling cleanup releases all tracking charges", forkless_memory_bytes == 0);
}

int forklessTest(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);
    forklessTestBufferedFrames();
    forkless_memory_limit = 64;
    test_cond("Snapshot budget admits exactly its limit", forklessMemoryReserve(64));
    test_cond("Snapshot budget rejects overflow before allocation", !forklessMemoryReserve(1));
    forklessMemoryRelease(64);
    test_cond("Denied tracking allocation leaves no charge",
              forklessTrackingAlloc(65) == NULL && forkless_memory_bytes == 0);
    forkless_memory_limit = 4096;
    rio bounded;
    test_cond("Budgeted encoding buffer starts", forklessInitBuffer(&bounded) == C_OK);
    char payload[4096] = {0};
    test_cond("Budgeted buffer accepts small writes", rioWrite(&bounded, payload, 100));
    test_cond("Budgeted buffer refuses growth before allocating",
              !rioWrite(&bounded, payload, sizeof(payload)) && forkless_memory_bytes <= 4096);
    forklessBufferFree(bounded.io.buffer.ptr);
    test_cond("Encoding failure releases all buffer charges", forkless_memory_bytes == 0);
    test_cond("Small encoding buffer starts", forklessInitBuffer(&bounded) == C_OK);
    for (int i = 0; i < 128; i++) {
        serverAssert(rioWrite(&bounded, payload, 1));
    }
    forklessBufferFree(bounded.io.buffer.ptr);
    test_cond("Small repeated growth releases all capacity charges", forkless_memory_bytes == 0);
    forkless_memory_limit = 0;
    forkless_reuse_buffer = 1;
    serverAssert(forklessInitBuffer(&bounded) == C_OK);
    serverAssert(rioWrite(&bounded, payload, 100));
    sds scratch = bounded.io.buffer.ptr;
    size_t scratch_charge = forkless_memory_bytes;
    forklessRecycleEncoded(scratch);
    test_cond("Serializer scratch retains capacity and its budget charge",
              forkless_encode_scratch == scratch && sdslen(scratch) == 0 &&
              forkless_memory_bytes == scratch_charge);
    serverAssert(forklessInitBuffer(&bounded) == C_OK);
    serverAssert(rioWrite(&bounded, payload, 100));
    forklessRecycleEncoded(bounded.io.buffer.ptr);
    test_cond("Independent captured bytes do not replace cached scratch",
              forkless_encode_scratch == scratch && forkless_memory_bytes == scratch_charge);
    forklessBufferFree(forkless_encode_scratch);
    forkless_encode_scratch = NULL;
    serverAssert(forklessInitBuffer(&bounded) == C_OK);
    for (int i = 0; i < 20; i++) serverAssert(rioWrite(&bounded, payload, sizeof(payload)));
    forklessRecycleEncoded(bounded.io.buffer.ptr);
    test_cond("Oversized serializer scratch is released instead of cached",
              forkless_encode_scratch == NULL && forkless_memory_bytes == 0);
    forkless_reuse_buffer = 0;
    forklessBeginRound(BGSAVE_MODE_HYBRID, 512000);
    test_cond("First split save uses an unused, trusted label",
              forkless_round == 2 && forkless_marks_valid);
    /* Simulate cleanup after an unsuccessful first round. */
    forkless_marks_valid = 0;
    forklessBeginRound(BGSAVE_MODE_HYBRID, 512000);
    test_cond("Failed first save cannot reuse the fresh-round exception",
              forkless_round == 1 && !forkless_marks_valid);
    forkless_marks_valid = 1; /* Successful recovery. */
    forklessBeginRound(BGSAVE_MODE_HYBRID, 512000);
    test_cond("Successful recovery enables later round shortcuts", forkless_marks_valid);
    forklessBeginRound(BGSAVE_MODE_FORKLESS, 512000);
    test_cond("Changing routing after an issued round requires recovery", !forkless_marks_valid);
    forkless_marks_valid = 1;
    forklessBeginRound(BGSAVE_MODE_FORKLESS, 1024);
    test_cond("Changing the size cap after an issued round requires recovery", !forkless_marks_valid);
    /* Snapshot flags occupy no metadata slots and must survive relocation.
     * Exercise every flag combination before the snapshot protocol tests. */
    for (unsigned int mark = 0; mark < 4; mark++) {
        sds name = sdsnew("snapshot-layout");
        kvobj *value = kvobjSet(name, createStringObject("value", 5), 0);
        sdsfree(name);
        test_cond("Fresh kvobj initializes snapshot flags", kvobjSnapshot(value) == 0);
        void *base = kvobjGetAllocPtr(value);
        kvobjSetSnapshot(value, mark);
        test_cond("Snapshot flags do not change metadata allocation",
                  kvobjMetaBits(value) == 0 && getNumMeta(kvobjMetaBits(value)) == 0 &&
                  kvobjGetAllocPtr(value) == base && !getModuleMetaBits(kvobjMetaBits(value)));
        value = kvobjSetExpire(value, 123456);
        test_cond("Expiry relocation preserves snapshot flags and value",
                  kvobjSnapshot(value) == mark && getNumMeta(kvobjMetaBits(value)) == 1 &&
                  kvobjGetExpire(value) == 123456 &&
                  !memcmp(value->ptr, "value", 5));
        decrRefCount(value);
    }
    /* Completion marks must skip allocation, but pins take precedence over
     * marks (especially a retained RAW string that now needs detaching). */
    sds progress_key = sdsnew("progress-test");
    kvobj *progress = kvobjSet(progress_key, createRawStringObject("before", 6), 0);
    sdsfree(progress_key);
    forkless_marks_valid = 1;
    forkless_serializer_kv = progress;
    forklessSerializerReadDone();
    unsigned long long skips_before = forkless_capture_skipped;
    /* Holding the mutex here proves that the completed/unshared path no
     * longer attempts to acquire it. This test runs without a helper. */
    pthread_mutex_lock(&forkless_mutex);
    forklessCaptureKvInternal(progress, 0, 0, NULL);
    pthread_mutex_unlock(&forkless_mutex);
    test_cond("Completed untouched object needs no pin",
              kvobjSnapshot(progress) == forkless_round && !forklessPinFindLocked(progress));
    test_cond("Completed unshared capture bypasses the snapshot mutex",
              forkless_capture_skipped == skips_before + 1);
    forkless_round = forkless_round == 1 ? 2 : 1;
    forklessCaptureKvInternal(progress, 0, 1, NULL);
    test_cond("Previous round completion still needs capturing",
              forklessPinFindLocked(progress) && progress->refcount == 2);
    forklessCaptureKvInternal(progress, 0, 0, NULL);
    test_cond("Current progress mark does not bypass retained-value detachment",
              forklessPinFindLocked(progress)->snapshot != progress && progress->refcount == 1);
    kvobj *private_snapshot = forklessPinFindLocked(progress)->snapshot;
    pthread_mutex_lock(&forkless_mutex);
    forklessCaptureKvInternal(progress, 1, 0, NULL);
    pthread_mutex_unlock(&forkless_mutex);
    test_cond("Private snapshot permits repeated metadata changes without recapture",
              forklessPinFindLocked(progress)->snapshot == private_snapshot);
    forklessRetirePins();
    while (forkless_retired_pins) {
        forklessResetCleanupBudget();
        forklessReleaseDonePins();
    }
    forkless_marks_valid = 0;
    forklessCaptureKvInternal(progress, 0, 0, NULL);
    test_cond("Recovery ignores a current-round mark",
              forklessPinFindLocked(progress) != NULL);
    decrRefCount(progress);
    forklessRetirePins();
    while (forkless_retired_pins) {
        forklessResetCleanupBudget();
        forklessReleaseDonePins();
    }
    kvobj keys[FORKLESS_WORK_BATCH_SIZE + 1];
    forklessWorkRec batch[FORKLESS_WORK_BATCH_SIZE], work;
    for (size_t i = 0; i < FORKLESS_WORK_BATCH_SIZE; i++) {
        batch[i].kv = &keys[i];
        batch[i].dbid = i % 2;
    }
    forklessRetirePins();
    forkless_priority_keys = 0;
    forkless_save_active = 1;
    atomicSet(forkless_capture_enabled, 1);

    /* Capturing a post-fork key must not authorize writing it. */
    forklessPin *pin = forklessTestPinAdd(&keys[FORKLESS_WORK_BATCH_SIZE]);
    pin->state = FORKLESS_PIN_OBJECT;
    forklessPrioritizeEviction(pin->kv);
    test_cond("Eviction without a child record produces no work",
              !forklessTakeReadyLocked(&work));

    pin = forklessTestPinAdd(&keys[200]);
    pin->state = FORKLESS_PIN_OBJECT;
    forklessPrioritizeEviction(pin->kv);
    forklessPublishReadyLocked(batch, FORKLESS_WORK_BATCH_SIZE);
    test_cond("An earlier eviction goes ahead of FIFO work",
              forklessTakeReadyLocked(&work) && work.kv == &keys[200]);

    pin = forklessTestPinAdd(&keys[100]);
    pin->state = FORKLESS_PIN_OBJECT;
    forklessPrioritizeEviction(pin->kv);
    forklessPrioritizeEviction(pin->kv);
    test_cond("Eviction promotes an already queued record, only once",
              forklessTakeReadyLocked(&work) && work.kv == &keys[100]);
    forklessPrioritizeEviction(pin->kv); /* Already taken: do not enqueue again. */

    int ordered = 1;
    for (size_t i = 0; i < FORKLESS_WORK_BATCH_SIZE; i++) {
        if (i == 100 || i == 200) continue;
        if (!forklessTakeReadyLocked(&work) || work.kv != &keys[i] ||
            work.dbid != (intptr_t)(i % 2)) ordered = 0;
    }
    test_cond("All remaining records retain FIFO order and their database", ordered);
    test_cond("Full window drains without duplicates", !forklessTakeReadyLocked(&work));
    test_cond("Only promoted records are counted", forklessPriorityKeys() == 2);
    forklessRetirePins();
    test_cond("Cleanup discards the scheduling window", !forklessTakeReadyLocked(&work));

    /* Exercise a partial window with a null record and no saved priorities. */
    batch[0].kv = NULL;
    forklessPublishReadyLocked(batch, 2);
    test_cond("Partial windows skip null records",
              forklessTakeReadyLocked(&work) && work.kv == &keys[1] &&
              !forklessTakeReadyLocked(&work));
    forklessRetirePins();
    forkless_save_active = 0;
    atomicSet(forkless_capture_enabled, 0);
    /* Exercise shard growth, lookup stability and overlapping cleanup/save
     * generations. Fake objects are only hashed; none owns a reference. */
    size_t nkeys = 65536;
    kvobj *many = zcalloc(nkeys * sizeof(*many));
    for (size_t i = 0; i < nkeys; i++)
        forklessTestPinAdd(&many[i])->expire = i;
    int found = 1;
    for (size_t i = 0; i < nkeys; i++) {
        forklessPin *p = forklessPinFindLocked(&many[i]);
        if (!p || p->expire != (long long)i) found = 0;
    }
    test_cond("Sharded growth preserves every pin", found);
    forkless_marks_valid = 1;
    pthread_mutex_lock(&forkless_mutex);
    for (size_t i = 0; i < nkeys; i += 2) {
        forklessPinFindLocked(&many[i])->state = FORKLESS_PIN_WRITTEN;
        forklessPinRemoveWrittenLocked(&many[i]);
    }
    pthread_mutex_unlock(&forkless_mutex);
    found = forkless_pins->count == nkeys / 2;
    for (size_t i = 0; i < nkeys; i++) {
        forklessPin *p = forklessPinFindLocked(&many[i]);
        if (i % 2 ? (!p || p->expire != (long long)i) : p != NULL) found = 0;
    }
    test_cond("Removing completed pins preserves colliding lookup chains", found);
    for (size_t i = 0; i < nkeys; i += 2)
        forklessTestPinAdd(&many[i])->expire = i;
    test_cond("Reused identities can be inserted after completed pins are removed",
              forkless_pins->count == nkeys);
    forkless_marks_valid = 0;
    pin = forklessPinFindLocked(&many[0]);
    pin->state = FORKLESS_PIN_WRITTEN;
    forklessPinRemoveWrittenLocked(&many[0]);
    test_cond("Recovery keeps completed identity tombstones", forklessPinFindLocked(&many[0]) != NULL);
    forklessRetirePins();
    test_cond("Retirement detaches lookup immediately", !forklessPinFindLocked(&many[0]));
    forklessTestPinAdd(&many[0])->expire = 123;
    forklessResetCleanupBudget();
    forklessReleaseDonePins();
    size_t cleanup_before = forkless_cleanup_slots;
    monotime cleanup_time_before = forkless_cleanup_used;
    forklessReleaseDonePins();
    test_cond("Repeated cleanup shares the event-loop slot/time allowance",
              forkless_cleanup_slots <= FORKLESS_CLEANUP_SLOTS &&
              ((cleanup_before < FORKLESS_CLEANUP_SLOTS &&
                cleanup_time_before < FORKLESS_CLEANUP_USEC) ||
               (forkless_cleanup_slots == cleanup_before &&
                forkless_cleanup_used == cleanup_time_before)));
    while (forkless_retired_pins) {
        forklessResetCleanupBudget();
        forklessReleaseDonePins();
    }
    test_cond("Retired cleanup cannot touch the next generation",
              forklessPinFindLocked(&many[0])->expire == 123);
    forklessRetirePins();
    while (forkless_retired_pins) {
        forklessResetCleanupBudget();
        forklessReleaseDonePins();
    }
    zfree(many);
    test_cond("Retired tracking generations release their admission charges",
              forkless_memory_bytes == 0);
    forklessTestEarlyPins();
    forklessTestScheduling();

    /* Retain a RAW replacement without copying; a later in-place mutation
     * must upgrade that same pin to a private snapshot. */
    sds rawkey = sdsnew("replacement-test");
    robj *raw = createRawStringObject("original", 8);
    kvobj *original = kvobjSet(rawkey, raw, 0);
    sdsfree(rawkey);
    forklessCaptureKvInternal(original, 0, 1, NULL);
    pin = forklessPinFindLocked(original);
    test_cond("Replacement retains RAW string instead of copying",
              pin && pin->snapshot == original && original->refcount == 2);
    forklessCaptureKvInternal(original, 0, 0, NULL);
    pin = forklessPinFindLocked(original);
    test_cond("In-place capture detaches a previously retained RAW string",
              pin && pin->snapshot != original && original->refcount == 1 &&
              !memcmp(pin->snapshot->ptr, "original", 8));
    ((char *)original->ptr)[0] = 'X';
    test_cond("Detached snapshot survives live RAW mutation",
              !memcmp(pin->snapshot->ptr, "original", 8));
    decrRefCount(original);
    forklessRetirePins();
    while (forkless_retired_pins) {
        forklessResetCleanupBudget();
        forklessReleaseDonePins();
    }
    long long idle;
    int frequency;
    forkless_eviction.policy = MAXMEMORY_ALLKEYS_LFU;
    forkless_eviction.lfu_clock = 100;
    forkless_eviction.lfu_decay_time = 2;
    forklessEvictionMetadata((90 << 8) | 100, &idle, &frequency);
    test_cond("Snapshot LFU uses frozen policy, clock and decay", idle == -1 && frequency == 95);
    forklessEvictionMetadata((90 << 8) | 3, &idle, &frequency);
    test_cond("Snapshot LFU decay stops at zero", frequency == 0);
    forkless_eviction.lfu_decay_time = 0;
    forklessEvictionMetadata((90 << 8) | 100, &idle, &frequency);
    test_cond("Zero LFU decay preserves the counter", frequency == 100);
    forkless_eviction.lfu_decay_time = 1;
    forkless_eviction.lfu_clock = 2;
    forklessEvictionMetadata((65533U << 8) | 100, &idle, &frequency);
    test_cond("Snapshot LFU matches the clock wrap arithmetic", frequency == 96);
    forkless_eviction.policy = MAXMEMORY_ALLKEYS_LRU;
    forkless_eviction.lru_clock = 1000;
    forklessEvictionMetadata(400, &idle, &frequency);
    test_cond("Snapshot LRU preserves idle time", idle == 600 && frequency == -1);
    forkless_eviction.policy = MAXMEMORY_ALLKEYS_RANDOM;
    forklessEvictionMetadata(400, &idle, &frequency);
    test_cond("Other policies omit eviction metadata", idle == -1 && frequency == -1);
    test_report();
    return 0;
}
#endif
