#!/usr/bin/env python3
"""Compile the production arena/guard functions with fault-injected syscalls.

Run with python3 tests/unit/forkless-arena-safety.py. No Redis build or Linux
host is required: the harness models a madvise failure after partial progress.
All generated files live in a temporary directory.
"""
import os
from pathlib import Path
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[2]
source = (ROOT / "src/forkless.c").read_text()
server = (ROOT / "src/server.c").read_text()


def section(start, end):
    return source[source.index(start):source.index(end, source.index(start))]


functions = section("static int forklessApplyAdvice(", "static void *forklessExtentAlloc(")
functions += section("static int forklessSetNoforkForkable(", "static int forklessInstallDontforkHooks(")
functions += section("int forklessNonRdbForkAllowed(", "const char *forklessChildRdbTmpfile(")
guard = server[server.index("int redisFork(int purpose) {"):]
guard = guard[:guard.index("    if (isMutuallyExclusiveChildType(purpose))")]
functions += guard + "    return 42;\n}\n"

PREAMBLE = r'''
#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define C_OK 0
#define C_ERR -1
#define LL_WARNING 1
#define UNUSED(x) (void)(x)
#define MADV_DOFORK 11
#define MADV_DONTFORK 10
#define CHILD_TYPE_RDB 1
#define CHILD_TYPE_MODULE 2
#define CHILD_TYPE_LDB 3
#define CHILD_TYPE_AOF 4
#define FORKLESS_MAX_EXTENTS 8
typedef struct { void *addr; size_t size; } forklessExtent;
static forklessExtent forkless_extents[FORKLESS_MAX_EXTENTS];
static size_t forkless_extent_count;
static int forkless_extents_overflowed;
static int forkless_nofork_forkable = 1;
static pthread_mutex_t forkless_extent_mutex = PTHREAD_MUTEX_INITIALIZER;
static int arenas_ready = 1;
static int pages[8], calls;
static unsigned fail_calls;
/* Deliberately clobber errno: production must preserve the syscall error. */
#define serverLog(...) ((void)(errno = EBADF))
#define serverPanic(...) abort()
static void zmallocNoTcacheWindowAdd(void *p, size_t n) { (void)p; (void)n; }
static int forklessArenasReady(void) { return arenas_ready; }
static int forklessNoforkIsForkable(void) { return forkless_nofork_forkable; }
static int madvise(void *addr, size_t size, int advice) {
    (void)size;
    /* Even a failed call may have applied advice to part of its range. */
    *(int *)addr = advice == MADV_DOFORK;
    calls++;
    if (fail_calls & (1U << calls)) { errno = EIO; return -1; }
    return 0;
}
'''

TESTS = r'''
static void reset(int forkable) {
    forkless_nofork_forkable = forkable;
    forkless_extent_count = 3;
    forkless_extents_overflowed = 0;
    calls = 0;
    fail_calls = 0;
    for (int i = 0; i < 3; i++) {
        pages[i] = forkable;
        forkless_extents[i] = (forklessExtent){&pages[i], sizeof(pages[i])};
    }
}
int main(int argc, char **argv) {
#ifdef __linux__
    if (argc > 1 && !strcmp(argv[1], "rollback-failure")) {
        reset(1);
        fail_calls = (1U << 2) | (1U << 3);
        forklessSetNoforkForkable(0);
        return 99; /* Must terminate, not return with inconsistent visibility. */
    }
    reset(0);
    fail_calls = 1U << 1;
    forklessExtentRegister(&pages[3], sizeof(pages[3]));
    assert(forkless_extent_count == 4 && calls == 1);
    /* An inherited new extent is safe, and must remain tracked for DOFORK. */
    fail_calls = 0;
    assert(forklessSetNoforkForkable(1) == C_OK);
    for (int i = 0; i < 4; i++) assert(pages[i] == 1);
    for (int original = 0; original <= 1; original++) {
        reset(original);
        fail_calls = 1U << 2;
        assert(forklessSetNoforkForkable(!original) == C_ERR);
        assert(errno == EIO);
        assert(forkless_nofork_forkable == original);
        assert(calls == 4); /* Two attempts, failed range + earlier range undone. */
        for (int i = 0; i < 3; i++) assert(pages[i] == original);
        /* The failure path also releases the mutex and allows a retry. */
        fail_calls = 0;
        assert(forklessSetNoforkForkable(!original) == C_OK);
        for (int i = 0; i < 3; i++) assert(pages[i] == !original);
        assert(forkless_nofork_forkable == !original);
    }
#else
    (void)argc; (void)argv;
    reset(1);
    assert(forklessSetNoforkForkable(0) == C_OK);
    assert(calls == 0);
#endif
    for (int original = 0; original <= 1; original++) {
        reset(original);
        forkless_extents_overflowed = 1;
        assert(forklessSetNoforkForkable(!original) == C_ERR);
        assert(errno == ENOSPC && calls == 0);
        assert(forkless_nofork_forkable == original);
        /* CONFIG SET rollback must accept an unchanged applied state. */
        assert(forklessSetNoforkForkable(original) == C_OK);
    }
    for (int ready = 0; ready <= 1; ready++) {
        arenas_ready = ready;
        for (int visible = 0; visible <= 1; visible++) {
            forkless_nofork_forkable = visible;
            assert(redisFork(CHILD_TYPE_RDB) == 42);
            for (int purpose = CHILD_TYPE_MODULE; purpose <= CHILD_TYPE_AOF; purpose++) {
#if defined(USE_JEMALLOC) && defined(__linux__)
                int denied = ready && !visible;
#else
                int denied = 0;
#endif
                assert(redisFork(purpose) == (denied ? -1 : 42));
                if (denied) assert(errno == ENOTSUP);
            }
        }
    }
    puts("arena rollback and fork guard tests passed");
    return 0;
}
'''


with tempfile.TemporaryDirectory(prefix="redis-arena-safety-") as directory:
    path = Path(directory)
    harness = path / "test.c"
    harness.write_text(PREAMBLE + functions + TESTS)
    for name, flags in (
        ("linux-jemalloc", ["-D__linux__", "-DUSE_JEMALLOC"]),
        ("nonlinux-jemalloc", ["-U__linux__", "-DUSE_JEMALLOC"]),
        ("no-jemalloc-guard", ["-U__linux__", "-UUSE_JEMALLOC"]),
    ):
        binary = path / name
        subprocess.run([os.environ.get("CC", "cc"), "-std=c11", "-pthread",
                        *flags, str(harness), "-o", str(binary)], check=True)
        subprocess.run([str(binary)], check=True)
        if name == "linux-jemalloc":
            for failure in ("rollback-failure",):
                result = subprocess.run([str(binary), failure], cwd=path)
                assert result.returncode == -6, (failure, result.returncode)
    print("PASS: both transition directions, partial failure, fatal rollback, "
          "new extent failure, registry overflow, and fork guards")
