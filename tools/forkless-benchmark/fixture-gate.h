/* TEST BUILD ONLY: filesystem gates controlled independently of Redis commands.
 * Never include this header in measured performance binaries. */
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
static void benchFixtureGate(const char *position) {
    const char *root=getenv("BENCH_FIXTURE_DIR");
    const char *selected=getenv("BENCH_FIXTURE_POSITION");
    if (!root || !selected || strcmp(selected,position)) return;
    char armed[1024],arrived[1024],release[1024],expired[1024];
    snprintf(armed,sizeof(armed),"%s/fixture.armed",root);
    snprintf(arrived,sizeof(arrived),"%s/fixture.arrived",root);
    snprintf(release,sizeof(release),"%s/fixture.release",root);
    snprintf(expired,sizeof(expired),"%s/fixture.timeout",root);
    if (access(armed,F_OK)) return;
    int fd=open(arrived,O_WRONLY|O_CREAT|O_EXCL,0600);
    if (fd<0) return; /* One designated gate per attempt, across parent/child. */
    close(fd);
    struct timespec now; clock_gettime(CLOCK_MONOTONIC,&now);
    time_t until=now.tv_sec+30;
    while(access(release,F_OK)) {
        clock_gettime(CLOCK_MONOTONIC,&now);
        if(now.tv_sec>=until) {
            fd=open(expired,O_WRONLY|O_CREAT,0600); if(fd>=0)close(fd);
            break;
        }
        usleep(1000);
    }
}

static int benchFixtureTarget(const char *key) {
    return key && !strcmp(key,"transition:000000");
}
