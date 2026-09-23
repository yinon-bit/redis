/* Linux test-only fwrite fault gate. Loaded only by the after_failure fixture.
 * Never use with a production server. The controller arms it for one save,
 * waits for gate.arrived, then releases gate.release. All timed saves are disarmed.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>

static size_t (*real_fwrite)(const void *,size_t,size_t,FILE *);
static pthread_mutex_t guard = PTHREAD_MUTEX_INITIALIZER;
static unsigned long long written;
static char root[PATH_MAX];
static int marker(const char *name, int create) {
    char p[PATH_MAX];
    if (snprintf(p,sizeof(p),"%s/gate.%s",root,name)>=(int)sizeof(p)) return -1;
    if (!create) return access(p,F_OK)==0;
    int fd=open(p,O_WRONLY|O_CREAT|O_EXCL,0600);
    if(fd<0) return -1;
    close(fd); return 0;
}
__attribute__((constructor)) static void init(void) {
    real_fwrite=dlsym(RTLD_NEXT,"fwrite");
    const char *dir=getenv("BENCH_EIO_DIR");
    if(dir) snprintf(root,sizeof(root),"%s",dir);
}
size_t fwrite(const void *ptr,size_t size,size_t count,FILE *stream) {
    if(!root[0] || !marker("armed",0) || marker("injected",0) || marker("passed",0))
        return real_fwrite(ptr,size,count,stream);
    char fdpath[64],target[PATH_MAX];
    snprintf(fdpath,sizeof(fdpath),"/proc/self/fd/%d",fileno(stream));
    ssize_t len=readlink(fdpath,target,sizeof(target)-1);
    if(len<0) return real_fwrite(ptr,size,count,stream);
    target[len]=0;
    size_t n=strlen(root);
    if(strncmp(target,root,n) || strncmp(target+n,"/temp-",6) || !strstr(target,".rdb"))
        return real_fwrite(ptr,size,count,stream);
    pthread_mutex_lock(&guard);
    if(written>=1048576 && !marker("injected",0)) {
        marker("arrived",1);
        struct timespec t; clock_gettime(CLOCK_MONOTONIC,&t);
        time_t deadline=t.tv_sec+30;
        while(!marker("release",0)) {
            clock_gettime(CLOCK_MONOTONIC,&t);
            if(t.tv_sec>=deadline) { marker("timeout",1); break; }
            usleep(1000);
        }
        const char *mode=getenv("BENCH_EIO_MODE");
        if(mode && !strcmp(mode,"pause")) {
            marker("passed",1);
            pthread_mutex_unlock(&guard);
            return real_fwrite(ptr,size,count,stream);
        }
        int first=marker("injected",1)==0;
        pthread_mutex_unlock(&guard);
        if(first) { errno=EIO; return 0; }
        return real_fwrite(ptr,size,count,stream);
    }
    size_t done=real_fwrite(ptr,size,count,stream);
    written+=done*size;
    pthread_mutex_unlock(&guard);
    return done;
}
