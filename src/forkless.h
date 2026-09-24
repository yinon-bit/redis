#ifndef __FORKLESS_H
#define __FORKLESS_H

#include <stdint.h>
#include "sds.h"

struct redisDb;
struct redisObject;
struct dictEntry;
struct _rio;
struct rdbSaveInfo;
typedef struct redisObject robj;
typedef struct redisObject kvobj;
typedef struct dictEntry **dictEntryLink;
typedef struct _rio rio;

/* Configuration and allocation. */
int forklessInit(void);
int forklessEnabled(void);
int forklessArenasReady(void);
int forklessSplitSaveActive(void);
int forklessNoForkActive(void);
int forklessNoForkStart(int req, const char *filename, struct rdbSaveInfo *rsi, int flags, int fd);
int forklessNoForkCron(void);
void forklessHybridCron(void);
int forklessHybridCleanupPending(void);
void forklessNoForkCancel(void);
int forklessApplyModeChange(int newmode, const char **err);
const char *bgsaveModeName(void);
unsigned int forklessArenaIndex(void);
unsigned int forklessForkArenaIndex(void);
int forklessNoforkIsForkable(void);
int forklessNonRdbForkAllowed(void);
kvobj *forklessCreateKvobj(sds key, robj *val, uint32_t metabits);
kvobj *forklessMoveKvobjToDefault(kvobj *kv);

/* Value routing and mutation capture. */
int forklessMigrateIfNeeded(struct redisDb *db, robj *key, kvobj **kvp,
                            dictEntryLink link);
void forklessFinalizeDbValue(struct redisDb *db, robj *key, kvobj **kvp,
                             dictEntryLink link);
kvobj *forklessFinalizeHashValue(struct redisDb *db, robj *key, kvobj *kv);
void forklessRetagLink(dictEntryLink link, kvobj *kv);
void forklessCaptureFromLink(struct redisDb *db, dictEntryLink link);
/* May retain RAW strings; any subsequent in-place mutation must use ordinary capture. */
void forklessCaptureForUnlink(struct redisDb *db, dictEntryLink link);
void forklessCaptureMetadata(struct redisDb *db, kvobj *kv);
void forklessEvictionMetadata(unsigned int lru, long long *idle_seconds, int *frequency);
void forklessPrioritizeEviction(kvobj *kv);
unsigned long long forklessPriorityKeys(void);
sds forklessAppendInfo(sds info);
/* Diagnostic elapsed time; child encoding includes synchronous output. */
unsigned long long forklessTimingStart(void);
void forklessTimingChildEncode(unsigned long long start);
/* Returns nonzero while detached cleanup work needs another event-loop batch. */
int forklessReleaseDonePins(void);
void forklessResetCleanupBudget(void);

#ifdef REDIS_TEST
int forklessTest(int argc, char **argv, int flags);
#endif

/* Save lifecycle. */
int forklessSaveBegin(const char *rdb_tmpfile);
int forklessStartDiskSerializer(void);
int forklessStartSocketSerializer(void);
void forklessSaveCleanup(void);
void forklessSaveComplete(void);
void forklessAbortBeforeFlush(void);

/* Child work and single-writer output. */
int forklessEmitWork(kvobj *kv, int dbid);
int forklessChildPumpWork(void);
void forklessChildSetOutput(rio *rdb);
int forklessChildFinishStream(rio *rdb);
const char *forklessChildRdbTmpfile(void);

/* Parent completion. */
int forklessParentInstallRdb(const char *child_tmpfile, const char *filename);

#endif
