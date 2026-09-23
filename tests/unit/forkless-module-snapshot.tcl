# Run after building tests/modules/testrdb.so. Both configurations save AUX
# BEFORE and AFTER: 6 uses aux_save, 7 uses aux_save2.
# True forkless saving must capture module values and both AUX phases on the
# main thread before commands resume. No module callback should run in a child.
# testrdb has no digest callback, so compare exact values instead of DEBUG DIGEST.
set testmodule [file normalize tests/modules/testrdb.so]

foreach aux_mode {6 7} {
    start_server [list tags {forkless bgsave modules external:skip} overrides \
        [list save {} bgsave-mode forkless loadmodule "$testmodule $aux_mode"]] {
        test "forkless module disk snapshot freezes values and both AUX phases (args $aux_mode)" {
            r select 0
            set seed {}
            for {set i 0} {$i < 2000} {incr i} {
                lappend seed filler:$i original:$i
            }
            r mset {*}$seed
            r set string original-string
            r set counter 41
            r rpush list first second
            r hset hash field original-hash
            r sadd set first second
            r zadd zset 1 first 2 second
            r xadd stream 1-0 field original-stream
            r testrdb.set.key modulekey original-module
            r testrdb.set.before original-before
            r testrdb.set.after original-after
            set forks [status r total_forks]
            r config set rdb-key-save-delay 1000

            assert_match {*Background saving started*} [r bgsave]
            assert_equal 1 [status r rdb_bgsave_in_progress]
            # Delete/recreate avoids testrdb.set.key's replacement path, which
            # frees the old string before ModuleTypeSetValue frees it again.
            r multi
            r testrdb.set.before changed-before
            r testrdb.set.after changed-after
            r del modulekey
            r testrdb.set.key modulekey changed-module
            r append string -changed
            r incr counter
            r rpush list third
            r hset hash field changed-hash
            r sadd set third
            r zadd zset 3 third
            r xadd stream 2-0 field changed-stream
            r del filler:0
            r set filler:1 changed-filler
            r set created after-snapshot
            r exec
            assert_equal 1 [status r rdb_bgsave_in_progress]
            assert_equal changed-module [r testrdb.get.key modulekey]
            assert_equal changed-before [r testrdb.get.before]
            assert_equal changed-after [r testrdb.get.after]
            # Continue writing built-in values while the snapshot is serialized.
            for {set i 2} {$i < 66} {incr i} {
                r set filler:$i changed:$i
            }

            waitForBgsave r
            assert_equal 0 [status r rdb_bgsave_in_progress]
            assert_equal ok [status r rdb_last_bgsave_status]
            assert_equal 0 [expr {[status r total_forks] - $forks}]
            r config set rdb-key-save-delay 0
            # Reload only after the snapshot is inactive, without saving the
            # changed live values over the point-in-time RDB under test.
            assert_equal OK [r debug reload nosave]
            assert_equal original-module [r testrdb.get.key modulekey]
            assert_equal original-before [r testrdb.get.before]
            assert_equal original-after [r testrdb.get.after]
            assert_equal original-string [r get string]
            assert_equal 41 [r get counter]
            assert_equal {first second} [r lrange list 0 -1]
            assert_equal {field original-hash} [r hgetall hash]
            assert_equal {first second} [lsort [r smembers set]]
            assert_equal {first 1 second 2} [r zrange zset 0 -1 withscores]
            assert_equal {{1-0 {field original-stream}}} [r xrange stream - +]
            for {set i 0} {$i < 2000} {incr i} {
                assert_equal original:$i [r get filler:$i]
            }
            assert_equal 0 [r exists created]
            assert_equal 2008 [r dbsize]
        }
    }

    start_server [list tags {forkless bgsave repl modules external:skip} overrides \
        [list save {} bgsave-mode forkless repl-diskless-sync yes \
            repl-diskless-sync-delay 0 loadmodule "$testmodule $aux_mode"]] {
        set master [srv 0 client]
        set master_host [srv 0 host]
        set master_port [srv 0 port]
        start_server [list overrides [list save {} repl-diskless-load on-empty-db \
            loadmodule "$testmodule $aux_mode"]] {
            set replica [srv 0 client]
            test "forkless module diskless snapshot and buffered commands (args $aux_mode)" {
                $master select 0
                $replica select 0
                set seed {}
                for {set i 0} {$i < 2000} {incr i} {
                    lappend seed filler:$i original:$i
                }
                $master mset {*}$seed
                $master set counter 41
                $master rpush list original
                $master testrdb.set.key modulekey original-module
                $master testrdb.set.before original-before
                $master testrdb.set.after original-after
                $master config set rdb-key-save-delay 1000
                set forks [status $master total_forks]
                set fullsyncs [status $master sync_full]
                $replica replicaof $master_host $master_port
                wait_for_condition 200 50 {
                    [status $master rdb_bgsave_in_progress] eq "1"
                } else {
                    fail "forkless module diskless snapshot did not start"
                }

                # testrdb setters do not propagate. Changing AUX locally must
                # leave the replica with the original globals from the stream.
                # RENAME does propagate: its buffered replay must find the
                # original module key loaded from that same snapshot.
                $master multi
                $master testrdb.set.before changed-before
                $master testrdb.set.after changed-after
                $master rename modulekey moved-modulekey
                $master incr counter
                $master rpush list buffered
                $master set filler:0 changed-filler
                $master del filler:1
                $master set created buffered
                $master exec
                assert_equal 1 [status $master rdb_bgsave_in_progress]
                assert_equal 0 [expr {[status $master total_forks] - $forks}]

                waitForBgsave $master
                assert_equal 0 [status $master rdb_bgsave_in_progress]
                $master config set rdb-key-save-delay 0
                wait_for_sync $replica
                wait_for_ofs_sync $master $replica
                assert_equal ok [status $master rdb_last_bgsave_status]
                assert_equal 1 [expr {[status $master sync_full] - $fullsyncs}]
                assert_equal 0 [expr {[status $master total_forks] - $forks}]
                assert_equal original-before [$replica testrdb.get.before]
                assert_equal original-after [$replica testrdb.get.after]
                assert_equal changed-before [$master testrdb.get.before]
                assert_equal changed-after [$master testrdb.get.after]
                foreach client [list $master $replica] {
                    assert_equal 0 [$client exists modulekey]
                    assert_equal original-module [$client testrdb.get.key moved-modulekey]
                    # Non-idempotent writes detect a changed snapshot followed
                    # by duplicate application of the buffered command tail.
                    assert_equal 42 [$client get counter]
                    assert_equal {original buffered} [$client lrange list 0 -1]
                    assert_equal changed-filler [$client get filler:0]
                    assert_equal 0 [$client exists filler:1]
                    assert_equal buffered [$client get created]
                    assert_equal 2003 [$client dbsize]
                }
                for {set i 2} {$i < 2000} {incr i} {
                    assert_equal original:$i [$replica get filler:$i]
                }
            }
        }
    }
}
