# Works with libc as well as jemalloc. total_forks is a cumulative counter,
# so this also catches children which exit between observations.
start_server {tags {forkless bgsave} overrides {save {} bgsave-mode forkless stop-writes-on-bgsave-error no}} {
    test {forkless: zero children and a point-in-time live scan under churn} {
        r debug populate 2500 old 128
        r set big [string repeat b 1048576]
        r lpush list {*}[lrepeat 3000 original]
        r sadd set a b c
        r zadd zset 1 a 2 b
        r hset hash a old b old
        r xadd stream 1-0 field original
        r set ttl original px 3600000
        set expected [debug_digest]
        set forks [status r total_forks]
        r config set rdb-key-save-delay 300
        r bgsave
        assert_equal 1 [status r rdb_bgsave_in_progress]
        r append big changed
        r lset list 0 changed
        r sadd set changed
        r zadd zset 4 changed
        r hset hash a changed
        r xadd stream 2-0 field changed
        r pexpire ttl 1
        # Insert/delete enough keys to exercise rehash and address reuse.
        for {set i 0} {$i < 2500} {incr i} {
            r del old:$i
            r set new:$i new
            if {$i % 2 == 0} {r set old:$i replacement}
        }
        waitForBgsave r
        assert_equal ok [status r rdb_last_bgsave_status]
        assert_equal $forks [status r total_forks]
        r config set rdb-key-save-delay 0
        r debug reload nosave
        assert_equal $expected [debug_digest]
        assert {[r pttl ttl] > 3500000}
    }

    test {forkless: repeated cancellation normalizes marks before retry} {
        set forks [status r total_forks]
        r config set rdb-key-save-delay 1000
        for {set n 0} {$n < 3} {incr n} {
            r bgsave
            r set old:$n changed:$n
            r debug forkless-cancel
            waitForBgsave r
            assert_equal err [status r rdb_last_bgsave_status]
        }
        set expected [debug_digest]
        r bgsave
        r del old:10
        r set after-retry new
        waitForBgsave r
        assert_equal ok [status r rdb_last_bgsave_status]
        assert_equal $forks [status r total_forks]
        r config set rdb-key-save-delay 0
        r debug reload nosave
        assert_equal $expected [debug_digest]
    }

    test {forkless: SWAPDB preserves snapshot database identities} {
        r select 0
        r set dbkey zero
        r select 1
        r set dbkey one
        set expected [debug_digest]
        r config set rdb-key-save-delay 300
        r bgsave
        r swapdb 0 1
        r set dbkey changed
        r select 0
        r del dbkey
        waitForBgsave r
        r config set rdb-key-save-delay 0
        r debug reload nosave
        assert_equal $expected [debug_digest]
    }

    test {forkless: idle worker wakes the loop without busy polling} {
        r config set rdb-key-save-delay 500
        set cycles [status r eventloop_cycles]
        r bgsave
        after 500
        set elapsed [expr {[status r eventloop_cycles] - $cycles}]
        assert {$elapsed < 10000}
        # No client traffic is needed to feed the remaining scan or finish.
        after 2000
        assert_equal 0 [status r rdb_bgsave_in_progress]
        assert_equal ok [status r rdb_last_bgsave_status]
        r config set rdb-key-save-delay 0
    }

    test {forkless: stream group writes through read lookups preserve pending state} {
        # Keep the live scan busy in an earlier DB: these streams must be
        # captured by their mutations, not an already completed scan.
        r select 0
        r debug populate 1500 stream-filler 128
        r select 15
        r xadd pending 1-0 field before
        r xgroup create pending group 0
        foreach key {ack claim autoclaim ackdel scripted-ack} {
            r xadd $key 1-0 field original
            r xgroup create $key group 0
            r xreadgroup group group original streams $key >
        }
        r config set rdb-key-save-delay 500
        r bgsave
        r xreadgroup group group consumer streams pending >
        r xack ack group 1-0
        assert_equal 1 [r eval {return redis.call('XACK', KEYS[1], 'group', '1-0')} 1 scripted-ack]
        r xclaim claim group replacement 0 1-0
        r xautoclaim autoclaim group replacement 0 0-0
        r xackdel ackdel group ids 1 1-0
        assert_equal 1 [lindex [r xpending pending group] 0]
        waitForBgsave r
        r config set rdb-key-save-delay 0
        r debug reload nosave
        assert_equal 0 [lindex [r xpending pending group] 0]
        assert_equal {} [r xinfo consumers pending group]
        foreach key {ack claim autoclaim ackdel scripted-ack} {
            assert_equal 1 [lindex [r xpending $key group] 0]
            assert_equal original [lindex [lindex [r xpending $key group - + 10] 0] 1]
            assert_equal 1 [r xlen $key]
        }
    }

    test {forkless: readonly PFCOUNT preserves the original raw HLL bytes} {
        r select 15
        r pfadd hll a b c d e f
        set original [r get hll]
        r config set rdb-key-save-delay 500
        r bgsave
        assert_equal 6 [r pfcount hll]
        assert {$original ne [r get hll]}
        waitForBgsave r
        r config set rdb-key-save-delay 0
        r debug reload nosave
        assert_equal $original [r get hll]
    }

    test {forkless: template snapshots survive concurrent registry growth} {
        r himport prepare original a b
        r himport set template original before one
        r himport set template-array original [string repeat a 4096] two
        set expected [debug_digest]
        r config set rdb-key-save-delay 500
        r bgsave
        r hset template a changed
        r del template-array
        # Grow the by-ID registry while the helper still owns template copies.
        for {set i 0} {$i < 1200} {incr i} {
            r himport prepare added:$i field:$i
            r himport set added:$i added:$i value
        }
        waitForBgsave r
        r config set rdb-key-save-delay 0
        r debug reload nosave
        assert_equal $expected [debug_digest]
    }

    test {forkless: mode changes reject active fork and parent-worker saves} {
        r config set rdb-key-save-delay 300
        foreach mode {fork forkless} {
            r config set bgsave-mode $mode
            r bgsave
            set target [expr {$mode eq "fork" ? "forkless" : "fork"}]
            assert_error {*background save*} {r config set bgsave-mode $target}
            waitForBgsave r
            assert_equal ok [status r rdb_last_bgsave_status]
        }
        r config set rdb-key-save-delay 0
    }

    test {forkless: failed publication recovers without trusting partial marks} {
        set dir [lindex [r config get dir] 1]
        set original [lindex [r config get dbfilename] 1]
        file mkdir [file join $dir rejected-output]
        r config set dbfilename rejected-output
        r bgsave
        waitForBgsave r
        assert_equal err [status r rdb_last_bgsave_status]
        r config set dbfilename $original
        set expected [debug_digest]
        r config set rdb-key-save-delay 300
        r bgsave
        r set after-publication-failure new
        waitForBgsave r
        assert_equal ok [status r rdb_last_bgsave_status]
        r config set rdb-key-save-delay 0
        r debug reload nosave
        assert_equal $expected [debug_digest]
    }

    test {forkless: FLUSHDB is intentional cancellation and does not block writes} {
        r config set save {3600 100000}
        r config set stop-writes-on-bgsave-error yes
        r config set rdb-key-save-delay 300
        r bgsave
        r flushdb
        waitForBgsave r
        assert_equal ok [status r rdb_last_bgsave_status]
        assert_equal OK [r set after-flushdb writable]
        r config set save {}
        r config set stop-writes-on-bgsave-error no
    }

    test {forkless: FLUSHALL cancellation does not publish or free worker objects} {
        r debug populate 1000 flush-filler 128
        r config set rdb-key-save-delay 300
        r bgsave
        r flushall
        waitForBgsave r
        assert_equal 0 [r dbsize]
        r set after-flush survives
        r config set rdb-key-save-delay 0
        r bgsave
        waitForBgsave r
        r debug reload nosave
        assert_equal survives [r get after-flush]
    }
}

start_server {tags {forkless bgsave modules external:skip} overrides {save {} bgsave-mode forkless}} {
    r module load [file normalize tests/modules/testrdb.so] 0
    test {forkless: registered modules without payloads save without a child} {
        r select 0
        r debug populate 1500 filler 128
        set forks [status r total_forks]
        r bgsave
        waitForBgsave r
        assert_equal $forks [status r total_forks]
    }
    test {forkless: module payloads without AUX still freeze at the boundary} {
        r select 15
        r testrdb.set.key modulekey original
        r config set rdb-key-save-delay 500
        r bgsave
        r del modulekey
        r testrdb.set.key modulekey changed
        waitForBgsave r
        r config set rdb-key-save-delay 0
        r debug reload nosave
        assert_equal original [r testrdb.get.key modulekey]
    }
}

start_server {tags {forkless bgsave modules external:skip} overrides {save {} bgsave-mode forkless}} {
    r module load [file normalize tests/modules/testrdb.so] 7
    test {forkless: module values and both AUX phases freeze before commands resume} {
        r debug populate 1500 filler 128
        r testrdb.set.key modulekey original
        r testrdb.set.before before-original
        r testrdb.set.after after-original
        r config set rdb-key-save-delay 500
        set forks [status r total_forks]
        r bgsave
        # testrdb.set.key's replacement path frees its old module string
        # twice; delete first, then create the replacement through its API.
        r del modulekey
        r testrdb.set.key modulekey changed
        r testrdb.set.before before-changed
        r testrdb.set.after after-changed
        waitForBgsave r
        assert_equal ok [status r rdb_last_bgsave_status]
        assert_equal $forks [status r total_forks]
        r config set rdb-key-save-delay 0
        r debug reload nosave
        assert_equal original [r testrdb.get.key modulekey]
        assert_equal before-original [r testrdb.get.before]
        assert_equal after-original [r testrdb.get.after]
    }
}

start_server {tags {forkless bgsave repl} overrides {save {} bgsave-mode forkless repl-diskless-sync yes repl-diskless-sync-delay 0}} {
    set master [srv 0 client]
    $master debug populate 2000 repl 128
    $master config set rdb-key-save-delay 500
    set forks [status $master total_forks]
    start_server {overrides {save {} repl-diskless-load on-empty-db}} {
        set replica [srv 0 client]
        test {forkless: diskless checksum EOF and buffered commands with zero children} {
            $replica replicaof [srv -1 host] [srv -1 port]
            wait_for_condition 200 10 {[status $master rdb_bgsave_in_progress] == 1} else {
                fail "snapshot did not start"
            }
            for {set i 0} {$i < 500} {incr i} {
                $master set repl:$i changed
                $master set new:$i new
            }
            wait_for_condition 200 50 {[status $replica master_link_status] eq "up"} else {
                fail "diskless replication did not finish"
            }
            wait_for_condition 200 50 {[$replica get new:499] eq "new"} else {
                fail "replication command tail missing"
            }
            assert_equal [$master debug digest] [$replica debug digest]
            assert_equal $forks [status $master total_forks]
        }
    }
}
