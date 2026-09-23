start_server {tags {"forkless bgsave"} overrides {save {} bgsave-mode hybrid}} {
    test {CONFIG bgsave-mode hybrid} {
        assert_equal [r config get bgsave-mode] {bgsave-mode hybrid}
        assert_equal [r config get forkless-string-max] {forkless-string-max 512000}
    }

    test {INFO reports bgsave_mode hybrid} {
        assert_match {*bgsave_mode:hybrid*} [r info persistence]
    }

    test {hybrid: small strings and listpack hashes in forkless arena} {
        r flushall
        r set small hello
        r set mid [string repeat x 2048]
        r set big [string repeat a 600000]
        r hset lh f1 v1 f2 v2
        r sadd ls a b c
        if {[string match {*jemalloc*} [r info memory]]} {
            set info [r info persistence]
            regexp {forkless_arena:([0-9]+)} $info -> fa
            set small_a [r debug forkless-arena small]
            set mid_a [r debug forkless-arena mid]
            set big_a [r debug forkless-arena big]
            set lh_a [r debug forkless-arena lh]
            set ls_a [r debug forkless-arena ls]
            if {$small_a >= 0} {
                assert_equal $small_a $fa
                assert_equal $mid_a $fa
                assert_equal $lh_a $fa
                assert {$ls_a != $fa}
                assert {$big_a != $fa}
            }
        }
    }

    test {BGSAVE strings hashes and sets then reload} {
        r flushall
        r set small hello
        r set big [string repeat a 600000]
        r hset lh f1 v1 f2 v2
        r sadd ls a b c
        r bgsave
        waitForBgsave r
        r debug reload nosave
        assert_equal [r get small] hello
        assert_equal [string length [r get big]] 600000
        assert_equal [r hget lh f1] v1
        assert_equal [r scard ls] 3
    }

    foreach mode {hybrid forkless} {
        foreach checksum {yes no} {
            start_server [list overrides [list save {} bgsave-mode $mode rdbchecksum $checksum]] {
                foreach autosync {yes no} {
                    test "$mode single-writer disk stream: checksum=$checksum autosync=$autosync" {
                        r flushall
                        r config set bgsave-mode $mode
                        r config set rdb-save-incremental-fsync $autosync
                        foreach db {0 1 3} {
                            r select $db
                            r set large [string repeat $db 1100000]
                            for {set i 0} {$i < 600} {incr i} {
                                r set key:$i [string repeat $db 128]
                            }
                            r sadd child-set a b c
                        }
                        set expected [debug_digest]
                        r bgsave
                        waitForBgsave r
                        assert_equal ok [status r rdb_last_bgsave_status]
                        r debug reload nosave
                        assert_equal $expected [debug_digest]
                        set dir [lindex [r config get dir] 1]
                        assert_equal {} [glob -nocomplain [file join $dir temp-forkless-parent-*.rdb]]
                    }
                }
            }
        }
    }
    r select 0
    r config set bgsave-mode hybrid
    r config set rdb-save-incremental-fsync yes

    test {mutate forkless key during BGSAVE} {
        r flushall
        r set k 1
        r bgsave
        r set k 2
        waitForBgsave r
        r debug reload nosave
        assert_equal 1 [r get k]
    }

    test {in-place string writes during BGSAVE keep the fork-time value} {
        r flushall
        r set append hello
        r set range hello
        r bgsave
        r append append world
        r setrange range 1 XX
        waitForBgsave r
        r debug reload nosave
        assert_equal hello [r get append]
        assert_equal hello [r get range]
    }

    test {failed SET NX followed by in-place writes preserves the RAW snapshot} {
        r flushall
        r config set rdb-key-save-delay 1000
        r debug populate 2000 filler 128
        set original [string repeat a 2048]
        r set retained $original
        r bgsave
        assert_equal 1 [status r rdb_bgsave_in_progress]
        assert_equal {} [r set retained replacement nx]
        r append retained suffix
        r setrange retained 0 changed
        waitForBgsave r
        assert_equal ok [status r rdb_last_bgsave_status]
        r debug reload nosave
        assert_equal $original [r get retained]
        r config set rdb-key-save-delay 0
    }

    test {RAW replacements and deletes preserve the starting snapshot} {
        r flushall
        r config set rdb-key-save-delay 1000
        r debug populate 2000 filler 128
        set original [string repeat b 2048]
        foreach key {replace delete unlink metadata} {r set $key $original}
        r bgsave
        assert_equal 1 [status r rdb_bgsave_in_progress]
        r set replace new
        r del delete
        r unlink unlink
        r set metadata ignored nx
        r pexpire metadata 1000000
        waitForBgsave r
        assert_equal ok [status r rdb_last_bgsave_status]
        r debug reload nosave
        foreach key {replace delete unlink metadata} {
            assert_equal $original [r get $key]
            assert_equal -1 [r pttl $key]
        }
        r config set rdb-key-save-delay 0
    }

    test {GETEX persist during BGSAVE keeps the fork-time TTL} {
        r flushall
        r config set rdb-key-save-delay 100
        r debug populate 2000 filler 128
        r set k v
        r pexpire k 3600000
        r bgsave
        r persist k
        waitForBgsave r
        r debug reload nosave
        assert {[r pttl k] > 0}
        r config set rdb-key-save-delay 0
    }

    test {INCR during BGSAVE keeps the fork-time integer} {
        r flushall
        r set k 1
        r bgsave
        r incr k
        waitForBgsave r
        r debug reload nosave
        assert_equal 1 [r get k]
    }

    test {hybrid snapshot excludes post-fork insert delete and updates} {
        r flushall
        r debug populate 10000 snapshot 256
        r set changed old
        r set deleted keep
        r hset hash field old
        r bgsave
        r set changed new
        r del deleted
        r hset hash field new
        r set created late
        waitForBgsave r
        r debug reload nosave
        assert_equal old [r get changed]
        assert_equal keep [r get deleted]
        assert_equal old [r hget hash field]
        assert_equal 0 [r exists created]
    }

    test {hybrid snapshot keeps the fork-time DB after SWAPDB} {
        r flushall
        r config set rdb-key-save-delay 100
        r select 0
        r debug populate 10000 filler 128
        r set snap old
        r hset hash field old
        r select 1
        r set other one
        r bgsave
        r swapdb 0 1
        r set snap new
        r hset hash field new
        waitForBgsave r
        r debug reload nosave
        r select 0
        assert_equal old [r get snap]
        assert_equal old [r hget hash field]
        r select 1
        assert_equal {} [r get snap]
        assert_equal {} [r hget hash field]
        assert_equal one [r get other]
        r select 0
        r config set rdb-key-save-delay 0
    }

    test {HEXPIRE keeps a listpack-ex hash in the forkless arena} {
        r flushall
        r hset hx f v
        r hexpire hx 60 FIELDS 1 f
        if {[string match {*jemalloc*} [r info memory]]} {
            regexp {forkless_arena:([0-9]+)} [r info persistence] -> fa
            if {[r debug forkless-arena hx] >= 0} {
                assert_equal [r debug forkless-arena hx] $fa
            }
        }
        r bgsave
        waitForBgsave r
        r debug reload nosave
        assert_equal v [r hget hx f]
        assert {[lindex [r httl hx FIELDS 1 f] 0] > 0}
    }

    test {listpack-ex mutation during BGSAVE keeps the fork-time fields} {
        r flushall
        r config set rdb-key-save-delay 100
        r debug populate 2000 filler 128
        r hset hx f old
        r hexpire hx 3600 FIELDS 1 f
        r bgsave
        r hset hx f new
        r hset hx g late
        waitForBgsave r
        r debug reload nosave
        assert_equal old [r hget hx f]
        assert_equal 0 [r hexists hx g]
        assert {[lindex [r httl hx FIELDS 1 f] 0] > 0}
        r config set rdb-key-save-delay 0
    }

    test {listpack hash convert to HT still reloads} {
        r flushall
        r config set hash-max-listpack-entries 2
        r hset h a 1 b 2
        r hset h c 3
        if {[string match {*jemalloc*} [r info memory]]} {
            regexp {forkless_arena:([0-9]+)} [r info persistence] -> fa
            if {[r debug forkless-arena h] >= 0} {
                assert {[r debug forkless-arena h] != $fa}
            }
        }
        r bgsave
        waitForBgsave r
        r debug reload nosave
        assert_equal [r hlen h] 3
        r config set hash-max-listpack-entries 512
    }

    test {APPEND past forkless-string-max still reloads} {
        r flushall
        r set s [string repeat a 100]
        r append s [string repeat b 600000]
        r bgsave
        waitForBgsave r
        r debug reload nosave
        assert_equal [string length [r get s]] 600100
    }

    test {SETRANGE past forkless-string-max moves the value back to the child arena} {
        r flushall
        r set s short
        r setrange s 600000 x
        if {[string match {*jemalloc*} [r info memory]]} {
            regexp {forkless_arena:([0-9]+)} [r info persistence] -> fa
            if {[r debug forkless-arena s] >= 0} {
                assert {[r debug forkless-arena s] != $fa}
            }
        }
        r bgsave
        waitForBgsave r
        r debug reload nosave
        assert_equal 600001 [string length [r get s]]
    }

    test {FLUSHDB cancels split snapshot before releasing values} {
        r flushall
        r config set forkless-string-max 1024
        r config set rdb-key-save-delay 10000
        for {set i 0} {$i < 100} {incr i} {
            r set small:$i [string repeat s 512]
            r set large:$i [string repeat l 2048]
        }
        r bgsave
        assert_equal 1 [status r rdb_bgsave_in_progress]
        r flushdb async
        waitForBgsave r
        assert_equal 1 [status r forkless_progress_recovery]
        assert_equal 0 [r dbsize]
        assert_equal PONG [r ping]
        r config set rdb-key-save-delay 0
        r config set forkless-string-max 512000
        r set after-flush alive
        r bgsave
        waitForBgsave r
        r debug reload nosave
        assert_equal alive [r get after-flush]
    }

    test {snapshot progress: alternating rounds and new values} {
        r flushall
        r config set bgsave-mode hybrid
        r debug populate 2000 progress-filler 128
        r set progress-original before
        r bgsave
        waitForBgsave r
        assert_equal 0 [status r forkless_progress_recovery]
        r config set rdb-key-save-delay 1000
        for {set round 0} {$round < 4} {incr round} {
            r set progress-original before:$round
            r del progress-new
            set skips [status r forkless_capture_skipped]
            r bgsave
            assert_equal 1 [status r rdb_bgsave_in_progress]
            r set progress-original after:$round
            r set progress-new first
            r append progress-new second
            r del progress-new
            assert {[status r forkless_capture_skipped] > $skips}
            waitForBgsave r
            assert_equal ok [status r rdb_last_bgsave_status]
            r debug reload nosave
            assert_equal before:$round [r get progress-original]
            assert_equal 0 [r exists progress-new]
        }
        r config set rdb-key-save-delay 0
    }

    test {snapshot progress: repeated failures recover before reusing marks} {
        r config set stop-writes-on-bgsave-error no
        r config set rdb-key-save-delay 1000
        for {set attempt 0} {$attempt < 3} {incr attempt} {
            r set recovery-original before:$attempt
            r bgsave
            r set recovery-original after:$attempt
            exec kill -9 [get_child_pid 0]
            waitForBgsave r
            assert_equal err [status r rdb_last_bgsave_status]
            assert_equal 1 [status r forkless_progress_recovery]
        }
        r set recovery-original recovered
        r bgsave
        r set recovery-original changed
        r set recovery-new created
        waitForBgsave r
        assert_equal ok [status r rdb_last_bgsave_status]
        assert_equal 0 [status r forkless_progress_recovery]
        r debug reload nosave
        assert_equal recovered [r get recovery-original]
        assert_equal 0 [r exists recovery-new]
        foreach mode {fork forkless hybrid} {
            r config set bgsave-mode $mode
            r set recovery-original $mode
            r bgsave
            r set recovery-original changed
            waitForBgsave r
            r debug reload nosave
            assert_equal $mode [r get recovery-original]
        }
        r config set forkless-string-max 64
        r bgsave
        assert_equal 1 [status r forkless_progress_recovery]
        waitForBgsave r
        assert_equal 0 [status r forkless_progress_recovery]
        r config set forkless-string-max 512000
        r config set stop-writes-on-bgsave-error yes
        r config set rdb-key-save-delay 0
    }

    test {completed pin retirement survives overwrite delete and address reuse} {
        r flushall
        r debug populate 4000 retire-key 256
        r bgsave
        waitForBgsave r
        set expected [debug_digest]
        set removed [status r forkless_removed_pins]
        r config set rdb-key-save-delay 200
        r bgsave
        for {set i 0} {$i < 4000} {incr i} {
            r set retire-key:$i changed
            if {$i % 2 == 0} {
                r del retire-key:$i
                r set retire-key:$i replacement
            }
        }
        waitForBgsave r
        assert_equal ok [status r rdb_last_bgsave_status]
        assert {[status r forkless_removed_pins] > $removed}
        r debug reload nosave
        assert_equal $expected [debug_digest]
        r config set rdb-key-save-delay 0
    }

    test {preserved-value limit aborts the save before a large capture} {
        r flushall
        r config set stop-writes-on-bgsave-error no
        r config set forkless-retained-max-memory 1
        r config set rdb-key-save-delay 1000
        r debug populate 2000 limit-key 128
        set aborted [status r forkless_limit_aborts]
        r bgsave
        for {set i 0} {$i < 2000 && [status r forkless_limit_aborts] == $aborted} {incr i} {
            r set limit-key:$i changed
        }
        waitForBgsave r
        assert_equal [expr {$aborted + 1}] [status r forkless_limit_aborts]
        assert_equal err [status r rdb_last_bgsave_status]
        assert_equal 1 [status r forkless_progress_recovery]
        assert_equal PONG [r ping]
        r config set forkless-retained-max-memory 0
        r config set rdb-key-save-delay 0
        r bgsave
        waitForBgsave r
        assert_equal ok [status r rdb_last_bgsave_status]
        wait_for_condition 100 10 {
            [status r forkless_retained_bytes] == 0
        } else {fail "Preserved-value accounting did not drain"}
        r config set stop-writes-on-bgsave-error yes
    }

    test {AOF rewrite refused in hybrid mode} {
        catch {r bgrewriteaof} e
        assert_match {*Can't execute an AOF background rewriting*} $e
    }

    test {hybrid BGSAVE records persistence stats} {
        r flushall
        r set m measure
        r bgsave
        waitForBgsave r
        assert_match {*rdb_last_bgsave_status:ok*} [r info persistence]
        assert_match {*bgsave_mode:hybrid*} [r info persistence]
    }
}

start_server {tags {"forkless bgsave"} overrides {save {} bgsave-mode forkless}} {
    test {forkless: every built-in type is placed in the flippable arena} {
        r set integer 1
        r set small hello
        r set big [string repeat a 600000]
        r hset lh f1 v1
        r hset hx f v
        r hexpire hx 60 FIELDS 1 f
        r config set hash-max-listpack-entries 1
        r hset hh f1 v1 f2 v2
        r config set hash-max-listpack-entries 512
        r sadd set a b
        r lpush list a b
        r zadd zset 1 a 2 b
        r xadd stream * f v
        if {[string match {*jemalloc*} [r info memory]]} {
            set info [r info persistence]
            regexp {forkless_arena:([0-9]+)} $info -> fa
            set small_a [r debug forkless-arena small]
            if {$small_a >= 0} {
                # Forkless shares the widest placement rule with fork so that
                # switching between the two is a pure page flip.
                foreach key {integer small big lh hx hh set list zset stream} {
                    assert_equal [r debug forkless-arena $key] $fa
                }
            }
        }
        r bgsave
        waitForBgsave r
        r debug reload nosave
        assert_equal [r get small] hello
        assert_equal [string length [r get big]] 600000
        assert_equal [r hget lh f1] v1
        assert_equal v [r hget hx f]
        assert {[lindex [r httl hx FIELDS 1 f] 0] > 0}
        assert_equal [r hget hh f2] v2
        assert_equal [r scard set] 2
        assert_equal [r llen list] 2
        assert_equal [r zcard zset] 2
        assert_equal [r xlen stream] 1
    }

    test {forkless snapshot handles parent-owned and child-owned mutations} {
        r flushall
        r debug populate 10000 snapshot 256
        r hset lh f1 old
        r sadd set old
        r lpush list old
        r zadd zset 1 old
        r config set hash-max-listpack-entries 1
        r hset hash f1 old f2 old
        r config set hash-max-listpack-entries 512
        r xadd stream 1-0 f old
        r bgsave
        r hset lh f2 new
        r sadd set new
        r lpush list new
        r zadd zset 2 new
        r hset hash f3 new
        r xadd stream 2-0 f new
        waitForBgsave r
        r debug reload nosave
        assert_equal 1 [r hlen lh]
        assert_equal 0 [r hexists lh f2]
        assert_equal {old} [r smembers set]
        assert_equal {old} [r lrange list 0 -1]
        assert_equal {old} [r zrange zset 0 -1]
        assert_equal 2 [r hlen hash]
        assert_equal 0 [r hexists hash f3]
        assert_equal 1 [r xlen stream]
    }

    test {AOF rewrite refused in forkless mode} {
        catch {r bgrewriteaof} e
        assert_match {*Can't execute an AOF background rewriting*} $e
    }
}

start_server {tags {"forkless bgsave"} overrides {save {} bgsave-mode fork}} {
    test {classic fork BGSAVE still works} {
        r set x y
        r bgsave
        waitForBgsave r
        r debug reload nosave
        assert_equal [r get x] y
        assert_match {*bgsave_mode:fork*} [r info persistence]
    }

    test {CONFIG SET bgsave-mode works after a cold fork start} {
        if {![string match {*jemalloc*} [r info memory]]} {
            skip "requires jemalloc arenas"
        }
        r set before-switch old
        r pexpire before-switch 3600000
        foreach mode {hybrid fork forkless fork} {
            assert_equal OK [r config set bgsave-mode $mode]
            r bgsave
            waitForBgsave r
            r debug reload nosave
            assert_equal old [r get before-switch]
            assert {[r pttl before-switch] > 0}
        }
        assert_match {*bgsave_mode:fork*} [r info persistence]
    }
}

start_server {tags {"forkless bgsave"} overrides {
    save {}
    bgsave-mode forkless
    enable-debug-command yes
}} {
    test {bgsave-mode switches at runtime without relocating values} {
        if {![string match {*jemalloc*} [r info memory]]} {
            skip "requires jemalloc arenas"
        }
        regexp {forkless_arena:([0-9]+)} [r info persistence] -> fa
        r set s hello
        r sadd st a b c
        r xadd stream * f v
        if {[r debug forkless-arena s] < 0} {
            skip "arena lookup unavailable"
        }

        # Switching to fork flips page visibility only: the objects must stay
        # in the same arena, which is what makes the switch O(extents).
        foreach key {s st stream} { assert_equal [r debug forkless-arena $key] $fa }
        r config set bgsave-mode fork
        assert_match {*bgsave_mode:fork*} [r info persistence]
        foreach key {s st stream} { assert_equal [r debug forkless-arena $key] $fa }
        r bgsave
        waitForBgsave r
        assert_equal ok [status r rdb_last_bgsave_status]

        # Hybrid narrows placement, so only keys written afterwards move.
        r config set bgsave-mode hybrid
        r set bigstr [string repeat a 600000]
        assert {[r debug forkless-arena bigstr] != $fa}
        assert_equal [r debug forkless-arena s] $fa

        r config set bgsave-mode forkless
        r bgsave
        waitForBgsave r
        assert_equal ok [status r rdb_last_bgsave_status]

        r debug reload nosave
        assert_equal hello [r get s]
        assert_equal 3 [r scard st]
        assert_equal 1 [r xlen stream]
        assert_equal 600000 [string length [r get bigstr]]
    }

    test {churning forkless values keeps the RDB and the server healthy} {
        # Frees of arena-B memory must bypass the thread cache, otherwise those
        # pages get recycled into unrelated allocations.
        r config set bgsave-mode forkless
        for {set i 0} {$i < 500} {incr i} { r set churn:$i val$i }
        for {set i 0} {$i < 500} {incr i} { r del churn:$i }
        for {set i 0} {$i < 200} {incr i} { r append grow:$i 0123456789 }
        r bgsave
        waitForBgsave r
        assert_equal ok [status r rdb_last_bgsave_status]
        set digest [debug_digest]
        r debug reload nosave
        assert_equal $digest [debug_digest]
    }
}

start_server {tags {"forkless bgsave repl"} overrides {
    save {}
    bgsave-mode hybrid
    forkless-string-max 1024
    repl-diskless-sync yes
    repl-diskless-sync-delay 0
}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    $master set small before
    $master set big [string repeat x 4096]

    start_server {overrides {save {}}} {
        set replica [srv 0 client]
        test {hybrid supports diskless full sync over RDB channel} {
            $replica replicaof $master_host $master_port
            wait_for_condition 100 100 {
                [string match {*master_link_status:up*} [$replica info replication]]
            } else {
                fail "replica did not complete hybrid diskless full sync"
            }
            assert_equal before [$replica get small]
            assert_equal 4096 [string length [$replica get big]]
        }
    }
}

start_server {tags {"forkless bgsave repl"} overrides {
    save {}
    bgsave-mode hybrid
    repl-diskless-sync yes
    repl-diskless-sync-delay 0
}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    for {set i 0} {$i < 20000} {incr i} {
        $master set k$i v$i
    }

    start_server {overrides {save {}}} {
        set replica [srv 0 client]
        test {hybrid diskless sync streams many small keys without buffering them in the child} {
            $replica replicaof $master_host $master_port
            wait_for_condition 200 100 {
                [string match {*master_link_status:up*} [$replica info replication]]
            } else {
                fail "replica did not complete 20k-key hybrid diskless full sync"
            }
            assert_equal 20000 [$replica dbsize]
            assert_equal v0 [$replica get k0]
            assert_equal v19999 [$replica get k19999]
        }
    }
}

start_server {tags {"forkless bgsave repl"} overrides {
    save {}
    bgsave-mode forkless
    repl-diskless-sync yes
    repl-diskless-sync-delay 0
}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    $master set small before
    $master set big [string repeat x 4096]

    start_server {overrides {save {}}} {
        set replica [srv 0 client]
        test {forkless supports diskless full sync over RDB channel} {
            $replica replicaof $master_host $master_port
            wait_for_condition 100 100 {
                [string match {*master_link_status:up*} [$replica info replication]]
            } else {
                fail "replica did not complete forkless diskless full sync"
            }
            assert_equal before [$replica get small]
            assert_equal 4096 [string length [$replica get big]]
        }
    }
}

start_server {tags {"forkless bgsave repl"} overrides {
    save {}
    bgsave-mode hybrid
    forkless-string-max 1024
    repl-diskless-sync yes
    repl-diskless-sync-delay 0
}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]

    # Populate a mix of forkless-small and forkable-large keys.
    for {set i 0} {$i < 200} {incr i} {
        $master set k$i v$i
        $master set big$i [string repeat x 2048]
        $master hset lh$i f1 v1 f2 v2
    }

    start_server {overrides {save {}}} {
        set replica [srv 0 client]
        test {hybrid diskless sync under concurrent writes matches digest} {
            $replica replicaof $master_host $master_port

            # Overwrite existing keys and insert new ones while the RDB streams.
            for {set i 0} {$i < 200} {incr i} {
                $master set k$i new$i
                $master set big$i [string repeat y 2048]
                $master set n$i late$i
                $master hset lh$i f3 v3
            }

            wait_for_condition 200 50 {
                [string match {*master_link_status:up*} [$replica info replication]]
            } else {
                fail "replica did not complete concurrent hybrid diskless sync"
            }

            # After catch-up the replica must match the live master.
            wait_for_condition 200 50 {
                [$master debug digest] eq [$replica debug digest]
            } else {
                set mdigest [$master debug digest]
                set rdigest [$replica debug digest]
                fail "master/replica digest mismatch after concurrent diskless sync: master=$mdigest replica=$rdigest"
            }
        }

        test {hybrid diskless frames preserve large records and database selection} {
            set fullsyncs [status $master sync_full]
            $replica replicaof no one
            $master config set rdbcompression no
            $master config set forkless-string-max 131072
            foreach db {0 1} {
                $master select $db
                for {set i 0} {$i < 1200} {incr i} {
                    $master set framed:$db:$i [string repeat $db 8192]
                    $master sadd child:$db:$i one two three
                }
            }
            $master select 0
            # Promotion discarded the replica's cached upstream state. Require
            # an actual full sync below so command replay cannot mask an RDB bug.
            $replica flushall
            $replica replicaof $master_host $master_port
            wait_for_condition 300 100 {
                [string match {*master_link_status:up*} [$replica info replication]] &&
                [$master debug digest] eq [$replica debug digest]
            } else {
                fail "framed mixed multi-DB diskless snapshot did not match master"
            }
            assert {[status $master sync_full] > $fullsyncs}
        }
    }
}
