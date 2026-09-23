# Random eviction is supported without relaxing the LRU/LFU safety guard.
# A slow serializer makes evictions overlap pending child-confirmed records.
foreach mode {hybrid forkless} {
    foreach lazy {no yes} {
        start_server [list tags {forkless eviction} overrides [list save {} bgsave-mode $mode maxmemory-policy allkeys-random lazyfree-lazy-eviction $lazy]] {
            test "$mode / lazy=$lazy: eviction priority preserves the starting snapshot" {
                r config set rdb-key-save-delay 1000
                r config set forkless-string-max 2048
                for {set i 0} {$i < 2048} {incr i} {
                    r set original:$i [string repeat x 1024]
                }
                set expected [debug_digest]
                r config set maxmemory [expr {[status r used_memory] + 65536}]
                r bgsave
                assert_equal 1 [status r rdb_bgsave_in_progress]
                set overlap 0
                for {set i 0} {$i < 1500} {incr i} {
                    if {[catch {r set incoming:$i [string repeat y 3072]} err]} {
                        assert_match {*OOM*} $err
                    }
                    if {$i % 50 == 0 && [status r evicted_keys] > 0 &&
                        [status r rdb_bgsave_in_progress]} {set overlap 1}
                }
                assert_equal 1 $overlap
                waitForBgsave r
                assert_equal ok [status r rdb_last_bgsave_status]
                if {$mode eq "hybrid"} {
                    assert {[status r forkless_priority_keys] > 0}
                } else {
                    assert {[status r forkless_retained_strings] > 0}
                    assert_equal 0 [status r total_forks]
                }
                r config set maxmemory 0
                r config set rdb-key-save-delay 0
                r debug reload nosave
                assert_equal $expected [debug_digest]
                # A new save starts with an empty priority window and counter.
                r bgsave
                waitForBgsave r
                assert_equal 0 [status r forkless_priority_keys]
            }
        }

        start_server [list tags {forkless eviction repl} overrides [list save {} bgsave-mode $mode maxmemory-policy allkeys-random lazyfree-lazy-eviction $lazy repl-diskless-sync yes repl-diskless-sync-delay 0]] {
            set master [srv 0 client]
            set master_host [srv 0 host]
            set master_port [srv 0 port]
            start_server {overrides {save {}}} {
                set replica [srv 0 client]
                test "$mode / lazy=$lazy: eviction priority keeps diskless replicas consistent" {
                    $master config set rdb-key-save-delay 1000
                    $master config set forkless-string-max 2048
                    foreach db {0 1} {
                        $master select $db
                        for {set i 0} {$i < 1024} {incr i} {
                            $master set original:$i [string repeat $db 1024]
                        }
                    }
                    $master config set maxmemory [expr {[status $master used_memory] + 65536}]
                    $replica replicaof $master_host $master_port
                    wait_for_condition 100 20 {
                        [status $master rdb_bgsave_in_progress] == 1
                    } else {fail "full sync did not start"}
                    set overlap 0
                    for {set i 0} {$i < 1500} {incr i} {
                        $master select [expr {$i % 2}]
                        if {[catch {$master set incoming:$i [string repeat y 3072]} err]} {
                            assert_match {*OOM*} $err
                        }
                        if {$i % 50 == 0 && [status $master evicted_keys] > 0 &&
                            [status $master rdb_bgsave_in_progress]} {set overlap 1}
                    }
                    assert_equal 1 $overlap
                    $master config set maxmemory 0
                    wait_for_condition 300 50 {
                        [status $replica master_link_status] eq {up} &&
                        [$master debug digest] eq [$replica debug digest]
                    } else {fail "diskless snapshot and command replay diverged"}
                    if {$mode eq "hybrid"} {
                        assert {[status $master forkless_priority_keys] > 0}
                    } else {
                        assert {[status $master forkless_retained_strings] > 0}
                        assert_equal 0 [status $master total_forks]
                    }
                    assert {[status $master sync_full] > 0}
                }
            }
        }
    }
}
