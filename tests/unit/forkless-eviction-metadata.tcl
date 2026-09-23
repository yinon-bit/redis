set testmodule [file normalize tests/modules/misc.so]
foreach mode {hybrid forkless} {
    foreach policy {allkeys-lru allkeys-lfu} {
        start_server [list tags {forkless modules external:skip} overrides [list save {} bgsave-mode $mode maxmemory-policy $policy]] {
            r module load $testmodule
            test "$mode / $policy: metadata-only module writes and CONFIG SET preserve snapshot metadata" {
                r config set lfu-decay-time 0
                r config set rdb-key-save-delay 1000
                r debug populate 1000 filler 128
                for {set i 0} {$i <= 120} {incr i} {
                    switch [expr {$i % 4}] {
                        0 {r set note:$i 123}
                        1 {r set note:$i small}
                        2 {r set note:$i [string repeat x 128]}
                        3 {r hset note:$i field original}
                    }
                    r pexpire note:$i 3600000
                    if {$policy eq {allkeys-lfu}} {
                        assert_equal 1 [r test.setlfu note:$i 100]
                    } else {
                        assert_equal 1 [r test.setlru note:$i 600000]
                    }
                }
                set expected [debug_digest]
                r bgsave
                assert_equal 1 [status r rdb_bgsave_in_progress]
                # Ordinary GET has the same no-touch behavior as classic BGSAVE.
                r get note:0
                if {$policy eq {allkeys-lfu}} {
                    assert_equal 100 [r object freq note:0]
                } else {
                    assert {[r object idletime note:0] >= 600}
                }
                for {set i 0} {$i < 120} {incr i} {
                    # First create an ordinary pin, then modify its header via
                    # a module key opened READ|NOTOUCH (not a write lookup).
                    if {$i < 60} {r pexpire note:$i 7200000}
                    if {$policy eq {allkeys-lfu}} {
                        assert_equal 1 [r test.setlfu note:$i 5]
                        assert_equal 5 [r object freq note:$i]
                    } else {
                        assert_equal 1 [r test.setlru note:$i 0]
                        assert {[r object idletime note:$i] < 3}
                    }
                }
                assert_equal 1 [status r rdb_bgsave_in_progress]
                # These settings change immediately for the live DB, not the save.
                set other [expr {$policy eq {allkeys-lfu} ? "allkeys-lru" : "allkeys-lfu"}]
                r config set maxmemory-policy $other
                r config set lfu-decay-time 7
                assert_equal [list maxmemory-policy $other] [r config get maxmemory-policy]
                # Switching to LRM makes writes update the same header. This
                # key has not had a module mutation/private copy yet.
                r config set maxmemory-policy allkeys-lrm
                r pexpire note:120 7200000
                waitForBgsave r
                assert_equal ok [status r rdb_last_bgsave_status]
                r config set maxmemory-policy $policy
                r config set lfu-decay-time 0
                r config set rdb-key-save-delay 0
                r debug reload nosave
                assert_equal $expected [debug_digest]
                for {set i 0} {$i <= 120} {incr i} {
                    set ttl [r pttl note:$i]
                    assert {$ttl > 3500000 && $ttl <= 3600000}
                    if {$policy eq {allkeys-lfu}} {
                        assert_equal 100 [r object freq note:$i]
                    } else {
                        set idle [r object idletime note:$i]
                        assert {$idle >= 600 && $idle < 630}
                    }
                }
                # The next save must use the new policy, not the old context.
                r config set maxmemory-policy $other
                r bgsave
                waitForBgsave r
                assert_equal ok [status r rdb_last_bgsave_status]
                # Cold fork -> hybrid/forkless with eviction is also accepted.
                r config set bgsave-mode fork
                r config set bgsave-mode $mode
            }
        }
    }
}
