# Fresh processes are essential: another split save would hide the startup bug.
foreach mode {hybrid forkless} {
    foreach startup [list $mode fork] {
        start_server [list tags {forkless bgsave} overrides [list save {} bgsave-mode $startup]] {
            test "$mode first split save from $startup skips new/saved values and retires pins" {
                r config set forkless-string-max 1024
                r debug populate 4000 first-key 256
                r hset first-hash field original
                r set first-big [string repeat b 1048576]
                if {$startup eq "fork"} {
                    # Classic saves and RDB loading must not consume the fresh label.
                    r bgsave
                    waitForBgsave r
                    r debug reload nosave
                    r config set bgsave-mode $mode
                }
                set expected [debug_digest]
                set forks [status r total_forks]
                r config set rdb-key-save-delay 200
                r bgsave
                assert_equal 1 [status r rdb_bgsave_in_progress]
                assert_equal 0 [status r forkless_progress_recovery]
                set skipped [status r forkless_capture_skipped]
                r set born-after-start first
                r append born-after-start second
                r del born-after-start
                assert {[status r forkless_capture_skipped] > $skipped}
                r hset first-hash field changed
                r append first-big changed
                for {set i 0} {$i < 4000} {incr i} {
                    r set first-key:$i changed
                    if {$i % 2 == 0} {
                        r del first-key:$i
                        r set first-key:$i replacement
                    }
                }
                waitForBgsave r
                assert_equal ok [status r rdb_last_bgsave_status]
                if {$mode eq "hybrid"} {
                    assert {[status r forkless_removed_pins] > 0}
                } else {
                    assert_equal $forks [status r total_forks]
                    assert_equal 0 [status r forkless_removed_pins]
                    wait_for_condition 100 10 {[status r forkless_snapshot_bytes] == 0} else {
                        fail "Full forkless snapshot memory did not drain"
                    }
                }
                r debug reload nosave
                assert_equal $expected [debug_digest]
                assert_equal 0 [r exists born-after-start]
                r config set rdb-key-save-delay 0
            }
        }
    }

    start_server [list tags {forkless bgsave} overrides [list save {} bgsave-mode $mode stop-writes-on-bgsave-error no]] {
        test "$mode aborted first save still requires conservative recovery" {
            r debug populate 2000 failure-key 256
            r config set rdb-key-save-delay 1000
            r bgsave
            assert_equal 0 [status r forkless_progress_recovery]
            r set failure-key:0 changed
            if {$mode eq "forkless"} {
                r debug forkless-cancel
            } else {
                exec kill -9 [get_child_pid 0]
            }
            waitForBgsave r
            assert_equal err [status r rdb_last_bgsave_status]
            assert_equal 1 [status r forkless_progress_recovery]
            set expected [debug_digest]
            r bgsave
            # Full forkless explicitly normalizes marks before starting recovery.
            assert_equal [expr {$mode eq "hybrid"}] [status r forkless_progress_recovery]
            r set failure-key:0 changed-again
            r del failure-key:1
            waitForBgsave r
            assert_equal ok [status r rdb_last_bgsave_status]
            assert_equal 0 [status r forkless_progress_recovery]
            r debug reload nosave
            assert_equal $expected [debug_digest]
            r bgsave
            assert_equal 0 [status r forkless_progress_recovery]
            waitForBgsave r
            r config set rdb-key-save-delay 0
        }
    }
}
