start_server {tags {forkless bgsave} overrides {save {} bgsave-mode hybrid}} {
    test {hybrid: pins drain without child work and retain snapshot identities} {
        # DB 0 keeps the child away from DB 15 before we stop it. All filler
        # strings exceed the hybrid cap, so no helper records precede DB 15.
        r select 0
        r debug populate 20 big-blocker 600000
        r select 15
        r set replaced [string repeat a 32768] px 600000
        r set swapped [string repeat b 32768]
        r hset hash field old
        set expected [debug_digest]
        r config set rdb-key-save-delay 100000
        set early [status r forkless_early_keys]
        r bgsave
        set child [get_child_pid 0]
        exec kill -STOP $child
        set failed [catch {
            r set replaced replacement
            r del replaced
            r set replaced recreated
            r hset hash field changed
            r swapdb 14 15
            r select 14
            r set swapped changed
            # Post-boundary keys must never enter early output.
            for {set i 0} {$i < 100} {incr i} {
                r set new:$i first
                r set new:$i second
                r del new:$i
                r set new:$i final
            }
            wait_for_condition 200 5 {
                [status r forkless_early_keys] == $early + 3 &&
                [status r forkless_retained_bytes] == 0
            } else {fail "helper did not drain three pins with the child stopped"}
            assert_equal 3 [status r forkless_pin_records]
            assert_equal 1 [status r rdb_bgsave_in_progress]
        } message options]
        catch {exec kill -CONT $child}
        if {$failed} {return -options $options $message}
        waitForBgsave r
        assert_equal ok [status r rdb_last_bgsave_status]
        assert_equal 0 [status r forkless_pin_records]
        r config set rdb-key-save-delay 0
        r debug reload nosave
        assert_equal $expected [debug_digest]
        r select 15
        assert {[r pttl replaced] > 0}
        assert_equal old [r hget hash field]
    }

    test {hybrid: failed-save recovery disables early pins then re-enables them} {
        r select 15
        r config set rdb-key-save-delay 100000
        r bgsave
        set child [get_child_pid 0]
        exec kill -USR1 $child
        waitForBgsave r
        assert_equal 1 [status r forkless_progress_recovery]
        set expected [debug_digest]
        set early [status r forkless_early_keys]
        r bgsave
        r set replaced recovery-change
        r set new-during-recovery first
        r set new-during-recovery second
        waitForBgsave r
        assert_equal ok [status r rdb_last_bgsave_status]
        assert_equal $early [status r forkless_early_keys]
        assert_equal 0 [status r forkless_progress_recovery]
        r config set rdb-key-save-delay 0
        r debug reload nosave
        assert_equal $expected [debug_digest]
    }

    test {hybrid: full early queue and blocked output fall back without losing keys} {
        r select 15
        for {set i 0} {$i < 1000} {incr i} {
            r set pressure:$i [string repeat x 1024]
        }
        set expected [debug_digest]
        r config set rdbcompression no
        r config set rdb-key-save-delay 1000
        r bgsave
        set child [get_child_pid 0]
        exec kill -STOP $child
        set failed [catch {
            # More data than the output pipe and early FIFO can hold. Main
            # must still handle writes; excess pins await ordinary child work.
            for {set i 0} {$i < 1000} {incr i} {r set pressure:$i changed}
            assert_equal PONG [r ping]
            assert_equal 1 [status r rdb_bgsave_in_progress]
        } message options]
        catch {exec kill -CONT $child}
        if {$failed} {return -options $options $message}
        waitForBgsave r
        assert_equal ok [status r rdb_last_bgsave_status]
        r config set rdb-key-save-delay 0
        r debug reload nosave
        assert_equal $expected [debug_digest]
        r config set rdbcompression yes
    }
}
