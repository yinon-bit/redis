start_server {tags {forkless bgsave} overrides {save {} bgsave-mode hybrid}} {
    test "hybrid: SET crossing the cap retains the old small value" {
        r select 0
        r debug populate 2000 cap-filler 128
        r select 15
        set original [string repeat s 256]
        r set cap-transition $original px 600000
        r config set rdb-key-save-delay 1000
        set copied [status r forkless_copied_values]
        set retained [status r forkless_retained_strings]
        r bgsave
        r set cap-transition [string repeat b 600000] keepttl
        assert_equal $copied [status r forkless_copied_values]
        assert_equal [expr {$retained + 1}] [status r forkless_retained_strings]
        assert_equal 600000 [r strlen cap-transition]
        # Replacement/deletion must not lose the original child work identity.
        r del cap-transition
        r set cap-transition new-value
        waitForBgsave r
        assert_equal ok [status r rdb_last_bgsave_status]
        r config set rdb-key-save-delay 0
        r debug reload nosave
        assert_equal $original [r get cap-transition]
        assert {[r pttl cap-transition] > 0}
        r flushall
        r select 0
    }
    foreach lazy {no yes} {
        test "hybrid: SET retains RAW strings without copying, lazy=$lazy" {
            r flushall
            r config set lazyfree-lazy-server-del $lazy
            r select 0
            r debug populate 2000 filler 128
            r select 15
            set original [string repeat a 32768]
            r set replace $original px 600000
            r set rejected $original
            r set appended $original
            r set small [string repeat s 256]
            r config set rdb-key-save-delay 1000
            set copied [status r forkless_copied_values]
            set retained [status r forkless_retained_strings]
            r bgsave
            assert_equal 1 [status r rdb_bgsave_in_progress]

            # Rejected SET must not create a pin or copy, and a successful
            # full replacement needs only one reference to the old value.
            assert_equal {} [r set rejected ignored nx]
            assert_equal $copied [status r forkless_copied_values]
            assert_equal $retained [status r forkless_retained_strings]
            r set replace replacement keepttl
            assert_equal $copied [status r forkless_copied_values]
            assert_equal [expr {$retained + 1}] [status r forkless_retained_strings]

            # Tiny strings keep the original copy/header-reuse fast path.
            r set small replacement
            assert_equal [expr {$copied + 1}] [status r forkless_copied_values]

            # A later mutation of the rejected key must still preserve the
            # original bytes; normal in-place writes remain copy-on-write.
            r append rejected suffix
            r setrange appended 0 changed
            assert {[status r forkless_copied_values] >= $copied + 3}
            r del replace
            r set replace born-after-start
            waitForBgsave r
            assert_equal ok [status r rdb_last_bgsave_status]
            r config set rdb-key-save-delay 0
            r debug reload nosave
            foreach key {replace rejected appended} {
                assert_equal $original [r get $key]
            }
            assert {[r pttl replace] > 0}
            assert_equal -1 [r pttl rejected]
            assert_equal [string repeat s 256] [r get small]
            wait_for_condition 100 10 {
                [status r forkless_retained_bytes] == 0 &&
                [status r forkless_snapshot_bytes] == 0
            } else {fail "snapshot references or scratch buffer leaked"}
        }
    }
}

foreach mode {hybrid forkless} {
    start_server [list tags {forkless bgsave} overrides [list save {} bgsave-mode $mode]] {
        test "$mode: asynchronous flush safely drains shared snapshot references" {
            r config set rdb-key-save-delay 1000
            for {set round 0} {$round < 3} {incr round} {
                r debug populate 50000 flush-refs 128
                r bgsave
                # In hybrid this captures replacement references; in forkless
                # it also overlaps scan-owned references to still-live keys.
                for {set i 0} {$i < 1000} {incr i} {
                    r set flush-refs:$i changed
                }
                assert {[status r forkless_retained_bytes] > 0}
                r flushdb async
                assert_equal 0 [r dbsize]
                waitForBgsave r
                wait_for_condition 500 10 {
                    [status r lazyfree_pending_objects] == 0 &&
                    [status r forkless_retained_bytes] == 0 &&
                    [status r forkless_snapshot_bytes] == 0
                } else {fail "async DB free and snapshot cleanup did not drain"}
                assert_equal PONG [r ping]
            }
            r config set rdb-key-save-delay 0
            r set recovered yes
            r bgsave
            waitForBgsave r
            r debug reload nosave
            assert_equal yes [r get recovered]
        }
    }
}
