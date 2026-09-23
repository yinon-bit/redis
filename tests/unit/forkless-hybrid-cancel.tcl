start_server {tags {forkless bgsave} overrides {save {} bgsave-mode hybrid}} {
    test "hybrid: cancellation does not join a helper waiting on a stopped child" {
        r debug populate 200000 cancel-key 1024
        r bgsave
        set child [get_child_pid 0]
        after 20
        exec kill -STOP $child
        # An independent process guarantees release even if the old blocking
        # join prevents the Tcl client from receiving its FLUSHALL reply.
        exec sh -c "sleep 4; kill -CONT $child 2>/dev/null || true" &
        after 100
        set started [clock milliseconds]
        assert_equal OK [r flushall async]
        assert_equal PONG [r ping]
        set elapsed [expr {[clock milliseconds] - $started}]
        puts "Cancellation command and PING: $elapsed ms"
        assert {$elapsed < 2000}
        assert_equal OK [r set available during-cancellation]
        waitForBgsave r
        wait_for_condition 200 25 {
            [status r forkless_snapshot_bytes] == 0
        } else {fail "Cancelled hybrid state did not drain"}
        r config set rdb-key-save-delay 0
        r bgsave
        waitForBgsave r
        assert_equal ok [status r rdb_last_bgsave_status]
        r debug reload nosave
        assert_equal during-cancellation [r get available]
    }
}
