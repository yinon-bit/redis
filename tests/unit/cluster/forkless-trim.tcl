# SFLUSH is experimental and excluded from the default command table.
start_cluster 1 0 {tags {external:skip cluster forkless experimental}} {
    set master [srv 0 client]
    $master config set save {}
    foreach mode {hybrid forkless} {
        test "$mode: SFLUSH ASYNC cancels snapshot before detaching slot dictionaries" {
            $master config set bgsave-mode $mode
            $master config set rdb-key-save-delay 1000
            $master debug populate 10000 trim-snapshot 128
            $master bgsave
            assert_equal 1 [status $master rdb_bgsave_in_progress]
            $master sflush 0 16383 async
            wait_for_condition 500 10 {
                [status $master rdb_bgsave_in_progress] == 0 &&
                [status $master lazyfree_pending_objects] == 0 &&
                [status $master forkless_snapshot_bytes] == 0
            } else {fail "slot trim and cancelled snapshot did not drain"}
            assert_equal 0 [$master dbsize]
            assert_equal PONG [$master ping]
            $master config set rdb-key-save-delay 0
            $master set survived yes
            $master bgsave
            waitForBgsave $master
            assert_equal ok [status $master rdb_last_bgsave_status]
            $master del survived
        }
    }
}
