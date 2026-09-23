start_server {tags {forkless bgsave repl} overrides {save {} bgsave-mode hybrid repl-diskless-sync no}} {
    test {hybrid: failed parent publication must not send the previous RDB} {
        r set version previous
        r save
        set dir [lindex [r config get dir] 1]
        set filename [lindex [r config get dbfilename] 1]
        set oldsize [file size [file join $dir $filename]]
        r set version current
        r debug populate 2000 publication 128
        r config set rdb-key-save-delay 1000
        set replica [redis_deferring_client]
        $replica psync ? -1
        assert_match {FULLRESYNC *} [$replica read]
        set tmp [file join $dir temp-forkless-[srv 0 pid].rdb]
        wait_for_condition 100 10 {[file exists $tmp]} else {
            fail "child did not open its temporary snapshot"
        }
        # The child can finish writing its open descriptor, but the parent
        # cannot rename the now-unlinked file. The previous RDB stays intact.
        file delete $tmp
        waitForBgsave r
        assert_equal err [status r rdb_last_bgsave_status]
        assert_equal $oldsize [file size [file join $dir $filename]]
        # A failed snapshot disconnects the replica; a falsely successful
        # publication would keep it connected and stream the old file.
        wait_for_condition 100 10 {[status r connected_slaves] == 0} else {
            fail "replica was not disconnected after publication failed"
        }
        $replica close
        r config set rdb-key-save-delay 0
        r bgsave
        waitForBgsave r
        assert_equal ok [status r rdb_last_bgsave_status]
    }
}
