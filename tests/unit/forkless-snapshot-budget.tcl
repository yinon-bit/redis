foreach mode {hybrid forkless} {
    start_server [list tags {forkless bgsave} overrides [list save {} \
        bgsave-mode $mode stop-writes-on-bgsave-error no]] {
        test "$mode: snapshot budget covers untouched encoding and preserves previous RDB" {
            r set original before
            r bgsave
            waitForBgsave r
            assert_equal ok [s rdb_last_bgsave_status]
            r set original after
            r set large [string repeat x 200000]
            r config set rdbcompression no
            r config set forkless-snapshot-max-memory 131072
            set denials [s forkless_snapshot_denials]
            r bgsave
            waitForBgsave r
            assert_equal err [s rdb_last_bgsave_status]
            assert {[s forkless_snapshot_denials] > $denials}
            wait_for_condition 100 10 {
                [s forkless_snapshot_bytes] == 0
            } else {fail "Failed snapshot leaked budget charges"}
            r debug reload nosave
            assert_equal before [r get original]
            assert_equal 0 [r exists large]
        }

        test "$mode: tracking growth admission fails safely during writes" {
            r debug populate 1000 budget 256
            r config set forkless-snapshot-max-memory 131072
            r config set rdb-key-save-delay 1000
            set denials [s forkless_snapshot_denials]
            r bgsave
            for {set i 0} {$i < 1000 && [s rdb_bgsave_in_progress]} {incr i} {
                r set budget:$i changed
            }
            waitForBgsave r
            assert_equal err [s rdb_last_bgsave_status]
            assert {[s forkless_snapshot_denials] > $denials}
            assert {[s forkless_retained_peak] > 4096}
            wait_for_condition 100 10 {
                [s forkless_snapshot_bytes] == 0
            } else {fail "Tracking failure leaked budget charges"}
            r config set forkless-snapshot-max-memory 0
            r config set rdb-key-save-delay 0
            r bgsave
            waitForBgsave r
            assert_equal ok [s rdb_last_bgsave_status]
            wait_for_condition 100 10 {
                [s forkless_snapshot_bytes] == 0
            } else {fail "Successful retry leaked budget charges"}
        }
    }
}
