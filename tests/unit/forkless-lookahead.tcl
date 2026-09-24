start_server {tags {forkless bgsave} overrides {save {} bgsave-mode hybrid rdbcompression no}} {
    foreach workload {small big mixed} {
        test "hybrid look-ahead: $workload queues wrap and snapshot survives writes" {
            r flushall
            # Both queues wrap, partial batches cross database boundaries,
            # and uncompressed values apply return-pipe backpressure.
            foreach db {0 3} {
                r select $db
                for {set i 0} {$i < 513} {incr i} {
                    if {$workload eq "big" || ($workload eq "mixed" && $i % 4 == 0)} {
                        # A set is child-owned even when compact.
                        r sadd key:$i [string repeat b 8192] $i
                    } else {
                        r set key:$i [string repeat s 8192]
                    }
                }
            }
            set expected [debug_digest]
            r config set rdb-key-save-delay 1000
            r bgsave
            foreach db {0 3} {
                r select $db
                for {set i 0} {$i < 513} {incr i} {
                    r del key:$i
                    r set key:$i changed
                }
                r set created-after-save new
            }
            waitForBgsave r
            assert_equal ok [status r rdb_last_bgsave_status]
            r config set rdb-key-save-delay 0
            r debug reload nosave
            assert_equal $expected [debug_digest]
        }
    }
}
