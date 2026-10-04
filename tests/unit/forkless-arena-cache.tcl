start_server {tags {forkless bgsave} overrides {save {} bgsave-mode fork forkless-arena-tcache yes}} {
    test {arena cache is opt-in and startup-only} {
        assert_equal {forkless-arena-tcache yes} [r config get forkless-arena-tcache]
        assert_error {*immutable*} {r config set forkless-arena-tcache no}
    }

    test {arena cache survives mode changes, reallocations and repeated snapshots} {
        foreach mode {fork hybrid forkless fork hybrid} {
            r config set bgsave-mode $mode
            for {set i 0} {$i < 100} {incr i} {
                r set key:$i [string repeat a 256]
                r append key:$i [string repeat b 256]
                r hset hash:$i field [string repeat c 128]
            }
            r set crossing [string repeat d 256]
            r append crossing [string repeat e 600000]
            set expected [debug_digest]
            r bgsave
            waitForBgsave r
            assert_equal ok [status r rdb_last_bgsave_status]
            r debug reload nosave
            assert_equal $expected [debug_digest]
            r flushdb async
        }
    }

    test {arena cache preserves payloads across repeated raw string replacements} {
        r config set bgsave-mode hybrid
        r flushdb
        for {set i 0} {$i < 100} {incr i} {
            r set replace:$i [string repeat a 256] PX 600000
        }
        for {set round 0} {$round < 4} {incr round} {
            set payload [string repeat [format %c [expr {98 + $round}]] [expr {256 + 64 * $round}]]
            r bgsave
            for {set i 0} {$i < 100} {incr i} {
                r set replace:$i $payload KEEPTTL
                assert_equal $payload [r get replace:$i]
                assert {[r pttl replace:$i] > 0}
            }
            waitForBgsave r
            assert_equal ok [status r rdb_last_bgsave_status]
        }
        set expected [debug_digest]
        r bgsave
        waitForBgsave r
        r debug reload nosave
        assert_equal $expected [debug_digest]
    }
}
