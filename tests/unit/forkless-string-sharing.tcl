# Retained strings must protect both bytes and their frozen serialization
# header. Mutations happen in DB15 while the worker is held up by DB0.
start_server {tags {forkless bgsave} overrides {save {} bgsave-mode forkless stop-writes-on-bgsave-error no}} {
    test {forkless strings: untouched values retain references instead of copies} {
        r debug populate 1500 filler 128
        set copied [status r forkless_copied_values]
        set retained [status r forkless_retained_strings]
        set forks [status r total_forks]
        r bgsave
        waitForBgsave r
        assert_equal $copied [status r forkless_copied_values]
        assert {[status r forkless_retained_strings] >= $retained + 1500}
        assert_equal $forks [status r total_forks]
        wait_for_condition 100 10 {[status r forkless_snapshot_bytes] == 0} else {
            fail "retained string accounting did not drain"
        }
    }

    test {forkless strings: shared RAW EMBSTR INT bytes survive writes and header relocation} {
        r select 15
        foreach key {append range bit field replace rename unlink expire persist} {
            r set $key [string repeat x 4096]
        }
        r set embedded small
        r set integer 42
        r set float 1.5
        r pexpire persist 3600000
        r pfadd hll a b c d e
        set original_hll [r get hll]
        set expected [debug_digest]
        r config set rdb-key-save-delay 500
        r bgsave
        # Capture on metadata change, then mutate the already retained object.
        r pexpire append 3600000
        r append append tail
        r setrange range 8000 tail
        r setbit bit 50000 1
        r bitfield field SET u8 0 7
        r set replace changed
        r rename rename renamed
        r unlink unlink
        r pexpire expire 1
        r persist persist
        r append embedded tail
        r incr integer
        r incrbyfloat float 2.5
        assert_equal 5 [r pfcount hll]
        assert {$original_hll ne [r get hll]}
        waitForBgsave r
        assert_equal ok [status r rdb_last_bgsave_status]
        r config set rdb-key-save-delay 0
        r debug reload nosave
        assert_equal $expected [debug_digest]
        assert_equal $original_hll [r get hll]
        assert_equal -1 [r pttl append]
        assert {[r pttl persist] > 0}
    }

    test {forkless strings: cancellation releases shared references before retry} {
        r config set rdb-key-save-delay 1000
        r bgsave
        r append append changed
        r debug forkless-cancel
        waitForBgsave r
        wait_for_condition 100 10 {[status r forkless_snapshot_bytes] == 0} else {
            fail "cancelled retained strings did not drain"
        }
        assert_equal 1 [r object refcount append]
        r config set rdb-key-save-delay 0
        set expected [debug_digest]
        r bgsave
        waitForBgsave r
        r debug reload nosave
        assert_equal $expected [debug_digest]
    }
}

start_server {tags {forkless bgsave} overrides {save {} bgsave-mode hybrid}} {
    test {hybrid strings: reusable encoding scratch handles small and oversized frames} {
        r config set rdbcompression no
        r debug populate 1500 filler 128
        foreach size {31 1000 64000 66000 200000} {
            r set size:$size [string repeat x $size]
        }
        set expected [debug_digest]
        for {set round 0} {$round < 2} {incr round} {
            r bgsave
            waitForBgsave r
            assert_equal ok [status r rdb_last_bgsave_status]
            wait_for_condition 100 10 {[status r forkless_snapshot_bytes] == 0} else {
                fail "serializer scratch remains charged after completion"
            }
            r debug reload nosave
            assert_equal $expected [debug_digest]
        }
    }
}
