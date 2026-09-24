foreach timing {no yes} {
    start_server [list tags {forkless bgsave} overrides [list save {} bgsave-mode hybrid forkless-timing $timing rdbcompression no]] {
        test "hybrid timing $timing: repeated mixed snapshots remain valid" {
            for {set i 0} {$i < 256} {incr i} {
                r set small:$i [string repeat s 8192]
                r sadd big:$i [string repeat b 8192] $i
            }
            set expected [debug_digest]
            for {set round 0} {$round < 2} {incr round} {
                r bgsave
                waitForBgsave r
                assert_equal ok [status r rdb_last_bgsave_status]
                r debug reload nosave
                assert_equal $expected [debug_digest]
            }
            foreach field {forkless_helper_encode_usec forkless_helper_output_usec} {
                set value [status r $field]
                if {$timing eq "yes"} {
                    assert {$value > 0}
                } else {
                    assert_equal 0 $value
                }
            }
            set log [exec cat [srv 0 stdout]]
            if {$timing eq "yes"} {
                assert_match {*Hybrid timing: child_encode_with_output_usec=*child_output_usec=*} $log
                assert_match {*Hybrid timing: child_file_finalize_usec=*} $log
            } else {
                assert_no_match {*Hybrid timing:*} $log
            }
        }
    }
}
