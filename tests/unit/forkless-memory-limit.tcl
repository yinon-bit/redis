foreach mode {hybrid forkless} {
    start_server [list tags {forkless bgsave repl} overrides [list save {} \
        bgsave-mode $mode stop-writes-on-bgsave-error no \
        repl-diskless-sync yes repl-diskless-sync-delay 0]] {
        set master [srv 0 client]
        set master_host [srv 0 host]
        set master_port [srv 0 port]

        test "$mode: aggregate preserved-value admission budget" {
            $master debug populate 2000 budget-key 1024
            $master config set forkless-retained-max-memory 32768
            $master config set rdb-key-save-delay 1000
            $master bgsave
            for {set i 0} {$i < 2000 && [status $master forkless_limit_aborts] == 0} {incr i} {
                $master set budget-key:$i changed
            }
            waitForBgsave $master
            assert_equal 1 [status $master forkless_limit_aborts]
            assert_equal err [status $master rdb_last_bgsave_status]
            assert {[status $master forkless_retained_peak] <= 32768}
            assert {[status $master forkless_retained_peak] > 0}
            wait_for_condition 100 10 {
                [status $master forkless_retained_bytes] == 0
            } else {fail "Aborted snapshot references did not drain"}
        }

        start_server {overrides {save {}}} {
            set replica [srv 0 client]
            test "$mode: diskless memory-limit cancellation can recover" {
                $master config set forkless-retained-max-memory 1
                set aborted [status $master forkless_limit_aborts]
                $replica replicaof $master_host $master_port
                wait_for_condition 100 20 {
                    [status $master rdb_bgsave_in_progress] == 1
                } else {fail "Diskless snapshot did not start"}
                for {set i 0} {$i < 2000 && [status $master forkless_limit_aborts] == $aborted} {incr i} {
                    $master set budget-key:$i after-abort
                }
                if {$mode eq "forkless"} {
                    # Full forkless admits untouched copies as well. A fast
                    # replica may already have retried the failed snapshot.
                    assert {[status $master forkless_limit_aborts] > $aborted}
                } else {
                    assert_equal [expr {$aborted + 1}] [status $master forkless_limit_aborts]
                }
                $master config set forkless-retained-max-memory 0
                $master config set rdb-key-save-delay 0
                wait_for_condition 400 50 {
                    [status $replica master_link_status] eq {up} &&
                    [$master debug digest] eq [$replica debug digest]
                } else {fail "Diskless replica did not recover after memory-limit cancellation"}
            }
        }
    }
}
