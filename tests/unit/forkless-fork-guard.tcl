set testmodule [file normalize tests/modules/fork.so]

start_server {tags {forkless modules external:skip} overrides {save {} bgsave-mode fork}} {
    r module load $testmodule
    set jemalloc [string match {*jemalloc*} [s mem_allocator]]
    set modes {forkless}
    if {$jemalloc} {lappend modes hybrid}

    foreach mode $modes {
        test "$mode: non-RDB forks respect actual arena inheritance" {
            r set arena-key original
            r config set bgsave-mode $mode
            if {$jemalloc && $::tcl_platform(os) eq "Linux"} {
                set forks [s total_forks]
                assert_error {*Fork failed*} {r fork.create 0 1}
                r script debug yes
                assert_error {*Fork() failed:*} {r eval {return redis.call('GET', 'arena-key')} 0}
                r script debug no
                assert_equal $forks [s total_forks]
                assert_equal original [r get arena-key]
                assert_equal PONG [r ping]
            } else {
                # Platforms without DONTFORK must not lose module-fork support.
                assert {[r fork.create 0 1] > 0}
                wait_for_condition 100 10 {[r fork.exitcode] == 0} else {
                    fail "module child did not complete"
                }
            }
            r config set bgsave-mode fork
            assert {[r fork.create 0 1] > 0}
            wait_for_condition 100 10 {[r fork.exitcode] == 0} else {
                fail "module fork did not recover after restoring fork mode"
            }
        }
    }
    r module unload fork
}
