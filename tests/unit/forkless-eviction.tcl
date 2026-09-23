# Eviction overlap and snapshot metadata coverage for supported LRU/LFU policies.
foreach mode {fork hybrid forkless} {
    foreach policy {allkeys-lru allkeys-lfu volatile-lru volatile-lfu} {
        start_server [list tags {forkless eviction} overrides [list save {} bgsave-mode $mode maxmemory-policy $policy]] {
            test "$mode / $policy: evictions and writes overlap a correct snapshot" {
                r config set forkless-string-max 1024
                r config set lfu-decay-time 0
                r config set rdb-key-save-delay 1000
                for {set i 0} {$i < 2000} {incr i} {
                    # Mix child-owned and parent-owned values in hybrid.
                    set bytes [expr {$i % 10 == 0 ? 2048 : 512}]
                    r set key:$i [string repeat x $bytes] PX 3600000
                }
                r hset note-hash field original
                r set note-string [string repeat n 128]
                foreach key {note-string note-hash} {
                    set payload [r dump $key]
                    if {[string match *lfu $policy]} {
                        r restore $key 3600000 $payload REPLACE FREQ 100
                    } else {
                        r restore $key 3600000 $payload REPLACE IDLETIME 600
                    }
                }
                set expected [debug_digest]
                set used [status r used_memory]
                r config set maxmemory [expr {$used + 300000}]
                set before [status r evicted_keys]
                r bgsave
                assert_equal 1 [status r rdb_bgsave_in_progress]
                # Force private snapshot copies, including mutable hash and RAW SDS.
                r hset note-hash field changed
                r append note-string changed
                set successful 0
                set oom 0
                set overlap 0
                for {set i 0} {$i < 2000} {incr i} {
                    if {[catch {r set incoming:$i [string repeat y 2048] PX 3600000} err]} {
                        assert_match {*OOM*} $err
                        incr oom
                    } else {
                        incr successful
                    }
                    r get key:[expr {$i % 2000}]
                    if {$i % 50 == 0 && [status r rdb_bgsave_in_progress] && [status r evicted_keys] > $before} {
                        set overlap 1
                    }
                }
                assert {$successful > 0}
                assert_equal 1 $overlap
                set evicted [expr {[status r evicted_keys] - $before}]
                waitForBgsave r
                assert_equal ok [status r rdb_last_bgsave_status]
                # Avoid load-time eviction and do NOT replace the saved snapshot.
                r config set maxmemory 0
                r config set rdb-key-save-delay 0
                r debug reload nosave
                assert_equal $expected [debug_digest]
                foreach key {note-string note-hash} {
                    if {[string match *lfu $policy]} {
                        assert_equal 100 [r object freq $key]
                    } else {
                        set idle [r object idletime $key]
                        assert {$idle >= 598 && $idle <= 660}
                    }
                }
                puts "METRIC mode=$mode policy=$policy eviction_overlap=$overlap evicted=$evicted writes=$successful oom=$oom digest=match metadata=preserved"
            }
        }
    }
}

foreach mode {hybrid forkless} {
    foreach policy {allkeys-lru allkeys-lfu volatile-lru volatile-lfu} {
        start_server [list tags {forkless eviction repl} overrides [list save {} bgsave-mode $mode maxmemory-policy $policy repl-diskless-sync yes repl-diskless-sync-delay 0]] {
            set master [srv 0 client]
            set host [srv 0 host]
            set port [srv 0 port]
            $master config set forkless-string-max 1024
            $master config set rdb-key-save-delay 1000
            for {set i 0} {$i < 2000} {incr i} {
                $master set k:$i [string repeat a [expr {$i % 10 == 0 ? 2048 : 512}]] PX 3600000
            }
            $master config set maxmemory [expr {[status $master used_memory] + 300000}]
            set before [status $master evicted_keys]
            start_server {overrides {save {}}} {
                set replica [srv 0 client]
                test "$mode / $policy: diskless full sync under eviction converges" {
                    $replica replicaof $host $port
                    wait_for_condition 200 10 {[status $master rdb_bgsave_in_progress]} else {
                        fail "no diskless save observed"
                    }
                    set overlap 0
                    for {set i 0} {$i < 1500} {incr i} {
                        if {[catch {$master set k:$i [string repeat b 2048] PX 3600000} err]} {
                            assert_match {*OOM*} $err
                        }
                        $master get k:[expr {$i % 2000}]
                        if {$i % 50 == 0 && [status $master rdb_bgsave_in_progress] && [status $master evicted_keys] > $before} {
                            set overlap 1
                        }
                    }
                    assert_equal 1 $overlap
                    $master config set maxmemory 0
                    wait_for_condition 300 100 {
                        [string match {*master_link_status:up*} [$replica info replication]] &&
                        [$master debug digest] eq [$replica debug digest]
                    } else { fail "replica failed to converge under $policy" }
                    assert {[status $master sync_full] > 0}
                    puts "METRIC mode=$mode policy=$policy diskless=match eviction_overlap=$overlap"
                }
            }
        }
    }
}
