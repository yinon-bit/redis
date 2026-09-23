start_cluster 1 0 {tags {external:skip cluster forkless} overrides {save {} cluster-require-full-coverage no}} {
    set master [srv 0 client]
    test {hybrid: snapshot excludes a slot whose active trim already started} {
        $master config set bgsave-mode hybrid
        $master debug asm-trim-method active 1000
        for {set i 0} {$i < 2000} {incr i} {
            $master set "{06S}:$i" small
        }
        $master set "{Qi}:survivor" yes
        # Remove ownership but retain the physical keys until active trim.
        $master cluster delslots 0
        $master trimslots ranges 1 0 0
        $master client pause 60000 write
        assert_error {*pending trim job*} {$master cluster setslot 0 stable}
        $master bgsave
        waitForBgsave $master
        assert_equal ok [status $master rdb_last_bgsave_status]
        set dir [lindex [$master config get dir] 1]
        set name [lindex [$master config get dbfilename] 1]
        set report [exec src/redis-check-rdb [file join $dir $name]]
        assert_match {*1 keys read*} $report
        $master client unpause
    }
}
