namespace eval ::tdb::graphite {
    variable version 0.1
}

proc ::tdb::graphite::server { db {port 2003} } {
    upvar \#0 $db DB
    ::tdb::log 4 "Starting graphite server for database $DB(-name)\
                  on port $port"
    set fd [socket -server [list ::tdb::graphite::Accept $db] $port]
}


proc ::tdb::graphite::Accept { db sock ip port } {
    ::tdb::log 5 "New incoming graphite client on ${ip}:${port}"
    fileevent $sock readable [list ::tdb::graphite::Read $db $sock]
}

proc ::tdb::graphite::Read { db sock } {
    set line [gets $sock]
    if { [string trim $line] eq "" } {
	::tdb::log 5 "Client closed socket connection"
	close $sock
    } else {
	set path [lindex $line 0]
	set timestamp [lindex $line end]
	set val [lrange $line 1 end-1]
	
	set psplit [split $path .]
	set series [lindex $psplit 0]
	set sample [join [lrange $psplit 1 end] .]
	::tdb::log 6 "Storing in $series: $sample = $val"
	$db store $series $sample $val timestamp [expr {$timestamp*1000}]
    }
}

package provide tdb::graphite $::tdb::graphite::version