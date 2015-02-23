package require minihttpd
package require tdb::json

namespace eval ::tdb::influx {
    variable version 0.1
}


proc ::tdb::influx::server { db {port 8086} } {
    upvar \#0 $db DB
    ::tdb::log 4 "Starting InfluxDB server for database $DB(-name)\
                  on port $port"
    set srv [::minihttpd::new "" $port]
    if { $srv < 0 } {
	::tdb::log 3 "Cannot start web server on port $port!"
	return
    }
    
    ::minihttpd::handler $srv /db/$DB(-name)/series \
	[list ::tdb::influx::Write $db] "application/json"
}

proc ::tdb::influx::Write { db prt sock url qry } {
    foreach q $qry {
	if { [string trim $q] ne "" } {
	    foreach wr [::tdb::json::parse [string trim $q]] {
		if { [dict exists $wr name] && [dict exists $wr columns] \
			 && [dict exists $wr points] } {
		    set series [dict get $wr name]
		    set cols [dict get $wr columns]
		    set len [llength $cols]
		    foreach pts [dict get $wr points] {
			if { [llength $pts] == $len } {
			    set samples {}
			    for {set i 0} {$i < $len} {incr i} {
				lappend samples \
				    [lindex $cols $i] [lindex $pts $i]
			    }
			    ::tdb::log 6 "Writing $samples for $series"
			    eval [linsert $samples 0 $db store $series]
			}
		    }
		} else {
		    ::tdb::log 3 "Wrong write request, one of name, columns\
                                  or points missing: $q"
		}
	    }
	}
    }
}


package provide tdb::influx $::tdb::influx::version