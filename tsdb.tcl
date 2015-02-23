array set TSDB {
    peers     {}
    levels    {1 CRITICAL 2 ERROR 3 WARN 4 NOTICE 5 INFO 6 DEBUG}
    verbose   0
}

set rootdir [file normalize [file dirname [info script]]]
lappend auto_path [file join $rootdir .. lib] [file join $rootdir lib] \
    [file join $rootdir .. lib til] [file join $rootdir lib til]

package require tdb
package require tdb::graphite
package require tdb::influx

set prg_args {
    -root    {%progdir%}         "Root directory for databases"
    -dbs     {db}                "List of databases to create"
    -ports   {graphite:db:2003}  "Port mapping, support graphite and influx API for input"
    -v       0                   "Verbosity level \[0-6\]"
    -h       ""                  "Print this help and exit"
}

# Dump help based on the command-line option specification and exit.
proc ::help:dump { { hdr "" } } {
    if { $hdr ne "" } {
	puts $hdr
	puts ""
    }
    puts "NAME:"
    puts "\ttsdb - A simple timeseries database"
    puts ""
    puts "USAGE"
    puts "\ttsdb.tcl \[global options\]"
    puts ""
    puts "GLOBAL OPTIONS:"
    foreach { arg val dsc } $::prg_args {
	puts "\t${arg}\t$dsc (default: ${val})"
    }
    exit
}


proc ::getopt {_argv name {_var ""} {default ""}} {
    upvar $_argv argv $_var var
    set pos [lsearch -regexp $argv ^$name]
    if {$pos>=0} {
	set to $pos
	if {$_var ne ""} {
	    set var [lindex $argv [incr to]]
	}
	set argv [lreplace $argv $pos $to]
	return 1
    } else {
	# Did we provide a value to default?
	if {[llength [info level 0]] == 5} {set var $default}
	return 0
    }
}

proc ::log { lvl module msg} {
    global TSDB

    if { $TSDB(-v) >= $lvl } {
	array set L $TSDB(levels)
	puts stderr "\[$L($lvl)\] \[$module\] $msg"
    }
}


array set TSDB {}
foreach {arg val dsc} $prg_args {
    set TSDB($arg) $val
}

if { [getopt argv -h] } {
    ::help:dump
}
for {set eaten ""} {$eaten ne $argv} {} {
    set eaten $argv
    foreach opt [array names TSDB -*] {
	getopt argv $opt TSDB($opt) $TSDB($opt)
    }
}

# No remaining args? dump help and exit
if { [llength $argv] > 0 } {
    ::help:dump "$argv contains unknown options"
}

# Hook in log facility in tdb module
::tdb::verbosity $TSDB(-v)
::tdb::logger ::log


foreach dbname $TSDB(-dbs) {
    set db [::tdb::create \
		-name $dbname \
		-root [string map [list %progdir% $rootdir] $TSDB(-root)]]
    foreach ports $TSDB(-ports) {
	foreach {type tdb port} [split $ports ":"] break
	if { $tdb eq $dbname } {
	    switch $type {
		"graphite" {
		    ::tdb::graphite::server $db $port
		}
		"influx" -
		"http" {
		    ::tdb::influx::server $db $port
		}
		default {
		    ::tdb::log 1 "$type is not a known server API type"
		}
	    }
	}
    }
}

vwait forever
