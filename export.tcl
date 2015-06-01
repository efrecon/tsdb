array set T2I {
    peers     {}
    levels    {1 CRITICAL 2 ERROR 3 WARN 4 NOTICE 5 INFO 6 DEBUG}
    verbose   0
    dtFormat  "%Y/%m/%d %H:%M:%S"
}

set rootdir [file normalize [file dirname [info script]]]
lappend auto_path [file join $rootdir .. lib] [file join $rootdir lib] \
    [file join $rootdir .. lib til] [file join $rootdir lib til]

package require tdb
package require tdb::export
package require http

set prg_args {
    -root    {%progdir%}         "Root directory for databases"
    -db      {db}                "TSDB database to export"
    -influx  ""                  "InfluxDB destination, e.g. http://localhost:8086/db/data/series"
    -kairos  ""                  "KairosDB destination (text proto), e.g. tcp://localhost:4242/"
    -v       0                   "Verbosity level \[0-6\]"
    -slots   720                 "Slots divider when fetching data, in mins. Negative to turn off"
    -dryrun  0                   "Turn on to dump InfluxDB JSON calls on stdout instead"
    -h       ""                  "Print this help and exit"
}

# Dump help based on the command-line option specification and exit.
proc ::help:dump { { hdr "" } } {
    global argv0

    if { $hdr ne "" } {
	puts $hdr
	puts ""
    }
    puts "NAME:"
    puts "\ttsdb2influx - Exports all tsdb data into an influx database"
    puts ""
    puts "USAGE"
    puts "\t[file tail $argv0] \[global options\]"
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
    global T2I

    if { $T2I(-v) >= $lvl } {
	array set L $T2I(levels)
	puts stderr "\[$L($lvl)\] \[$module\] $msg"
    }
}

array set T2I {}
foreach {arg val dsc} $prg_args {
    set T2I($arg) $val
}

if { [getopt argv -h] } {
    ::help:dump
}
for {set eaten ""} {$eaten ne $argv} {} {
    set eaten $argv
    foreach opt [array names T2I -*] {
	getopt argv $opt T2I($opt) $T2I($opt)
    }
}

# No remaining args? dump help and exit
if { [llength $argv] > 0 } {
    ::help:dump "$argv contains unknown options"
}


# Hook in log facility in tdb module
::tdb::verbosity $T2I(-v)
::tdb::logger ::log


set db [::tdb::create \
	    -name $T2I(-db) \
	    -root [string map [list %progdir% $rootdir] $T2I(-root)]]

if { $T2I(-influx) ne "" } {
    if { $T2I(-dryrun) } {
	set output [export influx $db "" $T2I(-slots)]
	puts stdout $output
    } else {
	export influx $db $T2I(-influx) $T2I(-slots)
    }
}
if { $T2I(-kairos) ne "" } {
    if { $T2I(-dryrun) } {
	set output [export kairos $db "" $T2I(-slots)]
	puts stdout $output
    } else {
	export kairos $db $T2I(-kairos) $T2I(-slots)
    }
}
