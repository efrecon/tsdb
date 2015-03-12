array set T2I {
    peers     {}
    levels    {1 CRITICAL 2 ERROR 3 WARN 4 NOTICE 5 INFO 6 DEBUG}
    verbose   0
}

set rootdir [file normalize [file dirname [info script]]]
lappend auto_path [file join $rootdir .. lib] [file join $rootdir lib] \
    [file join $rootdir .. lib til] [file join $rootdir lib til]

package require tdb
package require http

set prg_args {
    -root    {%progdir%}         "Root directory for databases"
    -db      {db}                "TSDB database to export"
    -influx  {http://localhost:8086/db/data/series} "Influx destination"
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

proc ::dump {db serie sample start end} {
    global T2I

    ::tdb::log INFO "Pushing $sample in series $serie ($start->$end)"
    # Note that this creates a (possibly) gigantic JSON array with all
    # points for the sample, so this might be impractical from a
    # scaling point of view.
    set count 0
    set json ""
    append json "\["
    append json "\{"
    append json "\"name\": \"$serie\","
    append json "\"columns\": \[\"time\",\"$sample\"\],"
    append json "\"points\":\["
    foreach {tstamp val} [$db samples $serie $sample $end $start] {
	append json "\[$tstamp,"
	if { [string is integer -strict $val] \
		 || [string is boolean -strict $val] \
		 || [string is double -strict $val] } {
	    append json $val
	} else {
	    append json "\"$val\""
	}
	append json "\],"
	incr count
    }
    set json [string trimright $json ","];  # Not really a good idea...
    append json "\]"
    append json "\}"
    append json "\]"

    if { $T2I(-dryrun) } {
	puts stdout $json
    } else {
	set tok [::http::geturl $T2I(-influx) \
		     -type "application/json" \
		     -query $json]
	set code [::http::ncode $tok]
	if { $code >= 200 && $code < 300 } {
	    ::tdb::log INFO "Pushed $count value(s) for $sample"
	} else {
	    ::tdb::log WARN "Failed pushing data: $code, [http::error $tok]"
	}
	::http::cleanup $tok
    }
}

# Hook in log facility in tdb module
::tdb::verbosity $T2I(-v)
::tdb::logger ::log


set db [::tdb::create \
	    -name $T2I(-db) \
	    -root [string map [list %progdir% $rootdir] $T2I(-root)]]
foreach serie [$db series] {
    set nfo [$db info $serie]
    foreach sample [dict get $nfo samples] {
	if { [dict get [$db info $serie $sample] count] > 0 } {
	    if { $T2I(-slots) <= 0 } {
		dump $db $serie $sample \
		    [dict get $nfo earliest] [dict get $nfo latest]
	    } else {
		for {set tstart [dict get $nfo earliest]} \
		    {$tstart<[dict get $nfo latest]} \
		    {incr tstart $T2I(-slots)} {
			dump $db $serie $sample \
			    $tstart [expr {$tstart + $T2I(-slots)*60*1000}]
		}
	    }
	}
    }
}
