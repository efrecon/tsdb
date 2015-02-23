package require Tcl 8.5

package require tdb::timeblock

namespace eval ::tdb {
    variable TDB
    if { ![::info exists TDB] } {
	array set TDB {
	    idGene         0
	    idClamp        10000
	    idFormat       7
	    logger         ""
	    dateLogHeader  "\[%Y%m%d %H%M%S\] \[%module%\] \[%level%\] "
	    verboseTags    {1 CRITICAL 2 ERROR 3 WARN 4 NOTICE 5 INFO 6 DEBUG}
	    -name          "db"
	    -chunk         262144
	    -root          .
	    -ext           ".db"
	}
	variable version 0.1
	variable libdir [file dirname [file normalize [::info script]]]
    }
    namespace export store info samples
    namespace ensemble create
}

proc ::tdb::create { args } {
    variable TDB

    set db [Identifier [namespace current]::db:]
    interp alias {} $db {} ::tdb::Dispatch $db
    upvar \#0 $db DB
    set DB(self) $db
    array set DB [array get TDB -*]
    foreach {k v} $args {
	set k -[string trimleft $k -]
	if { [::info exists DB($k)] } {
	    set DB($k) $v
	} else {
	    return -code error "$k is an unknown option"
	}
    }

    Init $db

    return $db
}


proc ::tdb::store { db series args } {
    variable TDB

    upvar \#0 $db DB

    array set SAMPLES $args
    set timestamp [clock milliseconds]
    if { [::info exists SAMPLES(timestamp)] } {
	set timestamp $SAMPLES(timestamp)
	unset SAMPLES(timestamp)
    }
    if { [::info exists SAMPLES(time)] } {
	set timestamp $SAMPLES(time)
	unset SAMPLES(time)
    }
    
    foreach column [array names SAMPLES] {
	StoreSample $db $series $column $SAMPLES($column) $timestamp
    }
}


proc ::tdb::info { db series {sFilters *} } {
    upvar \#0 $db DB

    set sdir [SeriesDir $db $series 0]
    set earliest ""
    set latest ""
    set samples {}
    set count 0
    foreach s [glob -nocomplain -directory $sdir -tails -- $sFilters] {
	lappend samples $s
	foreach tb [timeblock::recap $db $series $s] {
	    upvar \#0 $tb TMB
	    incr count $TMB(-samples)
	    if { $earliest eq "" || $TMB(-start) < $earliest } {
		set earliest $TMB(-start)
	    }
	    if { $latest eq "" || $TMB(-end) > $latest } {
		set latest $TMB(-end)
	    }
	}
    }
    return [list samples $samples \
		earliest $earliest latest $latest count $count]
}


proc ::tdb::samples { db series sample {end -1000} {start ""}} {
    # Guess real start and stop
    if { $start eq "" } {
	set start [clock milliseconds]
    }
    
    if { $end < 0 } {
	set end [expr {$start+$end}]
    }
    if { $start > $end } {
	foreach {start end} [list $end $start] break
    }
    
    set samples {}
    foreach tb [timeblock::recap $db $series $sample] {
	set samples [concat $samples [timeblock::samples $tb $start $end]]
    }
    return $samples
}


# ::tdb::logger -- Set logger command
#
#       Arrange for a command to receive logging messages.  The
#       command will receive two more arguments which will be the
#       integer logging level and the message.  Lower numbers are for
#       critical messages, the higher the number is, the less
#       important it is.
#
# Arguments:
#	cmd	New log command, empty to revert to dump on stderr.
#
# Results:
#       None.
#
# Side Effects:
#       None.
proc ::tdb::logger { { cmd "" } } {
    variable TDB
    set TDB(logger) $cmd
}


# ::tdb::verbosity -- Get or Set verbosity
#
#       Get or set the verbosity level of the module.  By default,
#       unless this is changed, the module will be totally silent.
#       But verbosity can be turned up for debugging purposes.
#
# Arguments:
#	lvl	New level to set (a positive integer, or a recognised string)
#
# Results:
#       The resulting level that was set, or an error.  When called
#       with no argument or an empty level (or a negative level), this
#       will be returning the current level.
#
# Side Effects:
#       Will output timestamped messages on stderr, unless a log
#       command is specified.
proc ::tdb::verbosity { {lvl -1} } {
    variable TDB

    if { $lvl >= 0 || $lvl ne "" } {
	set lvl [LogLevel $lvl]
	if { $lvl < 0 } { 
	    return -code error "Verbosity level $lvl not recognised"
	}
	set TDB(verbose) $lvl
    }
    return $TDB(verbose)
}



# ::tdb::LogLevel -- Convert log levels
#
#       For convenience, log levels can also be expressed using
#       human-readable strings.  This procedure will convert from this
#       format to the internal integer format.
#
# Arguments:
#	lvl	Log level (integer or string).
#
# Results:
#       Log level in integer format, -1 if it could not be converted.
#
# Side Effects:
#       None.
proc ::tdb::LogLevel { lvl } {
    variable TDB

    if { ![string is integer $lvl] } {
	foreach {l str} $TDB(verboseTags) {
	    if { [string match -nocase $str $lvl] } {
		return $l
	    }
	}
	return -1
    }
    return $lvl
}


# ::tdb::log -- Conditional Log output
#
#       This procedure will output the message passed as a parameter
#       if the logging level of the module is set higher than the
#       level of the message.  The level can either be expressed as an
#       integer (preferred) or a string pattern.
#
# Arguments:
#	lvl	Log level (integer or string).
#	msg	Message
#
# Results:
#       None.
#
# Side Effects:
#       Will either callback the logger command or output on stderr
#       whenever the logging level allows.
proc ::tdb::log { lvl msg { module "" } } {
    variable TDB
    global argv0

    # Convert to integer
    set lvl [LogLevel $lvl]
    
    # If we should output, either pass to the global logger command or
    # output a message onto stderr.
    if { [LogLevel $TDB(verbose)] >= $lvl } {
	if { $module eq "" } {
	    set proc [lindex [::info level -1] 0]
	    set proc [string map [list "::" "/"] $proc]
	    set module [lindex [split $proc "/"] end-1]
	    if { $module eq "" } {
		set module [file rootname [file tail $argv0]]
	    }
	}
	if { $TDB(logger) ne "" } {
	    # Be sure we didn't went into problems...
	    if { [catch {eval [linsert $TDB(logger) end \
				   $lvl $module $msg]} err] } {
		puts $TDB(logd) "Could not callback logger command: $err"
	    }
	} else {
	    # Convert the integer level to something easier to
	    # understand and output onto TDB(logd) (which is stderr,
	    # unless this has been modified)
	    array set T $TDB(verboseTags)
	    if { [::info exists T($lvl)] } {
		set log [string map [list \
					 %level% $T($lvl) \
					 %module% $module] \
			     $TDB(dateLogHeader)]
		set log [clock format [clock seconds] -format $log]
		append log $msg
		puts $TDB(logd) $log
	    }
	}
    }
}


proc ::tdb::Dispatch { db method args } {
    if {[lsearch -exact [namespace export] $method] < 0} {
	return -code error \
	    "Bad method $method: must be one of [join [namespace export] ,]"
    }
    if {[catch {eval [linsert $args 0 $method $db]} msg]} {
	return -code error $msg
    }
    return $msg
}


# ::tdb::Identifier -- Create an identifier
#
#       Create a unique identifier within this namespace.
#
# Arguments:
#	pfx	String to prefix to the name of the identifier
#
# Results:
#       A unique identifier
#
# Side Effects:
#       None.
proc ::tdb::Identifier { {pfx "" } } {
    variable TDB
    
    set unique [incr TDB(idGene)]
    ::append unique [expr {[clock clicks -milliseconds] % $TDB(idClamp)}]
    return [format "${pfx}%.$TDB(idFormat)d" $unique]
}


proc ::tdb::SeriesDir { db series { create 1 } } {
    upvar \#0 $db DB

    set dir [file join $DB(-root) $DB(-name) $series]
    if {![file isdirectory $dir] && $create } {
	file mkdir $dir
    }
    return $dir
}


proc ::tdb::SamplesDir { db series samples { create 1 } } {
    set dir [file join [SeriesDir $db $series $create] $samples]
    if {![file isdirectory $dir] && $create } {
	file mkdir $dir
    }
    return $dir
}


proc ::tdb::StoreSample { db series sample value {time ""} } {
    upvar \#0 $db DB
    
    timeblock::recap $db $series $sample
    if { $time eq "" } {
	set time [clock milliseconds]
    }

    set tb [timeblock::block $db $series $sample $time]
    if { $tb eq "" } {
	set tb [timeblock::new $db $series $sample $time]
    }
    timeblock::insert $tb $time $value
}


proc ::tdb::Existing { db series sample } {
    upvar \#0 $db DB

    set existing {}
    set sdir [SamplesDir $db $series $sample 0]
    foreach fname [glob -nocomplain \
		       -directory $sdir \
		       -tails \
		       -- \
		       *.[string trimleft $DB(-ext) .]] {
	lappend existing [file rootname $fname]
    }
    return [lsort -decreasing $existing]
}


proc ::tdb::Init { db } {
    variable TDB

    upvar \#0 $db DB
    set dir [file join $DB(-root) $DB(-name)]
    if { ![file isdirectory $dir] } {
	file mkdir $dir
    }
}


package provide tdb $::tdb::version
