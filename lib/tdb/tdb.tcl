##################
## Module Name     --  tdb
## Original Author --  Emmanuel Frecon - emmanuel@sics.se
## Description:
##
##    This implements a timeseries database using plain text files.  A
##    database is contained in a main directory.  This directory can
##    contain a number of "series" (think data series), each series
##    will lead to the creation of a sub-directory.  Series can
##    contain any sample, where a sample is a key and a value (can be
##    of any type) at a given time.  Storage-wise, sample names will
##    generate yet another sub-directory and this directory will
##    contain one or several files, containing the timestamps and
##    values.  The name of these files is used to pinpoint the
##    timestamp of the earliest value that it contains and the library
##    arranges for keeping the size of those files under some control.
##    Management of the files is performed by the (internal) library
##    ::tdb::timeblock.
##
## Commands Exported:
##      ::tdb::create
##################
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
	    -chunk         131072
	    -root          .
	    -ext           ".db"
	}
	variable version 0.1
	variable libdir [file dirname [file normalize [::info script]]]
    }
    namespace export store info samples series
    namespace ensemble create
}

# ::tdb::create -- Create a new database
#
#       Creates a new database using the name and root directory
#       passed as (optional) arguments.  This will return a handle
#       that also is a command which should be used for all further
#       operations on the database, including insertions.  The command
#       takes a number of dash-led options with values, these are:
#	-root	Root directory on disk, this can be shared among several DBs
#	-name	Name of database, will result in the creation of a directory
#               with that name under the root directory.
#	-chunk	Maximum target size of samples chunks on disk
#	-ext	Extension to use for files containing the data.
#
# Arguments:
#	args	Dash-led options and arguments, see above.
#
# Results:
#       Returns a handle for the database, this is a command used for
#       tk-style calling conventions.
#
# Side Effects:
#       Will create sub-directories as necessary, reorder "dirty"
#       files, etc.
proc ::tdb::create { args } {
    variable TDB

    # Create an identifier, arrange for it to be a command.
    set db [Identifier [namespace current]::db:]
    interp alias {} $db {} ::tdb::Dispatch $db
    upvar \#0 $db DB
    set DB(self) $db
    # Inherit values from the arguments, make sure we pick up the
    # defaults from the main library variable.
    array set DB [array get TDB -*]
    foreach {k v} $args {
	set k -[string trimleft $k -]
	if { [::info exists DB($k)] } {
	    set DB($k) $v
	} else {
	    return -code error "$k is an unknown option"
	}
    }

    # Initialise the database, creating directories as necessary.
    Init $db

    return $db
}


# ::tdb::store -- Store samples
#
#       Store one or more samples in a series.  The samples to store
#       are picked up from the list of arguments, which should be an
#       even list: even values will be the name of a key, odd values
#       its value.  The special keys called time or timestamp can be
#       specified to pinpoint the sampling in time, otherwise the
#       current time will be used.  When specifying a timestamp, this
#       should be expressed in the number of milliseconds since the
#       epoch.
#
# Arguments:
#	db	Database to store samples in.
#	series	Name of series in database
#	args	Even list of keys and values to store
#
# Results:
#       None.
#
# Side Effects:
#       None.
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


# ::tdb::series -- Return available series in DB
#
#       Inspect the disk to know which series are known in a given
#       database.
#
# Arguments:
#	db	Identifier of database (as of create)
#
# Results:
#       Returns list of (data) series in the database.
#
# Side Effects:
#       None.
proc ::tdb::series { db } {
    upvar \#0 $db DB
    
    set dir [file join $DB(-root) $DB(-name)]
    set series {}
    foreach d [glob -nocomplain -directory $dir -tails -- *] {
	if { [file isdirectory [file join $dir $d]] } {
	    lappend series $d
	}
    }

    return $series
}


# ::tdb::info -- Query database for information
#
#       This will query the database for high-level information about
#       the samples that it contains.  For the matching samples (see
#       arguments), this will return an even-list of keys and values
#       with the following keys:
#	samples 	List of (matching) samples
#	earliest	Earliest timestamp for all those samples
#	latest  	Latest timestamp for all those samples
#	count   	Number of samples
#
# Arguments:
#	db	Identifier of database (as of create)
#	series	(data) series for which to get info for
#	sFilter	Glob-style pattern to match existing samples on
#
# Results:
#       Return an even-list (that can be used for an array set) or
#       treated as a dictionary.  See above for its content.
#
# Side Effects:
#       None.
proc ::tdb::info { db series {sFilter *} } {
    upvar \#0 $db DB

    # Access directory for that data serie
    set sdir [SeriesDir $db $series 0]

    # Initialise variables to store global information
    set earliest ""
    set latest ""
    set samples {}
    set count 0
    
    # Dig onto the disk for samples matching the pattern, and for each
    # sample found gather its statistics and accumulate in global
    # information variables.
    foreach s [glob -nocomplain -directory $sdir -tails -- $sFilter] {
	# Account for the samples
	lappend samples $s

	# Then for each timeblock for that sample, accumulate
	# statistics.
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

    # Return a properly formed list.
    return [list samples $samples \
		earliest $earliest latest $latest count $count]
}


# ::tdb::samples -- Get samples
#
#       Get samples for a (data) series in a database.
#
# Arguments:
#	db	Identifier of database (as of create).
#	series	Name of (data) series to get samples from
#	sample	Name of sample to get values for
#	end	End timestamp, negative to express value FROM start
#	start	Start timestamp, empty for current time.
#
# Results:
#       Return an even-lengthed list where the first element is the
#       timestamp for the value and the second is the value for that
#       sample at that time.
#
# Side Effects:
#       None.
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

    # Now form a list with the values for that sample and within that
    # timespan.
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
	    if { [catch {::info level -1} caller] } {
		# Catches all errors, but mainly when we call log from
		# toplevel of the calling stack.
		set module [file rootname [file tail $argv0]]
	    } else {
		set proc [lindex $caller 0]
		set proc [string map [list "::" "/"] $proc]
		set module [lindex [split $proc "/"] end-1]
		if { $module eq "" } {
		    set module [file rootname [file tail $argv0]]
		}
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


####################################################################
#
# Procedures below are internal to the implementation, they shouldn't
# be changed unless you wish to help...
#
####################################################################


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


# ::tdb::Dispatch -- Library dispatcher
#
#       This is the dispatcher that is used to offer a tk-style
#       object-like API for the library on the database objects
#       created by ::tdb::create.
#
# Arguments:
#	db	Identifier of the database
#	method	Method to call (i.e. one of our recognised procs)
#	args	Arguments to pass to the procedure after the DB identifier.
#
# Results:
#      Whatever is returned by the called procedure.
#
# Side Effects:
#       None.
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
