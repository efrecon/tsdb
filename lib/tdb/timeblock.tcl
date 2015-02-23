namespace eval ::tdb::timeblock {
    variable TMBLOCK
    if { ![::info exists TMBLOCK] } {
	array set TMBLOCK {
	    -start    0
	    -end      -1
	    -samples  0
	    -file     ""
	    -flags    {}
	    -bak      ".bak"
	    separator "-"
	}
    }
    variable version 0.1
}

proc ::tdb::timeblock::recap { db series sample } {
    variable TMBLOCK
    upvar \#0 $db DB

    set blocks {};   # Block initiated from disk
    set sdir [[namespace parent]::SamplesDir $db $series $sample 1]
    set existing [blocks $db $series $sample]

    set first 1
    foreach tgt [[namespace parent]::Existing $db $series $sample] {
	set bd [split [file rootname $tgt] "-"]
	set timestamp [lindex $bd 0]

	set found 0
	foreach tb $existing {
	    upvar \#0 $tb TMB
	    if { $TMB(-start) == $timestamp } {
		set found 1
		break
	    }
	}

	if { !$found } {
	    set tb [New $db $series $sample]
	    upvar \#0 $tb TMB
	    set TMB(-start) $timestamp
	    set TMB(-file) \
		[file join $sdir ${tgt}.[string trimleft $DB(-ext) .]]
	    set TMB(-flags) [lrange $bd 1 end]
	    Init $tb $first;   # Initialise from actual data on disk.
	}
	lappend blocks $tb
	set first 0
    }

    return $blocks
}


# Existing blocks
proc ::tdb::timeblock::blocks { db series sample } {
    set id [lindex [split $db ":"] end];  # Obj id of database
    set pfx [namespace current]::timeblock:$id:
    ::append pfx [string map [list ":" "_"] $sample]:*
    
    # Construct list to use for sorting
    set sorter {}
    foreach tb [info vars $pfx] {
	upvar \#0 $tb TMB
	lappend sorter [list $TMB(-start) $tb]
    }
    
    # Sort and extract in decreasing order, i.e. latest block first in
    # list.
    set blocks {}
    foreach s [lsort -decreasing -integer -index 0 $sorter] {
	lappend blocks [lindex $s 1]
    }
    return $blocks
}

# existing block to store at
proc ::tdb::timeblock::block { db series sample time } {
    upvar \#0 $db DB

    foreach tb [blocks $db $series $sample] {
	upvar \#0 $tb TMB
	if { $time >= $TMB(-start) } {
	    if { $DB(-chunk) > 0 && [file size $TMB(-file)] > $DB(-chunk) } {
		return "";  # Nowhere, too big
	    } else {
		return $tb
	    }
	}
    }
    return ""
}

proc ::tdb::timeblock::new { db series sample time } {
    set tb [New $db $series $sample]
    upvar \#0 $tb TMB
    upvar \#0 $TMB(db) DB
    set TMB(-start) $time
    set sdir [[namespace parent]::SamplesDir $db $series $sample 1]
    set TMB(-file) [file join $sdir ${time}.[string trimleft $DB(-ext) .]]
    set TMB(fd) [open $TMB(-file) w+]
    return $tb
}


proc ::tdb::timeblock::append { tb time val } {
    upvar \#0 $tb TMB
    
    if { $time > $TMB(-end) } {
	Append $tb $time $val
	return 1
    }
    return 0
}


proc ::tdb::timeblock::insert { tb time val } {
    upvar \#0 $tb TMB
    if { $time > $TMB(-end) } {
	return [append $tb $time $val]
    } else {
	if { ! [info exists [Cache $tb]] } {
	    upvar \#0 [Cache $tb] SAMPLES
	    array set SAMPLES [samples $tb]
	} else {
	    upvar \#0 [Cache $tb] SAMPLES
	}

	set SAMPLES($time) $val
	flag $tb dirty
	Append $tb $time $val
    }
}


proc ::tdb::timeblock::samples { tb { start 0 } { end -1 } } {
    upvar \#0 $tb TMB
    set samples {}
    if { $start <= $TMB(-end) || ($end >= 0 && $end < $TMB(-start))} {
	if { [info exists [Cache $tb]] } {
	    upvar \#0 [Cache $tb] SAMPLES
	    foreach tstamp [lsort -integer -increasing [array names SAMPLES]] {
		if { $tstamp >= 0 \
			 && (($end >= 0 && $tstamp <= $end) || $end < 0) } {
		    lappend samples $tstamp $SAMPLES($tstamp)
		}
	    }
	} else {
	    if { $TMB(fd) eq "" } {
		set TMB(fd) [open $TMB(-file) r+]
	    } else {
		seek $TMB(fd) 0
	    }
	    while {![eof $TMB(fd)]} {
		set line [gets $TMB(fd)]
		if { $line ne "" } {
		    foreach {tstamp val} $line break
		    if { $tstamp >= 0 \
			     && (($end >= 0 && $tstamp <= $end) || $end < 0) } {
			lappend samples $tstamp $val
		    }
		}
	    }
	}
    }
    return $samples
}


proc ::tdb::timeblock::select { tb sign { selected "" } { count_ "" } } {
    upvar \#0 $tb TMB
    if { $count_ ne "" } { upvar $count_ count}

    #### XXX: REWRITE WHEN CACHED!!!!
    set empty [string equal $selected ""]
    set res {}
    set count 0
    set fd [open $TMB(-file)]
    while {![eof $fd]} {
	set line [gets $fd]
	if { $line ne "" } {
	    incr count
	    foreach {tstamp val} $line break
	    if { $selected eq "" || [expr $tstamp $sign $selected]} {
		lappend res $tstamp $val
		if { $empty } {
		    set selected $tstamp
		}
	    }
	}
    }
    close $fd

    return $res
}

proc ::tdb::timeblock::latest { tb } {
    return [select $tb > ""]
}
proc ::tdb::timeblock::earliest { tb } {
    return [select $tb < ""]
}
proc ::tdb::timeblock::after { tb timestamp } {
    return [select $tb >= $timestamp]
}
proc ::tdb::timeblock::before { tb timestamp } {
    return [select $tb <= $timestamp]
}

proc ::tdb::timeblock::flag { tb flag } {
    upvar \#0 $tb TMB
    if { [lsearch $TMB(-flags) $flag] < 0 } {
	lappend TMB(-flags) $flag
	Flag $tb
    }
}


proc ::tdb::timeblock::unflag { tb flag } {
    upvar \#0 $tb TMB
    set idx [lsearch $TMB(-flags) $flag]
    if { $idx >= 0 } {
	set TMB(-flags) [lreplace $TMB(-flags) $idx $idx]
	Flag $tb
    }
}


proc ::tdb::timeblock::New { db series sample } {
    variable TMBLOCK
    upvar \#0 $db DB

    set id [lindex [split $db ":"] end];  # Obj id of database
    # Generate a good prefix for the internal identifier of the
    # timeblock.
    set pfx [namespace current]::timeblock:$id:
    ::append pfx [string map [list ":" "_"] $sample]:
    set tb [[namespace parent]::Identifier $pfx]
    upvar \#0 $tb TMB

    # Initialise timeblock
    set TMB(self) $tb
    set TMB(db) $db
    set TMB(fd) ""
    array set TMB [array get TMBLOCK -*]

    return $tb
}   


proc ::tdb::timeblock::Cache { tb } {
    upvar \#0 $tb TMB

    set id [lindex [split $TMB(db) ":"] end];  # Obj id of database
    return [string map [list timeblock:$id: samples:$id:] $tb]
}


proc ::tdb::timeblock::Append { tb time val } {
    upvar \#0 $tb TMB

    if { $TMB(fd) eq "" } {
	set TMB(fd) [open $TMB(-file) a+]
    }
    puts $TMB(fd) [list $time $val]
    flush $TMB(fd)

    if { $time > $TMB(-end) } {
	set TMB(-end) $time
    }
    incr TMB(-samples)
}


proc ::tdb::timeblock::Raw { tb } {
    upvar \#0 $tb TMB

    if { $TMB(fd) eq "" } {
	set TMB(fd) [open $TMB(-file) r+]
    } else {
	seek $TMB(fd) 0
    }
    set samples [read $TMB(fd)]

    return $samples
}


proc ::tdb::timeblock::Flag { tb } {
    variable TMBLOCK
    upvar \#0 $tb TMB

    set reopen 0
    if { $TMB(fd) ne "" } {
	set reopen 1
	close $TMB(fd)
    }

    set dirname [file dirname $TMB(-file)]
    set ext [file extension $TMB(-file)]
    set fname [join [concat $TMB(-start) $TMB(-flags)] $TMBLOCK(separator)]
    ::append fname $ext
    file rename -force -- $TMB(-file) [file join $dirname $fname]

    set TMB(-file) [file join $dirname $fname]
    if { $reopen } {
	set TMB(fd) [open $TMB(-file) a+]
    }
}

proc ::tdb::timeblock::Init { tb { keepopen 0 } } {
    upvar \#0 $tb TMB
    
    if { [lsearch $TMB(-flags) dirty] >= 0 } {
	Reorder $tb [expr {!$keepopen}]
	unflag $tb dirty
    } else {
	Sync $tb [expr {!$keepopen}]
    }
}


proc ::tdb::timeblock::Reorder { tb { close 0 } } {
    upvar \#0 $tb TMB

    set TMB(-end) -1
    set TMB(-samples) 0

    # Get current set of samples from file or memory
    if { [info exists [Cache $tb]] } {
	upvar \#0 [Cache $tb] SAMPLES
    } else {
	array set SAMPLES [Raw $tb]
    }

    # Force close, we'll create and open a new file.  This is
    # necessary since we might have SEVERAL entries for the same
    # timestamp at this point, meaning that if we only reorder in the
    # existing file, junk might appear at then end.
    if { $TMB(fd) ne "" } {
	close $TMB(fd)
	set TMB(fd) ""
    }

    # Creat and open new file
    set fname [file rootname $TMB(-file)].[string trimleft $TMB(-bak) .]
    set TMB(fd) [open $fname w+]

    # Dump into new file, gather statistics.
    foreach t [lsort -integer -increasing [array names SAMPLES]] {
	puts $TMB(fd) [list $t $SAMPLES($t)]
	# Account for end sample and number of samples.
	if { $t > $TMB(-end) } {
	    set TMB(-end) $t
	}
	incr TMB(-samples)
    }

    # Now make the new file OUR file and keep the file descriptor open
    # if relevant.
    file rename -force -- $fname $TMB(-file)

    if { $close } {
	close $TMB(fd)
	set TMB(fd) ""
    }
}


proc ::tdb::timeblock::Sync { tb { close 0 } } {
    upvar \#0 $tb TMB

    set TMB(-end) -1
    set TMB(-samples) 0

    if { $TMB(fd) eq "" } {
	set TMB(fd) [open $TMB(-file) r+]
    } else {
	seek $TMB(fd) 0
    }

    while {![eof $TMB(fd)]} {
	set line [gets $TMB(fd)]
	if { $line ne "" } {
	    foreach {tstamp val} $line break
	    if { $tstamp > $TMB(-end) } {
		set TMB(-end) $tstamp
	    }
	    incr TMB(-samples)
	}
    }

    if { $close } {
	close $TMB(fd)
	set TMB(fd) ""
    }
}


package provide tdb::timeblock $::tdb::timeblock::version
