namespace eval ::lockf {
    variable version 0.2;    # Current package version
    variable log     "";     # Should we output some logging info, and where?

    # Encapsulates variables global to this namespace under their own
    # namespace, an idea originating from http://wiki.tcl.tk/1489.
    # Variables which name start with a dash are options and which
    # values can be changed to influence the behaviour of this
    # implementation.
    namespace eval vars {
	# Extensions for lockfile generation and for hidden lock
	# information storage.
	variable -ext       ".lck"
	# Tokenising string when generating the lock information filename
	variable -tokeniser "~"
	# Sleep for this number of milliseconds when quick polling,
	# either a list (random value between min and max) or a single
	# value.
	variable -poller    {25 100}
	# Number of retries when polling to acquire lock.  Called
	# salves because we do this quickly.  Negative to turn this
	# off (only slow polling then).
	variable -salves    16
	# Sleep when incremental backoff slow polling (min and max) or
	# single value.  This is expressed in floating point seconds.
	variable -sleep      {2.0 32.0}
	# Increment when incremental backoff
	variable -increment  2.0
	# How many incremental backoffs polling retries (neg. == no limit)
	variable -retries    -1
	# Refresh period (in floating point seconds), negative to turn
	# off.  Beware: turning this off will basically render the
	# stale lock detection algrorithm useless!
	variable -refresh    8
	# The longest we will try (negative forever)
	variable -timeout    -1
	# Suspend time, i.e. time (in secs) we wait before stealing
	# locks.
	variable -suspend    1800
	# Stale in secs, unmodified files older that this are deamed
	# stale
	variable -stale      3600
	# Should we sweep away lockfiles that are on the same host
	# when their PID owner has gone?
	variable -sweep      on
        # Randomiser size for adding some differences to the size of
        # lock files.
        variable -randomiser 80
	# Impeder is a callback that decides how long to wait between
	# polling attempts.  It should return a number of milliseconds
	# or 0 on fatal errors.  It can use the internal
	# implementation impede.
	variable -impeder    ""
    }

    # Automatically export all procedures starting with lower case and
    # create an ensemble for an easier API.
    namespace export {[a-z]*}
    namespace ensemble create
}


# ::lockf::lockfile -- Generate a good name
#
#       This will generate a good name for a lockfile based on a file
#       that you would wish to control access to.
#
# Arguments:
#	fname	Path to base file
#
# Results:
#       Return the full normalised path to a file that can be used as
#       a lock control file for the file passed as a parameter.
#
# Side Effects:
#       None.
proc ::lockf::lockfile { fname } {
    set fname [file normalize $fname]
    set dirname [file dirname $fname]
    set rootname [file tail $fname]
    set lockf [file join $dirname .$rootname.[string trimleft ${vars::-ext} .]]
    Debug "Generated good lockfile name '$lockf' from $fname"
    return $lockf
}


# ::lockf::lock -- Lock on file
#
#       This procedure will arrange for a lock to be established based
#       on the file passed as a parameter.  No other process or thread
#       will be able to acquire the lock on the same file until you
#       have unlocked.  The exact algorithm is described else where.
#
# Arguments:
#	fname	Path to file to lock on.  When locking the file will be created!
#	args	List of dash-led options and their arguments, these should be
#               one of the variables described as part of the vars namespace.
#
# Results:
#       The number of milliseconds that it took to take the lock
#       (always non-zero) or 0 on failure.
#
# Side Effects:
#       Will create the file which path is passed as an argument to
#       store the lock on the host.  This will also create another
#       hidden file that is linked to the lock file.
proc ::lockf::lock { fname args } {
    # Generate a local object for the lockfile
    set l [namespace current]::lock[Hash $fname]
    upvar \#0 $l LCK

    # Store lockfile and default values (this will suppose that all
    # dash led variables in the vars sub-namespace are options that
    # can be given to the main lock function)
    set LCK(lockfile) $fname
    set LCK(id) $l
    set LCK(start) [clock milliseconds];   # When did we start locking?
    set LCK(sleeper) ""
    set LCK(refresher) ""
    foreach v [info vars vars::-*] {
	set k [namespace tail $v]
	set LCK($k) [set $v]
    }
    # Override with options from the procedure call, scream for unknown options
    foreach {k v} $args {
        set k -[string trimleft $k -]
        if { [info exists vars::$k] } {
            set LCK($k) $v
        } else {
	    return -code error "$k is an unknown option"
	}
    }

    # Sweep away old locks that are owned by the same host by do not
    # correspond to a running process anymore.
    if { [string is true $LCK(-sweep)] } {
	Sweep $l
    }

    # Generate the locking information file which we will try linking
    # to the lockfile
    Update $l

    set done 0
    set LCK(state) UNKNOWN

    while {!$done} {
	# Try linking, ignore all errors.
	if { [catch {file link -hard $LCK(lockfile) $LCK(locker)} err] == 0 } {
            if { [Same $LCK(locker) $LCK(lockfile)] } {
		Debug "Acquired lock at $LCK(lockfile)!"
		set LCK(state) LOCKED
		if { $LCK(-refresh) > 0 } {
		    set next [expr {int($LCK(-refresh)*1000)}]
		    Update $l $next;  # Start refreshing the lock
		}
		set elapsed [expr {[clock milliseconds]-$LCK(start)}]
		if { $elapsed == 0 } { set elapsed 1 }
		return $elapsed
	    }
	}

	set elapsed [expr {[clock milliseconds]-$LCK(start)}]
	if { $LCK(-impeder) ne "" } {
	    if { [catch {eval [linsert $LCK(-impeder) end \
				   $fname $elapsed]} ms] } {
		return -code error "Error when impeding lock acquisition: $ms!"
	    }
	} else {
	    set ms [impede $fname $elapsed]
	}
	
	if { $ms > 0 } {
	    Sleep $l $ms
	} else {
	    return 0; # Failure!
	}
    }

    return 0;  # Never reached
}


# ::lockf::impede -- Decide impeding timeout
#
#       Decide upon the impeding timeout that we should use this time
#       and before we try establishing the lock again.  This should
#       return a strictly positive number of milliseconds to
#       wait. Returning a negative or zero means that we will fail
#       acquiring the lock!
#
# Arguments:
#	fname	Path to lock file, must be the same as for lock procedure
#	elapsed	Number of milliseconds elapsed since we starting locking
#
# Results:
#       Number of milliseconds to wait, 0 or negative to mediate errors
#
# Side Effects:
#       None.
proc ::lockf::impede { fname elapsed } {
    # Generate a local object for the lockfile
    set l [namespace current]::lock[Hash $fname]
    if { ! [info exists $l] } {
	return 0
    }
    upvar \#0 $l LCK

    if { $LCK(-timeout) > 0 && $elapsed >= [expr {int($LCK(-timeout)*1000)}] } {
	Debug "We have been waiting too long for the lock on $LCK(lockfile)"
	return 0
    }

    set age [expr {[clock milliseconds]-[Modified $LCK(lockfile)]}]
    Debug "$LCK(lockfile) is $age ms old"
    if { $LCK(-stale) > 0 && $age >= [expr {int($LCK(-stale)*1000)}] } {
	if { $LCK(state) ne "SUSPENDING" && $LCK(-suspend) > 0 } {
	    Debug "Stale lock at $LCK(lockfile), giving it\
                   a respit of $LCK(-suspend) s."
	    set LCK(state) SUSPENDING
	    return [expr {int($LCK(-suspend)*1000)}]
	}
	Debug "Stealing the lock at $LCK(lockfile)"
	if { [Steal $LCK(lockfile)] } {
	    return [Shot $l];  # Mini respit
	} else {
	    return 0;  # Failure, we couldn't even steal the lock!
	}
    } else {
	# Wait for a while and try again
	return [Next $l]
    }

    return 0;
}


# ::lockf::cancel -- Cancel a pending lock operation
#
#       Cancel trying to acquire a lock.  All state will be lost upon
#       calling this procedure.
#
# Arguments:
#	fname	Path to lock file, must be the same as for lock procedure
#
# Results:
#       Return the total number of milliseconds that we have spent
#       trying to acquire the lock. 0 on errors.
#
# Side Effects:
#       Removes locking information file from the disk.
proc ::lockf::cancel { fname } {
    # access local object for the lockfile
    set l [namespace current]::lock[Hash $fname]
    if { ! [info exists $l] } {
	return 0
    }

    upvar \#0 $l LCK
    set tried 0
    if { $LCK(state) ne "LOCKED" } {
        set tried [Delete $l]
    }
    return $tried
}


# ::lockf::unlock -- Release a lock
#
#       Release the lock, which will also remove the lockfile passed
#       as a parameter.
#
# Arguments:
#	fname	Path to lock file, must be the same as for lock procedure
#
# Results:
#       Return the number of milliseconds that we've had the lock, 0 on errors.
#
# Side Effects:
#       Removes lockfile and locking information file from the disk.
proc ::lockf::unlock { fname } {
    # access local object for the lockfile
    set l [namespace current]::lock[Hash $fname]
    if { ! [info exists $l] } {
	return 0
    }

    upvar \#0 $l LCK
    set owned 0
    if { [locked $fname] } {
        set lockfile $LCK(lockfile)
        set owned [Delete $l]
	if { [catch {file delete -force -- $lockfile} err] } {
	    return -code error \
		"Could not properly remove lockfile at $lockfile: $err"
	}
	Debug "Released lock on $lockfile"
    } else {
	return [cancel $fname]
    }
    return $owned
}


# ::lockf::locked -- Do we own the lock?
#
#       Return whether we own the lock or not.
#
# Arguments:
#	fname	Path to lock file, must be the same as for lock procedure
#
# Results:
#       Return the number of milliseconds that we've had the lock for,
#       0 if we do not own.
#
# Side Effects:
#       Removes lockfile and locking information file from the disk.
proc ::lockf::locked { fname } {
    # access local object for the lockfile
    set l [namespace current]::lock[Hash $fname]
    if { ! [info exists $l] } {
	return 0
    }
    
    upvar \#0 $l LCK
    set elapsed [expr {[clock milliseconds]-$LCK(start)}]
    return [expr {$LCK(state) eq "LOCKED" ? $elapsed : 0}]
}


# ::lockf::owner -- Return owner info
#
#       Return low-level information about the owner of a lockfile.
#       This will be a dictionary that have the following keys:
#       host      Hostname of owner
#       pid       PID of owner
#       timestamp When was the information last updated (ms since epoch)
#       time      Same info as above, in human-readable form.
#
# Arguments:
#	fname	Path to a lockfile (not necessary one we own!)
#
# Results:
#       A dictionary, might be empty on problems.
#
# Side Effects:
#       None.
proc ::lockf::owner { fname } {
    # Read content of file
    if { [catch {open $fname} fd] == 0 } {
        set content [read $fd]
        # Carefully understand the content of the file as an array as
        # we might be reading garbled data.
        if { [catch {llength $content} len] == 0 } {
            if { $len % 2 == 0 } {
                return $content
            }
        }
	close $fd
    }
    Debug "Locker file content might have been garbled!"
    return {}
}


proc ::lockf::Same { f1 f2 {ino_diff 1} } {
    # Stat the file, if we fail for one of them, they can't be the
    # same...
    if { [catch {file lstat $f1 F1} err] } {
        return 0
    }
    if { [catch {file lstat $f2 F2} err] } {
        return 0
    }

    # Now compute ino difference
    set d [expr {$F2(ino) > $F1(ino) ? $F2(ino)-$F1(ino) : $F1(ino)-$F2(ino)}]

    # And return if they are on the same device, have the same size
    # and (ideally) have the same ino.  When behing SSHFS, we end up
    # having an ino diff of one, so we accept that as well and hope
    # that the size will make us fail otherwise.
    return [expr {$F1(dev) == $F2(dev) \
                      && $d <= $ino_diff \
                      && $F1(size) == $F2(size)}]
}


proc ::lockf::Delete { l } {
    upvar \#0 $l LCK

    # Compute howlong we've been waiting for the lock
    set now [clock milliseconds]
    set owned [expr {$now-$LCK(start)}]
    
    if { $LCK(refresher) ne "" } {
	after cancel $LCK(refresher)
    }
    if { $LCK(sleeper) ne "" } {
	after cancel $LCK(sleeper)
    }
    if { [catch {file delete -force -- $LCK(locker)} err] } {
	Debug "Left a remnant locker file at $LCK(locker)"
    }
    unset $l
    return $owned
}

proc ::lockf::Debug {str} {
    variable log
    
    if { $log ne "" } {
	set now [clock seconds]
	set when [clock format [clock seconds] -format "%Y%m%d %H%M%S"]
	puts $log "\[$when\] $str"
    }
}


proc ::lockf::Sleep { l { ms 200 } } {
    upvar \#0 $l LCK
    set LCK(sleeping) $ms
    set LCK(sleeper) [after $ms [list set ${l}(sleeping) 0]]
    Debug "Sleeping for $ms ms in the background"
    vwait ${l}(sleeping)
    set LCK(sleeper) ""
}


proc ::lockf::Next { l } {
    upvar \#0 $l LCK

    set ms -1
    switch $LCK(state) {
	"UNKNOWN" {
	    set LCK(attempts) 0
	    set LCK(salves) 0
	    set LCK(direction) 1
	    set LCK(next) 0
	    if { $LCK(-salves) > 0 } {
		set LCK(state) SHOT
	    } else {
		set LCK(state) SLOW
	    }
	    return [Next $l]
	}
	"SHOT" {
	    if { [incr LCK(salves)] >= $LCK(-salves) } {
		set LCK(salves) 0
		set LCK(state) SLOW
	    }
            # Decide how long to quickpoll
	    set ms [Shot $l]
            Debug "Quick shots for acquiring lock at $LCK(lockfile),\
                   waiting for $ms ms."
	}
	"SLOW" {
	    if { $LCK(-retries) > 0 && [incr LCK(attempts)] > $LCK(-retries)} {
                Debug "Tried slow polling too many times!"
	    } else {
		set ms [Slow $l]
                set LCK(state) SHOT;   # Next one(s) are shots again!
		Debug "Incremental polling for acquiring lock at\
                       $LCK(lockfile), waiting for $ms ms."
	    }
	}
    }

    return $ms
}

proc ::lockf::Shot { l } {
    upvar \#0 $l LCK

    # Decide how long to quickpoll
    foreach {min max} $LCK(-poller) break
    if { $max eq "" } {
	set ms $min
    } else {
	if { $max < $min } {
	    foreach {min max} [list $max $min] break
	}
	set ms [expr {int($min+rand()*($max-$min))}]
    }
    return $ms
}

proc ::lockf::Slow { l } {
    upvar \#0 $l LCK

    foreach {min max} $LCK(-sleep) break
    if { $max eq "" } {
	set ms $min
    } else {
	if { $max < $min } {
	    foreach {min max} [list $max $min] break
	}
	set next [expr {$min+$LCK(next)}]
	if { $next < $min } {
	    set LCK(next) 0
	    set LCK(direction) 1
	    set next $min
	} elseif { $next > $max } {
	    set LCK(next) [expr {$max-$min}]
	    set LCK(direction) -1
	    set next $max
	} else {
	    set LCK(next) [expr {$LCK(next)+$LCK(-increment)*$LCK(direction)}]
	}
	set ms [expr {int($next*1000)}]
    }
    return $ms
}


proc ::lockf::Modified { fname } {
    array set STATE [owner $fname]

    # Parse content of time information in UTC and return on success.
    if { [info exists STATE(time)] } {
	set usecs 0
	set dot [string last "." $STATE(time)]
	if { $dot >= 0 } {
	    set usecs [string trimleft [string range $STATE(time) $dot end] .]
	    incr dot -1
	    set STATE(time) [string range $STATE(time) 0 $dot]
	}
	if { [catch {clock scan $STATE(time) \
			 -format "%Y-%m-%d %H:%M:%S" \
			 -timezone :UTC} secs] == 0 } {
	    return [expr {$secs*1000+$usecs}]
	}
    }
    
    # All failures lead here, we'll use the date of the file instead.
    set secs [file mtime $fname]
    return [expr {$secs*1000}]
}


# ::lockf::Hash -- Return a hash for a string
#
#       Hash a string according to dr. KNUTH's art of programming
#       volume 3, adapted to be constrained to integers.
#
# Arguments:
#	str	String to hash
#	max	Maximum to constrain hashes to (use a prime!), neg. to turn off
#
# Results:
#       A "unique" hash for the string.
#
# Side Effects:
#       None.
proc ::lockf::Hash { str {max -1} } {
    set len [string length $str]
    set hash $len
    foreach c [split $str ""] {
	scan $c %c code
	set hash [expr {int((($hash<<5)^($hash>>27))^$code)}]
    }
    if { $max > 0 } {
	set hash [expr {$hash%$max}]
    }
    return $hash
}


proc ::lockf::Locker { l {force 0}} {
    upvar \#0 $l LCK
    if { ![info exists LCK(locker)] || [string is true $force] } {
	set dirname [file dirname [file normalize $LCK(lockfile)]]
	set basename [file tail [file rootname $LCK(lockfile)]]
	set now [clock milliseconds]
	set rnd [expr {int(rand()*65535)}]
	set module [string trim [namespace current] :]
	set locker [join [list .$module [info hostname] [pid] \
			      $basename $now $rnd] \
			$LCK(-tokeniser)]
        append locker .[string trimleft $LCK(-ext) .]
	set LCK(locker) [file join $dirname $locker]
	Debug "Locking information for $LCK(lockfile) will be\
               stored at $LCK(locker)"
    }

    return $LCK(locker)
}


proc ::lockf::Steal { fname } {
    array set STATE [owner $fname]
    if { [info exists STATE(locker)] } {
	catch {file delete -force -- $STATE(locker)} err
    }
    if { [info exists STATE(lockfile)] } {
	if { [catch {file delete -force -- $STATE(lockfile)} err] } {
	    Debug "Could not remove stale lockfile: $err"
	    return 0
	}
    }
    # Try removing the file directly in case we could not properly
    # read its content (but this should really be the same as
    # STATE(lockfile) above).
    if { [file exists $fname] } {
	if { [catch {file delete -force -- $fname} err] } {
	    Debug "Could not remove stale lockfile: $err"
	    return 0
	}
    }

    return 1
}


proc ::lockf::Owner { fname {tokeniser ""}} {
    array set STATE [owner $fname]

    # We have some proper content, return the information that was
    # stored in the locker file.
    if { [info exists STATE(host)] && [info exists STATE(pid)] } {
	if { [info exists STATE(lockfile)] } {
	    return [list $STATE(host) $STATE(pid) $STATE(lockfile)]
	} else {
	    return [list $STATE(host) $STATE(pid) ""]
	}
    }

    if { $tokeniser ne "" } {
	# Otherwise extract these from the name of the file, this should
	# match the names that we are creating in Locker.  However, we
	# won't be able to have a proper lockfile information...
	set sf [split [file tail $fname] $tokeniser]
	return [list [lindex $sf 1] [lindex $sf 2] ""]
    }

    return {"" "" ""}
}


proc ::lockf::Processes {{filter *}} {
    set processes {}
    if { [catch {open "|ps -aeo pid,comm"} fd] == 0 } {
	set skip 1
	while {![eof $fd]} {
	    set l [string trim [gets $fd]]
	    if { $skip } {
		# Skip header!
		set skip 0
	    } else {
		set pid [lindex $l 0]
		set cmd [lindex $l 1]
		if { [string match $filter $cmd] } {
		    lappend processes $pid
		}
	    }
	}
	catch {close $fd}
    }
    Debug "[llength $processes] process(es) matched '$filter'"

    return $processes
}


proc ::lockf::Update { l {next -1}} {
    upvar \#0 $l LCK

    # Generate the filename where to store locking information at.
    Locker $l

    # Generate what to store in the locking information file
    set STATE(host) [info hostname]
    set STATE(pid) [pid]
    set STATE(lockfile) $LCK(lockfile)
    set STATE(locker) $LCK(locker)
    set STATE(timestamp) [clock milliseconds]
    set secs [expr {int($STATE(timestamp)/1000)}]
    set usecs [expr {$STATE(timestamp)-$secs*1000}]
    set STATE(time) [clock format $secs \
			 -format "%Y-%m-%d %H:%M:%S.$usecs" \
			 -timezone :UTC]
    set STATE(randomiser) [string repeat $LCK(-tokeniser) \
                               [expr {int(rand()*$LCK(-randomiser))}]]

    # And dump info
    if { [catch {open $LCK(locker) w} fd] == 0 } {
	puts -nonewline $fd [array get STATE]
	close $fd
    } else {
	return -code error "Could not store locking information at $LCK(locker)"
    }

    if { $next >= 0 } {
	set LCK(refresher) \
	    [after $next [list [namespace current]::Update $l $next]]
    }
    Debug "Updated locking information for $LCK(lockfile) in $LCK(locker)"
}


proc ::lockf::Sweep { l } {
    upvar \#0 $l LCK
    
    set myhost [info hostname];    # Where we are?
    Debug "Sweeping away locks from '$myhost'..."

    set candidates {}

    # Gather PIDs (and filenames) of lockers that would be on our
    # host.  We only do this for files that look like they are ours
    # and we gather the lockfiles that the point at at the same time.
    set lockfiles {}
    set dirname [file dirname $LCK(lockfile)]
    set module [string trim [namespace current] :]
    set ptn .${module}$LCK(-tokeniser)*.[string trimleft $LCK(-ext) .]
    foreach fname [glob -directory $dirname -nocomplain -- $ptn] {
	if { [llength [split [file tail $fname] $LCK(-tokeniser)]] >= 6 } {
	    foreach {host pid lockfile} [Owner $fname $LCK(-tokeniser)] break
	    if { $host eq $myhost } {
		lappend candidates $fname $pid
	    }
	    lappend lockfiles $lockfile
	}
    }

    # Second pass on the lockfiles that were found above, these will
    # have the same content as the locker so we can also remove the
    # ones that are from us and have no running PID behind.
    foreach fname [lsort -unique $lockfiles] {
	foreach {host pid lockfile} [Owner $fname] break
	if { $host eq $myhost } {
	    lappend candidates $fname $pid
	}
    }

    # If we have candidates, check that their corresponding processes
    # still are running on the host.  If they are not, it is safe to
    # remove the file and release the lock.
    if { [llength $candidates] > 0 } {
	set processes [Processes]
	foreach {fullpath pid} $candidates {
	    # Process has died, remove the locks as much as we can...
	    if { $pid ni $processes } {
		Debug "Process $pid behind $fullpath had died, releasing"
		catch {file delete -force -- $fullpath}
	    }
	}
    }
}


package provide lockf $::lockf::version
