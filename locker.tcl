#! /usr/bin/env tclsh
package require Tcl 8.6;   # chan pipe!

set dirname [file dirname [file normalize [info script]]]
lappend auto_path [file join $dirname lib]
package require lockf

if { [llength $argv] < 2 } {
    puts stderr "You need at least the path to a lockfile and a command to lock"
    exit 1
}

set ddash [lsearch $argv --]
if { $ddash > 0 } {
    set cmd [lrange $argv [expr {$ddash+1}] end]
    set lcmd [lrange $argv 0 [expr {$ddash-1}]]
} else {
    set cmd {}
    set lcmd {}
    for {set i 1} {$i<[llength $argv]} {incr i 2} {
	if { [string index [lindex $argv $i] 0] != "-" } {
	    set cmd [lrange $argv $i end]
	    set lcmd [lrange $argv 0 [expr {$i-1}]]
	}
    }
}
if { [llength $cmd] == 0 } {
    puts stderr "Couldn't find any command to execute!"
    exit 1
}

proc popen4 { args } {
    foreach chan {In Out Err} {
        lassign [chan pipe] read$chan write$chan
    } 

    set pid [exec {*}$args <@ $readIn >@ $writeOut 2>@ $writeErr &]
    chan close $writeOut
    chan close $writeErr

    foreach chan [list stdout stderr $readOut $readErr $writeIn] {
        chan configure $chan -buffering line -blocking false
    }

    return [list $pid $writeIn $readOut $readErr]
}

set CLOSED {}
set DONE 0
proc forward { s where } {
    puts -nonewline $where [chan read $s]
    if { [chan eof $s] } {
	lappend ::CLOSED $where
	if { [llength $::CLOSED] >= 2 } {
	    set ::DONE 1
	}
    }
}


if { [catch {eval [linsert $lcmd 0 lockf lock]} ms] } {
    puts stderr "Could not lock using file at [lindex $lcmd 0]: $ms!"
    exit 1
}
puts stderr ">> Acquired lock in $ms ms"
if { $ms == 0 } {
    puts stderr "Could not acquire lock via [lindex $lcmd 0]!"
    exit 1
}
puts stderr ">> Now running: $cmd"
lassign [popen4 {*}$cmd] pid stdin stdout stderr
chan event $stdout readable [list forward $stdout stdout]
chan event $stderr readable [list forward $stderr stderr]
vwait ::DONE
puts stderr ">> Releasing lock!"
lockf unlock [lindex $lcmd 0]
