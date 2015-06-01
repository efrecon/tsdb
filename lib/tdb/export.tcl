##################
## Module Name     --  export.tcl
## Original Author --  Emmanuel Frecon - emmanuel@sics.se
## Description:
##
##    Module to export TSDB data to InfluxDB or KairosDB, uses the old
##    0.8 protocol for JSON export to influx and the text protocol for
##    kairos.
##
##################

namespace eval ::tdb::export {
    variable EXPORT
    if { ![::info exists EXPORT] } {
	array set EXPORT {
	    -separator  "."
	    -kairos     "put"
	}
	variable version 0.1
    }
    namespace export {[a-z]*}
    namespace path [namespace parent];    # Access the procedures from parent
    namespace ensemble create -command ::export
}


proc ::tdb::export::influx { db url {slots 0} } {
    return [Export $db influx $url $slots]
}

proc ::tdb::export::kairos { db url {slots 0} } {
    # This should be improved to support both the JSON and the TXT
    # formats.  We can easily segregates between those using the
    # scheme, i.e. tcp:// or http:// at the start of the URL.
    return [Export $db kairos $url $slots]
}


proc ::tdb::export::Export { db type url {slots 0} } {
    set converted ""
    set stepper [expr {$slots*60*1000}]
    foreach serie [$db series] {
	set nfo [$db info $serie]
	foreach sample [dict get $nfo samples] {
	    if { [dict get [$db info $serie $sample] count] > 0 } {
		if { $slots <= 0 } {
		    return [Dump $db $serie $sample \
				[dict get $nfo earliest] \
				[dict get $nfo latest] \
				$type $url]
		} else {
		    for {set tstart [dict get $nfo earliest]} \
			{$tstart<=[dict get $nfo latest]} \
			{incr tstart $stepper} {
			    set output [Dump $db $serie $sample \
					    $tstart \
					    [expr {$tstart+$stepper-1}] \
					    $type $url]
			    if { $output ne "" } {
				append converted "${output}\n"
			    }
			}
		}
	    }
	}
    }

    return [string trim $converted]
}


proc ::tdb::export::Dater { tstamp } {
    global T2I

    set dt [expr {$tstamp/1000}]
    set ms [expr {$tstamp-(1000*$dt)}]
    set str [clock format $dt -format $T2I(dtFormat)]
    append str ".[format %.03d $ms]"
    return $str
}


proc ::tdb::export::ASCII { vname {rpl ""}} {
    set vname [string map {Å A Ä A Ö O å a ä a ö o} $vname]
    set cln ""
    foreach c [split $vname ""] {
	if { [string match -nocase {[a-z0-9_]} $c] } {
	    append cln $c
	} else {
	    append cln $rpl
	}
    }
    return $cln
}


proc ::tdb::export::InfluxJSON {db serie sample start end {count_ ""}} {
    # Note that this creates a (possibly) gigantic JSON array with all
    # points for the sample, so this might be impractical from a
    # scaling point of view.
    if { $count_ ne "" } { upvar $count_ count }
    set count 0
    set json ""
    append json "\["
    append json "\{"
    append json "\"name\": \"[ASCII $serie]\","
    append json "\"columns\": \[\"time\",\"[ASCII $sample]\"\],"
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

    return $json
}


proc ::tdb::export::KairosTXT {db serie sample start end} {
    variable EXPORT

    set lines {}

    foreach {tstamp val} [$db samples $serie $sample $end $start] {
	set line "$EXPORT(-kairos) [ASCII $serie]$EXPORT(-separator)[ASCII $sample] $tstamp "
	if { [string is integer -strict $val] \
		 || [string is boolean -strict $val] \
		 || [string is double -strict $val] } {
	    append line "$val"
	    lappend lines $line
	}
    }

    return $lines
}


proc ::tdb::export::Dump {db serie sample start end type url} {
    global T2I

    log NOTICE "Pushing $sample in series $serie\
                ([Dater $start]->[Dater $end])"
    switch -nocase -glob -- $type {
	"influx*" {
	    set json [InfluxJSON $db $serie $sample $start $end count]
	    if { $count > 0 } {
		if { $url eq "" } {
		    # Dry run, just return the JSON
		    return $json
		} else {
		    set tok [::http::geturl $url \
				 -type "application/json" \
				 -query $json]
		    set code [::http::ncode $tok]
		    if { $code >= 200 && $code < 300 } {
			log INFO "Pushed $count value(s) for $sample"
		    } else {
			log WARN "Failed pushing data:$code, [http::error $tok]"
		    }
		    ::http::cleanup $tok
		}
	    }
	}
	"kairos*" {
	    # We should optimise this so as to keep the socket opened
	    # if possible.  A socket map perhaps?
	    set lines [KairosTXT $db $serie $sample $start $end]
	    if { [llength $lines] > 0 } {
		if { $url eq "" } {
		    return [join $lines "\n"]
		} else {
		    set s [string first "://" $url]
		    if { $s >= 0 } {
			set scheme [string range $url 0 [expr {$s-1}]]
			if { $scheme eq "tcp" } {
			    set slash [string first "/" $url [expr {$s+3}]]
			    if { $slash eq "" } {
				set srv [string range $url [expr {$s+3}] end]
			    } else {
				set srv [string range $url \
					     [expr {$s+3}] [expr {$slash-1}]]
			    }
			    foreach {hst prt} [split $srv :] break
			    log INFO "Pushing line-based data to ${hst}:${prt}"
			    set sock [socket $hst $prt]
			    fconfigure $sock -buffering line
			    foreach l $lines {
				puts $sock $l
			    }
			    close $sock
			}
		    }
		}
	    }
	}
    }

    return ""
}



package provide tdb::export $::tdb::export::version
