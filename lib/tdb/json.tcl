# JSON parser / encoder.
# Copyright (C) 2014 Danyil Bohdan.
# License: MIT

# Parse the string $str containing JSON into nested Tcl dictionaries.
# numberDictArrays: decode arrays as dictionaries with sequential integers
# starting with zero as keys; otherwise decode them as lists.
namespace eval ::tdb::json {}

proc ::tdb::json::parse {str {numberDictArrays 0}} {
    set result [::tdb::json::decode-value $str $numberDictArrays]
    if {[lindex $result 1] eq ""} {
        return [lindex $result 0]
    } else {
        error "trailing garbage after JSON data: $str"
    }
}

# Serialize nested Tcl dictionaries as JSON.
# numberDictArrays: encode dictionaries with keys {0 1 2 3 ...} as arrays,
# e.g., {0 a 1 b} to ["a", "b"].
proc ::tdb::json::stringify {dictionaryOrValue {numberDictArrays 1} {allstring 0}} {
    set result {}
    if {[llength $dictionaryOrValue] <= 1} {
        if {[string is integer $dictionaryOrValue] || \
                [string is double $dictionaryOrValue]} {
	    # Number.
	    if { $allstring } {
		set result "\"$dictionaryOrValue\""
	    } else {
		set result $dictionaryOrValue
	    }
        } else {
            # String.
            set result "\"$dictionaryOrValue\""
        }
    } else {
        # Dict.
        set allNumeric 1

        if {$numberDictArrays} {
            set values {}
            set i 0
            foreach {key value} $dictionaryOrValue {
                set allNumeric [expr {$allNumeric && ($key == $i)}]
                if {!$allNumeric} {
                    break
                }
                lappend values $value
                incr i
            }
        }

        if {$numberDictArrays && $allNumeric} {
            # Produce array.
            set arrayElements {}
            foreach x $values {
                lappend arrayElements [::tdb::json::stringify $x 1 $allstring]
            }
            set result "\[[join $arrayElements {, }]\]"
        } else {
            # Produce object.
            set objectDict {}
            foreach {key value} $dictionaryOrValue {
		foreach {k type} [split $key "!"] break
		if { $type eq "string" } {
		    lappend objectDict "\"$k\": [::tdb::json::stringify $value 0 1]"
		} else {
		    lappend objectDict "\"$k\": [::tdb::json::stringify $value 0 $allstring]"
		}
            }
            set result "{[join $objectDict {, }]}"
        }
    }
    return $result
}

# Choose how to decode a JSON value. Return a list consisting of the result of
# parsing the initial part of $str and the remainder of $str that was not
# parsed. E.g., ::tdb::json::decode-value {"string", 55} returns {{string} {, 55}}.
proc ::tdb::json::decode-value {str {numberDictArrays 0}} {
    set str [string trimleft $str]
    switch -regexp -- $str {
        {^\".*} {
            return [::tdb::json::decode-string $str]
        }
        {^[0-9-].*} {
            return [::tdb::json::decode-number $str]
        }
        {^\{.*} {
            return [::tdb::json::decode-object $str $numberDictArrays]
        }
        {^\[.*} {
            return [::tdb::json::decode-array $str $numberDictArrays]
        }
        {^(true|false|null)} {
	    return [::tdb::json::decode-boolean-or-null $str]
        }
        default {
            error "cannot decode value as JSON: \"$str\""
        }
    }
}

# Return a list of two elements: the initial part of $str parsed as "true",
# "false" or "null" and the remainder of $str that wasn't parsed.
proc ::tdb::json::decode-boolean-or-null {str} {
    regexp {^(true|false|null)} $str value
    return [list $value [string range $str [string length $value] end]]
}


# Return a list of two elements: the initial part of $str parsed as a JSON
# string and the remainder of $str that wasn't parsed.
proc ::tdb::json::decode-string {str} {
    if {[regexp {^"((?:[^"\\]|\\.)*)"} $str _ result]} {
        return [list \
                [subst -nocommands -novariables $result] \
                [string range $str [expr {2 + [string length $result]}] end]]
                # Add two to result length to account for the double quotes
                # around the string.
    } else {
        error "can't parse JSON string: $str"
    }
}

# Return a list of two elements: the initial part of $str parsed as a JSON
# number and the remainder of $str that wasn't parsed.
proc ::tdb::json::decode-number {str} {
    if {[regexp -- {^-?(?:0|[1-9][0-9]*)(?:\.[0-9]*)?(:?(?:e|E)[+-]?[0-9]*)?} \
            $str result]} {
        #            [][ integer part  ][ optional  ][  optional exponent  ]
        #            ^ sign             [ frac. part]
        return [list $result [string range $str [string length $result] end]]
    } else {
        error "can't parse JSON number: $str"
    }
}

# Return a list of two elements: the initial part of $str parsed as a JSON array
# and the remainder of $str that wasn't parsed. Arrays are parsed into
# dictionaries with numbers {0 1 2 ...} as keys if $numberDictArrays is true
# or lists if it is false. E.g., if $numberDictArrays == 1 then
# ["Hello, World" 2048] is converted to {0 {Hello, World!} 1 2048}; otherwise
# it is converted to {{Hello, World!} 2048}.
proc ::tdb::json::decode-array {str {numberDictArrays 0}} {
    set strInitial $str
    set result {}
    set value {}
    set i 0
    if {[string index $str 0] ne "\["} {
        error "can't parse JSON array: $strInitial"
    } else {
        set str [string range $str 1 end]
    }
    while 1 {
	# Empty array, get out from here.
	if { [string index [string trimleft $str] 0] eq "\]" } {
	    set str [string range [string trimleft $str] 1 end]
	    break
	}
        # Value.
        lassign [::tdb::json::decode-value $str $numberDictArrays] value str
        set str [string trimleft $str]
        if {$numberDictArrays} {
            lappend result $i
        }
        lappend result $value

        # ","
        set sep [string index $str 0]
        set str [string range $str 1 end]
        if {$sep eq "\]"} {
            break
        } elseif {$sep ne ","} {
            error "can't parse JSON array: $strInitial"
        }
        incr i
    }
    return [list $result $str]
}

# Return a list of two elements: the initial part of $str parsed as a JSON
# object and the remainder of $str that wasn't parsed.
proc ::tdb::json::decode-object {str {numberDictArrays 0}} {
    set strInitial $str
    set result {}
    set value {}
    if {[string index $str 0] ne "\{"} {
        error "can't parse JSON object: $strInitial"
    } else {
        set str [string range $str 1 end]
    }
    while 1 {
        # Key string.
        set str [string trimleft $str]
        lassign [::tdb::json::decode-string $str] value str
        set str [string trimleft $str]
        lappend result $value

        # ":"
        set sep [string index $str 0]
        set str [string range $str 1 end]
        if {$sep ne ":"} {
            error "can't parse JSON object: $strInitial"
        }

        # Value.
        lassign [::tdb::json::decode-value $str $numberDictArrays] value str
        set str [string trimleft $str]
        lappend result $value

        # ","
        set sep [string index $str 0]
        set str [string range $str 1 end]
        if {$sep eq "\}"} {
            break
        } elseif {$sep ne ","} {
            error "can't parse JSON object: $str"
        }
    }
    return [list $result $str]
}
package provide tdb::json 1.0
