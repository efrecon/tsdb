# tsdb

TSDB is a simple approach to implementing a time-series database.  It
is partly inspired by [InfluxDB][1], but writes its content in plain
text files.  TSDB provides support for the [Graphite API][2] and the
[InfluxDB API][3] to write data into the database.

This is a work in progress.  For example, there are currently no
client API implementation for retrieving data (even though there is an
implementation in the database code).

  [1]: http://influxdb.com/
  [2]: https://graphite.readthedocs.org/en/latest/feeding-carbon.html#the-plaintext-protocol
  [3]: http://influxdb.com/docs/v0.8/api/reading_and_writing_data.html

# Design

TSDB divides its space into series (as in time series (of metrics)).
Each series will accept any number of samples as time passes.  Time is
preferably implicit, i.e. at the time of writing, but can also be
specified.  On disk, each series will be associated to a directory,
itself having as many directories as the series has ever had samples.
In each sample directory, there will be a number of timeblocks, where
the name can be directly mapped to the timestamp of the first sample
in that block.  TSDB tries to limit the size of these blocks to ease
copying.

As time goes by and values for samples are written, these values will
be appended to a timeblocks and new timeblocks will be created
whenever necessary.  If values are inserted in the middle of an
existing timeblock, the value will first be appended to the end of the
file representing the timeblock and the block will be marked as
"dirty" (TSDB uses the filename for flag representation).  Later on,
dirty blocks will be reordered on disk.

# Dependencies

In order to support the InfluxDB API, the example client uses the mini
HTTP server from [til][4].  It assumes that a copy of the til is
available in the lib directory (see the Dockerfile for an example).

    [4]: http://code.google.com/p/efr-tools/source/browse/#svn%2Ftrunk%2Ftil

# Internal API

To create a new database called `mydb` in the root directory
`/opt/data` (where you might want to place other databases), you would
call:

    set db [::tdb::create -name mydb -root /opt/data]

This will return a command that will be used for all further access to
the database.

To start storing data for the series called `example` and create two
samples and their values, you would call:

    $db store example mykey 10 anotherkey "a string of your choice"

The special keys `time` or `timestamp` are recognised as the time at
which the keys should be set, these are expressed in the number of
milliseconds since the epoch.

To retrieve all the values for a sample that have been put in the
database for the past 10 seconds, you would call:

    $db samples example mykey -10000

To retrieve all the values for a sample between two specific dates,
you would call (note that the starting point is specified *after* the
end of the series, because of the relative case above.  This might
change in the future).

    $db samples example mykey 1424732064000 1424732054000

# Client API

## Graphite

The example client is able to listen for incoming writes expressed
using the textual carbon API.  Graphite uses dots `.` to create a
hierarchy of metrics.  The server will understand the top of the
hierarchy as the name of the series and the rest as the name of a
sample.

## InfluxDB

The example client is able to receive HTTP post on the standard
InfluxDB port.  There is no support for authentication at this point
(even though this is supported by the internal HTTP server).

# Docker Component

The example server can be packaged as a docker component. To create it
run the following:

    docker build -t efrecon/tsdb .

To run it with maximal verbosity (not recommended), execute the
following command.  This will create a server that listen for incoming
client connections on the graphite default port (`2003') and on the
InfluxDB default port (`8086`).  Both ports are exposed, as
examplified below.  The components is configured to use a database
called `db`, located in the exported volume `/data`.

    docker run -d --name="tsdb" -p 2003:2003 -p 8086:8086 efrecon/tsdb -v 6

