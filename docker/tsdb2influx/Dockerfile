FROM efrecon/tcl
MAINTAINER Emmanuel Frecon <emmanuel@sics.se>

# Set the env variable DEBIAN_FRONTEND to noninteractive to get
# apt-get working without error output.
ENV DEBIAN_FRONTEND noninteractive

# Update underlying ubuntu image and all necessary packages.
RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y subversion

#COPY tsdb.tcl /opt/tsdb/
#COPY lib/tdb/*.tcl /opt/tsdb/lib/tdb/

RUN svn checkout https://github.com/efrecon/tsdb/trunk /opt/tsdb

# Export where databases will be placed by default
VOLUME /data

# Execute tsdb2influx on start, Use same directory conventions as the
# main tsdb process
ENTRYPOINT ["tclsh8.6", "/opt/tsdb/tsdb2influx.tcl", "-v", "5", "-root", "/data", "-dbs", "data"]