FROM efrecon/tcl
MAINTAINER Emmanuel Frecon <emmanuel@sics.se>

# Set the env variable DEBIAN_FRONTEND to noninteractive to get
# apt-get working without error output.
ENV DEBIAN_FRONTEND noninteractive

# Update underlying ubuntu image and all necessary packages.
RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y subversion

COPY tsdb.tcl /opt/tsdb/
COPY lib/tdb/*.tcl /opt/tsdb/lib/tdb/

RUN svn checkout http://efr-tools.googlecode.com/svn/trunk/til /opt/tsdb/lib/til

# Export Graphite and Influx ports
EXPOSE 2003
EXPOSE 8086

# Export where databases will be placed by default
VOLUME /data

# Execute tsdb on start, listening for both client APIs connections on
# default database called db.
ENTRYPOINT ["tclsh8.6", "/opt/tsdb/tsdb.tcl", "-v", "3", "-root", "/data", "-ports", "graphite:db:2003 influx:db:8086"]