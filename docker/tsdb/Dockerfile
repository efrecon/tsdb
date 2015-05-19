FROM efrecon/mini-tcl
MAINTAINER Emmanuel Frecon <emmanuel@sics.se>

# Install git so we can install dependencies
RUN apk add --update-cache git

# Install til and tsdb into /tmp and move them to proper locations
WORKDIR /opt
RUN git clone https://github.com/efrecon/tsdb
WORKDIR /opt/tsdb/lib
RUN git clone https://github.com/efrecon/til
RUN rm -rf /var/cache/apk/*
WORKDIR /opt/tsdb

# Export Graphite and Influx ports
EXPOSE 2003
EXPOSE 8086

# Export where databases will be placed by default
VOLUME /data

# Execute tsdb on start, listening for both client APIs connections on
# default database called data (meaning that it will create a
# subdirectory itself called data in the rootdirectory /data,
# i.e. /data/data).
ENTRYPOINT ["tclsh8.6", "/opt/tsdb/tsdb.tcl", "-root", "/data", "-dbs", "data", "-ports", "graphite:data:2003 influx:data:8086"]
CMD ["-v", "3"]
