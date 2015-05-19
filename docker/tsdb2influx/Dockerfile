FROM efrecon/tcl
MAINTAINER Emmanuel Frecon <emmanuel@sics.se>

# Install git so we can install dependencies
RUN apk add --update-cache git

# Install tsdb into /opt
WORKDIR /opt
RUN git clone https://github.com/efrecon/tsdb
RUN rm -rf /var/cache/apk/*
WORKDIR /opt/tsdb

# Export where databases will be placed by default
VOLUME /data

# Execute tsdb2influx on start, Use same directory conventions as the
# main tsdb process
ENTRYPOINT ["tclsh8.6", "/opt/tsdb/tsdb2influx.tcl", "-root", "/data", "-dbs", "data"]
CMD ["-v", "5"]
