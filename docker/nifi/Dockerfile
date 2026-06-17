FROM apache/nifi:latest

# Create hadoop and ingestion conf directories
RUN mkdir $NIFI_HOME/conf/hadoop
RUN mkdir $NIFI_HOME/conf/nifi-ingestion
RUN mkdir $NIFI_HOME/nifi-output

# Copy hadoop configuration files
COPY --chown=nifi:nifi conf/hadoop/core-site.xml  $NIFI_HOME/conf/hadoop//hdfs-site.xml
COPY --chown=nifi:nifi conf/hadoop/hdfs-site.xml  $NIFI_HOME/conf/hadoop/core-site.xml

# Copy the extensions for nifi
COPY --chown=nifi:nifi lib/nifi-hadoop-libraries-nar-2.4.0.nar $NIFI_HOME/lib/nifi-hadoop-libraries-nar-2.4.0.nar
COPY --chown=nifi:nifi lib/nifi-hadoop-nar-2.4.0.nar $NIFI_HOME/lib/nifi-hadoop-nar-2.4.0.nar
COPY --chown=nifi:nifi lib/nifi-parquet-nar-2.4.0.nar $NIFI_HOME/lib/nifi-parquet-nar-2.4.0.nar

# Copy the flow configuration files for nifi
COPY --chown=nifi:nifi conf/flow.json.gz $NIFI_HOME/conf/flow.json.gz

EXPOSE 8443