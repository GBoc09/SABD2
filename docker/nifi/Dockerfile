FROM apache/nifi:2.4.0

RUN mkdir -p $NIFI_HOME/conf/hadoop \
    && mkdir -p $NIFI_HOME/conf/nifi-ingestion \
    && mkdir -p $NIFI_HOME/nifi-output

COPY --chown=nifi:nifi hadoop/hadoop-conf/core-site.xml  $NIFI_HOME/conf/hadoop/core-site.xml
COPY --chown=nifi:nifi hadoop/hadoop-conf/hdfs-site.xml  $NIFI_HOME/conf/hadoop/hdfs-site.xml

COPY --chown=nifi:nifi nifi/lib/nifi-hadoop-libraries-nar-2.4.0.nar $NIFI_HOME/lib/
COPY --chown=nifi:nifi nifi/lib/nifi-hadoop-nar-2.4.0.nar $NIFI_HOME/lib/
COPY --chown=nifi:nifi nifi/lib/nifi-parquet-nar-2.4.0.nar $NIFI_HOME/lib/

COPY --chown=nifi:nifi nifi/conf/flow.json.gz $NIFI_HOME/conf/flow.json.gz

EXPOSE 8443
