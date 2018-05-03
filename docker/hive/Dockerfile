FROM openjdk:8 as hadoop-build
WORKDIR /tmp
RUN wget http://apache.claz.org/hadoop/common/hadoop-2.8.3/hadoop-2.8.3.tar.gz
RUN tar xf hadoop-2.8.3.tar.gz

FROM openjdk:8 as hive-build
WORKDIR /tmp
RUN curl -L http://apache.claz.org/hive/stable-2/apache-hive-2.3.3-bin.tar.gz --output apache-hive-2.3.3-bin.tar.gz
RUN tar xf apache-hive-2.3.3-bin.tar.gz

FROM openjdk:8 as spark-build
WORKDIR /tmp
RUN curl -L http://apache.claz.org/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz --out spark-2.3.0-bin-hadoop2.7.tgz
RUN tar xf spark-2.3.0-bin-hadoop2.7.tgz

FROM openjdk:8

# JDBC
RUN apt-get update
RUN apt-get install -y libpostgresql-jdbc-java
RUN apt-get clean

# HADOOP
ENV HADOOP_HOME="/opt/hadoop-2.8.3"
ENV HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
ENV HADOOP_MAPRED_HOME="${HADOOP_HOME}"
ENV HADOOP_COMMON="${HADOOP_HOME}"
ENV HADOOP_HDFS="${HADOOP_HOME}"
ENV HADOOP_CLASSPATH="${HADOOP_HOME}/*"
ENV HADOOP_CLASSPATH="${HADOOP_HOME}/lib/*:${HADOOP_CLASSPATH}"
ENV HADOOP_SSH_OPTS="-p "
ENV HADOOP_OPTS="-Djava.net.preferIPv4Stack=true"
ENV PATH="${HADOOP_HOME}/bin:${PATH}"
COPY --from=hadoop-build /tmp/hadoop-2.8.3 ${HADOOP_HOME}
RUN mkdir -p /hdfs/data
RUN mkdir -p /hdfs/name

# HIVE
ENV HIVE_HOME="/opt/apache-hive-2.3.3-bin"
ENV HIVE_CONF_DIR="${HIVE_HOME}/conf"
ENV PATH="${HIVE_HOME}/bin:${PATH}"
COPY --from=hive-build /tmp/apache-hive-2.3.3-bin ${HIVE_HOME}
RUN ln /usr/share/java/postgresql-jdbc4.jar ${HIVE_HOME}/lib/postgresql-jdbc4.jar

# SPARK
ENV SPARK_HOME="/opt/spark-2.3.0-bin-hadoop2.7"
ENV SPARK_CONF_DIR="/opt/spark-2.3.0-bin-hadoop2.7/conf"
ENV PATH="${SPARK_HOME}/bin:${PATH}"
COPY --from=spark-build /tmp/spark-2.3.0-bin-hadoop2.7 ${SPARK_HOME}
RUN ln /usr/share/java/postgresql-jdbc4.jar ${SPARK_HOME}/jars/postgresql-jdbc4.jar

COPY hadoop/core-site.xml ${HADOOP_CONF_DIR}/core-site.xml
COPY hadoop/hdfs-site.xml ${HADOOP_CONF_DIR}/hdfs-site.xml
COPY hadoop/start-services.sh /opt/start-services.sh
COPY hive/hive-site.xml ${HIVE_CONF_DIR}/hive-site.xml
COPY hive/hive-site.xml ${SPARK_CONF_DIR}/hive-site.xml
