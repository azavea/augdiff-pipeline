#!/bin/sh

set -x

cat <<EOF > /tmp/hive-site.xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

<configuration>

  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:postgresql://$POSTGRES_HOST/$POSTGRES_DB</value>
    <description>JDBC connect string for a JDBC metastore</description>
  </property>

  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.postgresql.Driver</value>
    <description>Driver class name for a JDBC metastore</description>
  </property>

  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>$POSTGRES_USER</value>
    <description>username to use against metastore database</description>
  </property>

  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>$POSTGRES_PASSWORD</value>
    <description>password to use against metastore database</description>
  </property>

  <!-- hive.metastore.warehouse.dir deprecated: https://spark.apache.org/docs/latest/sql-programming-guide.html#sql -->
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/tmp</value>
    <description>location of default database for the warehouse</description>
  </property>

</configuration>
EOF
cp /tmp/hive-site.xml $HIVE_CONF_DIR/hive-site.xml
cp /tmp/hive-site.xml $SPARK_CONF_DIR/hive-site.xml

echo "Y" | hdfs namenode -format
nohup hdfs namenode &> /dev/null &
nohup hdfs secondarynamenode &> /dev/null &
nohup hdfs datanode &> /dev/null &
