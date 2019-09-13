#Basic image
FROM ubuntu:18.04

#Maintainer
MAINTAINER lar146

USER root

#install dev tools
RUN apt-get update
RUN apt-get install -y curl tar sudo openssh-server openssh-client rsync

# Environment
WORKDIR /usr/local
ENV HADOOP_HOME /usr/local/hadoop

#passwordless ssh
RUN rm -f /etc/ssh/ssh_host_dsa_key /etc/ssh/ssh_host_rsa_key /root/.ssh/id_rsa
RUN ssh-keygen -q -N "" -t dsa -f /etc/ssh/ssh_host_dsa_key
RUN ssh-keygen -q -N "" -t rsa -f /etc/ssh/ssh_host_rsa_key
RUN ssh-keygen -q -N "" -t rsa -f /root/.ssh/id_rsa
RUN cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys

#java installation
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    apt-get install -y ant && \
    apt-get clean;

#setup JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
RUN export JAVA_HOME

#hadoop
RUN cd /usr/local/
RUN wget  http://us.mirrors.quenda.co/apache/hadoop/common/hadoop-3.2.0/hadoop-3.2.0.tar.gz
RUN tar -zxf hadoop-3.2.0.tar.gz
RUN ln -s hadoop-3.2.0 hadoop
RUN cd ${HADOOP_HOME}
RUN mkdir testfile

COPY config/hadoop-env.sh ${HADOOP_HOME}/etc/hadoop/
COPY config/core-site.xml ${HADOOP_HOME}/etc/hadoop/
COPY config/hdfs-site.xml ${HADOOP_HOME}/etc/hadoop/
COPY config/mapred-site.xml ${HADOOP_HOME}/etc/hadoop/
COPY config/yarn-site.xml ${HADOOP_HOME}/etc/hadoop/
#
COPY demo/testfile1.txt ${HADOOP_HOME}/testfile/
COPY demo/testfile2.txt ${HADOOP_HOME}/testfile/
#
#
COPY demo/demo.sh ${HADOOP_HOME}
RUN chmod a+x ${HADOOP_HOME}/demo.sh
#
#
RUN ${HADOOP_HOME}/bin/hdfs namenode -format
#
COPY config/bootstrap.sh /root/
RUN chmod a+x /root/bootstrap.sh
#
## Zookeeper
EXPOSE 2181

# NameNode metadata service ( fs.defaultFS )
EXPOSE 9000

# FTP Filesystem impl. (fs.ftp.host.port)
EXPOSE 21

### Hdfs ports (Reference: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml)

# NameNode Web UI: Web UI to look at current status of HDFS, explore file system (dfs.namenode.http-address / dfs.namenode.https-address)
EXPOSE 9870 9871

# DataNode : DataNode WebUI to access the status, logs etc. (dfs.datanode.http.address / dfs.datanode.https.address)
EXPOSE 9864 9865

# DataNode  (dfs.datanode.address / dfs.datanode.ipc.address)
EXPOSE 9866 9867

# Secondary NameNode (dfs.namenode.secondary.http-address / dfs.namenode.secondary.https-address)
EXPOSE 9869 9868

# Backup node (dfs.namenode.backup.address / dfs.namenode.backup.http-address)
EXPOSE 50100 50105

# Journal node (dfs.journalnode.rpc-address / dfs.journalnode.http-address / dfs.journalnode.https-address )
EXPOSE 8485 8480 8481

### Mapred ports (Reference: https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml)

# Task Tracker Web UI and Shuffle (mapreduce.tasktracker.http.address)
EXPOSE 50060

# Job tracker Web UI (mapreduce.jobtracker.http.address)
EXPOSE 50030

# Job History Web UI (mapreduce.jobhistory.webapp.address)
EXPOSE 19888

# Job History Admin Interface (mapreduce.jobhistory.admin.address)
EXPOSE 10033

# Job History IPC (mapreduce.jobhistory.address)
EXPOSE 10020

### Yarn ports (Reference: https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-common/yarn-default.xml)

# Applications manager interface (yarn.resourcemanager.address)
EXPOSE 8032

# Scheduler interface (yarn.resourcemanager.scheduler.address)
EXPOSE 8030

# Resource Manager Web UI (yarn.resourcemanager.webapp.address / yarn.resourcemanager.webapp.https.address)
EXPOSE 8088 8090

# ??? (yarn.resourcemanager.resource-tracker.address)
EXPOSE 8031

# Resource Manager Administration Web UI
EXPOSE 8033

# Address where the localizer IPC is (yarn.nodemanager.localizer.address)
EXPOSE 8040

# Node Manager Web UI (yarn.nodemanager.webapp.address)
EXPOSE 8042

# Timeline servise RPC (yarn.timeline-service.address)
EXPOSE 10200

# Timeline servise Web UI (yarn.timeline-service.webapp.address / yarn.timeline-service.webapp.https.address)
EXPOSE 8188 8190

# Shared Cache Manager Admin Web UI (yarn.sharedcache.admin.address)
EXPOSE 8047

# Shared Cache Web UI (yarn.sharedcache.webapp.address)
EXPOSE 8788

# Shared Cache node manager interface (yarn.sharedcache.uploader.server.address)
EXPOSE 8046

# Shared Cache client interface (yarn.sharedcache.client-server.address)
EXPOSE 8045

### Other ports

# SSH
EXPOSE 22

#RUN sudo apt-get install net-tools
#RUN sudo netstat -laputen | grep ':3306 *LISTEN'

#RUN docker stop $(docker ps -a -q); docker rm $(docker ps -a -q); docker volume rm $(docker volume ls -qf dangling=true)
#
#RUN docker network rm $(docker network ls -q)
#
#RUN sudo lsof -nP | grep LISTEN
#
#RUN sudo kill -9 1548

CMD [ "/root/bootstrap.sh" ]


