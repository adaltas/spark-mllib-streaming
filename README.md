This repository backs up the article: [Spark Streaming part 4: clustering with Spark MLlib](http://www.adaltas.com/en/2019/07/11/ml-clustering-spark-mlib/). This project extends the Scala Spark application from [spark-streaming-scala](https://github.com/adaltas/spark-streaming-scala) repository with a Machine Learning k-means clustering algorithm. This repository accompanies mainly the part 4, but it builds on all parts from the series:

- [Spark Streaming part 1: build data pipelines with Spark Structured Streaming](http://www.adaltas.com/en/2019/04/18/spark-streaming-data-pipelines-with-structured-streaming/)
- [Spark Streaming part 2: run Spark Structured Streaming pipelines in Hadoop](http://www.adaltas.com/en/2019/05/28/spark-structured-streaming-in-hadoop/)
- [Spark Streaming part 3: tools and tests for Spark applications](http://www.adaltas.com/en/2019/06/19/spark-devops-tools-test/)
- [Spark Streaming part 4: clustering with Spark MLlib](http://www.adaltas.com/en/2019/07/11/ml-clustering-spark-mlib/)

Primary files related to the part 4 are:

- *MainKmeans.scala* from the `com.adaltas.taxistreaming.clustering` package - clusters are computed and saved as models

- *MainConsoleClustering.scala* from the `com.adaltas.taxistreaming` package - ML models are read and integrated within the streaming pipeline

- *TaxiProcessingBatch.scala* from the `com.adaltas.taxistreaming.processing` package

- *ClustersGeoJSON.scala* from the `com.adaltas.taxistreaming.utils` package - used for visualization

For more information about the content of these files, follow [the associated article](http://www.adaltas.com/en/2019/07/11/ml-clustering-spark-mlib/).

The instructions below regard the setup of a Hadoop cluster, as previously explained in the [spark-streaming-scala](https://github.com/adaltas/spark-streaming-scala) repository. The fourth part could be followed in local mode, in which case the *DataLoader.scala* file from `com.adaltas.taxistreaming` package can be used to get the data.

## Setup a VM cluster and prepare the environment for the HDP installation

After cloning this repo, `cd` into it and run:

```bash
ssh-keygen -t rsa -N "" -f ~/.ssh/id_rsa_ex
sudo tee -a /etc/hosts > /dev/null <<- EOF
  10.10.10.11 master01.cluster master01
  10.10.10.12 master02.cluster master02
  10.10.10.16 worker01.cluster worker01
  10.10.10.17 worker02.cluster worker02
EOF
vagrant up
```

- Creates SSH keys on the host machine (`~/.ssh/id_rsa_ex`)
- Appends FQDNs of cluster nodes in `/etc/hosts` on the host machine (sudo needed)
- Sets up a cluster of 4 VMs running on a laptop with 32GB and 8 cores
  - The VMs FQDNs: `master01.cluster`, `master02.cluster`, `worker01.cluster`, `worker02.cluster`
  - The VMs have respectively 5/6/7/7 GB RAM, 1/1/3/2 cores, and all are running CentOS
  - Sets up SSH keys for root of each node (each VM)
  - Configures the `/etc/hosts` file on each node
  - [Prepares the environment for HDP](https://docs.hortonworks.com/HDPDocuments/Ambari-2.7.3.0/bk_ambari-installation/content/prepare_the_environment.html) on each node
  - Installs Python 3 concurrently to Python 2 on each node
- Only on the node `master01.cluster`:
  - Installs mysql and sets up a mysql database called `hive`, creates a `hive` user with `NewHivePass4!` password
  - Installs Ambari server and mysql jdbc connector
  - Starts `ambari-server`
- Notice that all nodes are provisioned via shell, which is a fragile procedural approach. Warnings may occur.

## Install HDP distribution with Ambari

Now the cluster should be ready for the Hadoop installation. You should be able to access Ambari's Web UI on http://master01.cluster:8080. Deploy a minimal Hadoop cluster with [HDP 3.1.0 installed through Apache Ambari 2.7.3](https://docs.hortonworks.com/HDPDocuments/Ambari-2.7.3.0/bk_ambari-installation/content/install-ambari-server.html).

Guidelines for the Ambari Installer wizard:

- In "Target Hosts", enter the list of hosts with `master[01-02].cluster` and `worker[01-02].cluster` lines
- During the "Host Registration Information" provide the `~/.ssh/id_rsa_ex` SSH private key created earlier
- Install a minimal set of services:
  - ZooKeeper, YARN, HDFS, Hive, Tez on host `master01.cluster`
  - Kafka and Spark2 (including Spark2 History Server and Spark Thrift Server) on host `master02.cluster`
- Choose to connect to the existing mysql database with a URL `jdbc:mysql://master01.cluster/hive` (usr/pass `hive`/`NewHivePass4!`)
- Configure YARN containers: the memory allocated for containers at 6144MB, with a container memory threshold from the minimum value of 512MB to the maximum value of 6144MB
- Configure Spark2 to use Python 3 for PySpark - in "Spark2/Advanced spark2-env/content" field append `PYSPARK_PYTHON=python3`
- Spark's Driver program output tends to be rich in `INFO` level logs, which obfuscates the processing results outputted to console. In "Spark2/Advanced spark2-log4j-properties" `INFO` can be changed to `WARN` in "log4j.rootCategory" to make Spark output less verbose. When things don't work it's better to reverse to `INFO` log level
- Check that all services are running on hosts they were supposed to. Especially verify that Kafka Broker listens on `master02.cluster:6667` and Spark2 is available on `master02.cluster`
