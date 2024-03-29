Hadoop over GRNET ~okeanos
==========================

Python and Ansible scripts to create multi-node Hadoop clusters through [~okeanos](http://okeanos.grnet.gr/) kamaki API
(Hadoop installation based on [Michael Noll's instructions](http://www.michael-noll.com/tutorials/running-hadoop-on-ubuntu-linux-multi-node-cluster/)

### Requirements
	- OS for Ansible server: Debian 6
	- OS for Hadoop nodes: Debian 6 or 7, Ubuntu 13.04 (OpenSUSE, CentOS, RedHat: problems with 'root' account)
	- [Ansible](http://www.ansibleworks.com/): 1.7
	- [kamaki](https://www.synnefo.org/docs/kamaki/latest/): 0.12.10
	- Java: 1.7 (1.6 requires 2 edits in hadoop.yml: action: apt name=openjdk-6-jdk AND correct value for JAVA_HOME)

### Installation:
Install Ansible (ansible server) on the machine you want to run the ansible commands (NOT the Hadoop master)

### Execution
command> python hadoop_cluster.py [options]
see: python hadoop_cluster.py -h

to add new worker node(s): stop the cluster and run
command> python hadoop_cluster.py [options] --extend

NOTE: You have to manually format the namenode (see format-hdfs.yml) and start the Hadoop cluster
