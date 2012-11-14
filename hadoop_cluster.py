#!/usr/bin/env python

"""
Create a multi-node hadoop cluster on top of ~okeanos
Based on okeanos_cluster.py (vkoukis?)
Based on HBaseCluster (cslab)

Author: Byron Georgantopoulos (byron@admin.grnet.gr)
"""

import logging
import os
import paramiko
import sys
import time
import subprocess
from base64 import b64encode
from datetime import datetime

from kamaki.config import Config
from kamaki.clients.compute import ComputeClient
from kamaki.clients.cyclades import CycladesClient

config = Config()
DEFAULT_PREFIX = "hadoop"
DEFAULT_CYCLADES = config.get("compute", "url")
DEFAULT_TOKEN = (config.get("compute", "token") or
                 config.get("global", "token"))
# These should be converted to command-line options
# ACHTUNG: The script WILL upload DEFAULT_SSH_KEY into newly created instances
DEFAULT_SSH_KEY = os.path.expanduser(os.path.join("~", ".ssh", "id_rsa_hadoop"))
DEFAULT_SSH_PUB = os.path.expanduser(os.path.join("~", ".ssh", "id_rsa_hadoop.pub"))
DEFAULT_HADOOP_DIR = os.path.expanduser(os.path.join("~", "hadoop"))

CYCLADES = None

# Setup logging
logging.basicConfig(format="%(message)s")
log = logging.getLogger("okeanos-cluster")
log.setLevel(logging.INFO)

hostnames = [] # store hostnames of nodes

'''
class HadoopCluster(object):

    def addSlave():

    def Create():
'''

def parse_arguments(args):
    from optparse import OptionParser

    kw = {}
    kw["usage"] = "%prog [options]"
    kw["description"] = \
        "%prog creates and configures a hadoop multi-node cluster on top of ~okeanos."

    parser = OptionParser(**kw)
    parser.disable_interspersed_args()
    parser.add_option("--prefix",
                      action="store", type="string", dest="prefix",
                      help="The prefix to use for naming cluster nodes",
                      default=DEFAULT_PREFIX)
    parser.add_option("--dir",
                      action="store", type="string", dest="hadoop_dir",
                      help="The directory containing hadoop config files",
                      default=DEFAULT_HADOOP_DIR)
    parser.add_option("--clustersize",
                      action="store", type="string", dest="clustersize",
                      help="Number of virtual cluster nodes to create " \
                           "(mandatory argument)",
                      default=None)
    parser.add_option("--cyclades",
                      action="store", type="string", dest="cyclades",
                      help=("The API URI to use to reach the Cyclades API " \
                            "(default: %s)" % DEFAULT_CYCLADES),
                      default=DEFAULT_CYCLADES)
    parser.add_option("--token",
                      action="store", type="string", dest="token",
                      help="The token to use for authentication to the API",
                      default=DEFAULT_TOKEN)
    parser.add_option("--flavor-id",
                      action="store", type="int", dest="flavorid",
                      metavar="FLAVOR ID",
                      help="Choose flavor id for the virtual hardware " \
                           "of cluster nodes (mandatory argument)",
                      default=None)
    parser.add_option("--image-id",
                      action="store", type="string", dest="imageid",
                      metavar="IMAGE ID",
                      help="The image id to use to creating cluster nodes " \
                           "(mandatory argument)",
                      default=None)
    parser.add_option("--show-stale",
                      action="store_true", dest="show_stale",
                      help="Show stale servers from previous runs, whose " \
                           "name starts with the specified prefix, see " \
                           "--prefix",
                      default=False)
    parser.add_option("--delete-stale",
                      action="store_true", dest="delete_stale",
                      help="Delete stale servers from previous runs, whose "\
                           "name starts with the specified prefix, see "\
                           "--prefix",
                      default=False)

    # FIXME: Change the default for build-fanout to 10
    # FIXME: Allow the user to specify a specific set of Images to test

    (opts, args) = parser.parse_args(args)

    # Verify arguments
    if opts.delete_stale:
        opts.show_stale = True

    if not opts.show_stale:
        if opts.imageid is None:
            print >>sys.stderr, "The --image-id argument is mandatory."
            parser.print_help()
            sys.exit(1)

        if opts.flavorid is None:
            print >>sys.stderr, "The --flavor-id argument is mandatory."
            parser.print_help()
            sys.exit(1)

        if opts.clustersize is None:
            print >>sys.stderr, "The --clustersize argument is mandatory."
            parser.print_help()
            sys.exit(1)

    return (opts, args)


def cleanup_servers(prefix=DEFAULT_PREFIX, delete_stale=False):
    c = ComputeClient(CYCLADES, TOKEN)

    servers = c.list_servers()
    stale = [s for s in servers if s["name"].startswith(prefix)]

    if len(stale) == 0:
        return

    print >> sys.stderr, "Found these stale servers from previous runs:"
    print "    " + \
          "\n    ".join(["%d: %s" % (s["id"], s["name"]) for s in stale])

    if delete_stale:
        print >> sys.stderr, "Deleting %d stale servers:" % len(stale)
        for server in stale:
            c.delete_server(server["id"])
        print >> sys.stderr, "    ...done"
    else:
        print >> sys.stderr, "Use --delete-stale to delete them."


def cmd_execute(cmd):
    """ Execute cmd through subprocess """
    print "Executing: ", cmd
    proc = subprocess.Popen(cmd, shell=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    ret = proc.wait()
    for line in proc.stdout:
        print("stdout: " + line.rstrip())
    for line in proc.stderr:
        print("stderr: " + line.rstrip())
    return ret


def create_machine(opts, c, i):
    """ Create a vm hadoop node """
    servername = "%s-%d" % (opts.prefix, i)
    # print "i=",i, "servername=", servername
    if i>0:
        # copy ssh key to both root and hduser accounts
        personality = [{"path": "/home/hduser/.ssh/authorized_keys",
                        "owner": "hduser", "group": "hadoop",
                        "mode": 0600,
                        "contents": b64encode(open(opts.hadoop_dir+"/master_id_rsa_pub").read())},
                       {"path": "/home/hduser/.ssh/config",
                        "owner": "hduser", "group": "hadoop",
                        "mode": 0600,
                        "contents": b64encode("StrictHostKeyChecking no")},
	   	       {"path": "/root/.ssh/authorized_keys",
                        "owner": "root", "group": "root",
                        "mode": 0600,
                        "contents": b64encode(open(DEFAULT_SSH_PUB).read())},
                       {"path": "/root/.ssh/config",
                        "owner": "root", "group": "root",
                        "mode": 0600,
                        "contents": b64encode("StrictHostKeyChecking no")}]
    else:
        personality = [{"path": "/home/hduser/.ssh/authorized_keys",
                        "owner": "hduser", "group": "hadoop",
                        "mode": 0600,
                        "contents": b64encode(open(DEFAULT_SSH_PUB).read())},
                       {"path": "/home/hduser/.ssh/config",
                        "owner": "hduser", "group": "hadoop",
                        "mode": 0600,
                        "contents": b64encode("StrictHostKeyChecking no")},
                       {"path": "/root/.ssh/authorized_keys",
                        "owner": "root", "group": "root",
                        "mode": 0600,
                        "contents": b64encode(open(DEFAULT_SSH_PUB).read())},
                       {"path": "/root/.ssh/config",
                        "owner": "root", "group": "root",
                        "mode": 0600,
                        "contents": b64encode("StrictHostKeyChecking no")}]

    log.info("\nCreating node %s", servername)
    server = c.create_server(servername, opts.flavorid,
                             opts.imageid,
                             personality=personality)

    # Wait until all servers are up and running
    # log.info("Done. Waiting for node to become ACTIVE...")
    while True:
        done = False
        while not done:
            try:
                servers = c.list_servers(detail=True)
                done = True
            except:
                log.info("Will retry...")
                pass
#       cluster = [s for s in servers if (s["name"].startswith(opts.prefix) and (s["name"].startswith(opts.prefix+"-0")==False))]
        cluster = [s for s in servers if s["name"].startswith(opts.prefix)]

        active = [s for s in cluster if s["status"] == "ACTIVE"]
        attached = [s for s in cluster if "attachments" in s]
        build = [s for s in cluster if s["status"] == "BUILD"]
        error = [s for s in cluster if s["status"] not in ("ACTIVE", "BUILD")]
        if error:
            log.fatal("Server failed.")
	    print "Cluster = ", cluster
            return {}
        for n in cluster:
            if n["name"] == servername: progress = n["progress"]
        print '\rBuilding vm, %s%% progress' % str(progress),
        sys.stdout.flush()
        if len(build) == 0:
            break
        time.sleep(2)

    print
    # Find machine's ip
    ip = ''
    servers = c.list_servers(detail=True)
    for item in servers: 
        #print item
        if item["name"] == servername:
            ip = item["attachments"]["values"][0]["ipv4"]
	    #print "ip=", ip
    if ip=='':
        log.info("Error locating server ip")

    # Ping machine until it comes alive
    #log.info("Ping...")
    #time.sleep(5)
    cmd = "".join("ping -c 2 -w 3 %s" % (ip))
    while True:
        retval = cmd_execute(cmd)
        if (retval == 0):
            break    
        time.sleep(12)
    retval = cmd_execute(cmd)

    # Perform a short delay, before running rcp to get the hostname
    log.info("Delay...")
    time.sleep(10)
    try:
        cmd = "".join("rcp -o ServerAliveInterval=120 -o StrictHostKeyChecking=no root@%s:/etc/hostname %s/hostname_%d" % (ip, opts.hadoop_dir, i))
        retval = cmd_execute(cmd)
        #print "Returned value:", retval
    except:
        log.info("SSH error in getting hostname. Execution aborted.")
        return {}

    if i==0:
        log.info("Preparing master key...")

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            ssh.connect(ip, username = 'hduser')
	    ssh_cmd = 'ssh-keygen -q -t rsa -P \"\" -f /home/hduser/.ssh/id_rsa'
            #print ssh_cmd
            stdin, stdout, stderr = ssh.exec_command(ssh_cmd)
            time.sleep(2)
            output = stdout.readlines()
	    #print output
            ssh_cmd = 'cat /home/hduser/.ssh/id_rsa.pub >> /home/hduser/.ssh/authorized_keys'
            #print ssh_cmd
            stdin, stdout, stderr = ssh.exec_command(ssh_cmd)
            time.sleep(2)
            output = stdout.readlines()
	    #print output
            port = 10000 + int(ip.split('.')[3])
            cmd = "".join("rcp -o ServerAliveInterval=120 -o StrictHostKeyChecking=no root@%s:/home/hduser/.ssh/id_rsa.pub %s/master_id_rsa_pub" % (ip, opts.hadoop_dir))
            retval = cmd_execute(cmd)
#            print "Returned value:", retval
        except:
            log.info("SSH error. Execution aborted.")
            return {}
        ssh.close()

    log.info("Done.")
    if i!=0:
        return server
    else:
	return {'node_id': '0'}


def main():
    """Parse arguments, use kamaki to create cluster, setup using ssh"""

    (opts, args) = parse_arguments(sys.argv[1:])

    global CYCLADES, TOKEN

    CYCLADES = opts.cyclades
    TOKEN = opts.token

    # Cleanup stale servers from previous runs
    if opts.show_stale:
        cleanup_servers(prefix=opts.prefix, delete_stale=opts.delete_stale)
        return 0

    # Initialize a kamaki instance
    c = CycladesClient(CYCLADES, TOKEN)

    # Spawn a cluster of 'cnt' servers
    cnt = int(opts.clustersize)
    # Initialize
    nodes = []
    masterName = ''
    # Create a file to store the root password for later use
    pass_fname = opts.hadoop_dir+'/bak/adminPass'+str(datetime.now())[:19].replace(' ', '')
    adminPass_f = open(pass_fname, 'w')

    # Create master node (0th node)
    server = {}
    server = create_machine(opts, c, 0)
    if server == {}:
        return;
    servername = "%s-0" % (opts.prefix)
    masterName = servername
    nodes.append(server)

    # Create slave nodes
    if cnt>1:
        for i in xrange(1, cnt):
            server = {}
            server = create_machine(opts, c, i)
            if server == {}:
                return;
            nodes.append(server)
            servername = "%s-%d" % (opts.prefix, i)
            # Write the root password
            adminPass_f.write('machine = %s, password = %s\n' % (servername, server['adminPass']))

    adminPass_f.close()

    # Read all the hostname files to get the hostname strings and store them in a vector
    for i in xrange(0, cnt):
        hname_f = open('%s/hostname_%d' % (opts.hadoop_dir, i), 'r')
        hostnames.append(hname_f.readline())
        hostnames[i] = hostnames[i][:-1]
        hname_f.close()
    
    print "nodes=", nodes
    # Setup Hadoop files and settings on all cluster nodes
    servers = c.list_servers(detail=True)
    cluster = [s for s in servers if s["name"].startswith(opts.prefix)]
    cluster = [(s["name"], s["attachments"]["values"][0]["ipv4"]) for s in cluster]
    cluster = sorted(cluster)
    #print "Cluster v2:", cluster

    # Create the 'cluster' dictionary out of servers, with only hadoop-relevant keys
    print "hostnames=", hostnames

    etc_hosts_f = open("/etc/hosts", "r")
    etc_hosts = etc_hosts_f.readlines()
    etc_hosts_f.close()

    hadoop_ip_list = ""
    for i in xrange(0, cnt):
        for s in cluster:
            if s[0] == opts.prefix+"-"+str(i):
                hadoop_ip_list = hadoop_ip_list + "".join("%s\t%s %s\n" % (s[1], s[0], hostnames[i]))

    print "hadoop_ip_list="
    print hadoop_ip_list

    # prepare hadoop config files
    template = open(opts.hadoop_dir+'/mapred-site-template.xml', 'r')
    mapred = open(opts.hadoop_dir+'/mapred-site.xml', 'w')
    for line in template.readlines():
        line = line.replace("MASTER_IP",masterName).strip()
        mapred.write(line+'\n')
    template.close()
    mapred.close()

    template = open(opts.hadoop_dir+'/core-site-template.xml', 'r')
    core = open(opts.hadoop_dir+'/core-site.xml', 'w')
    for line in template.readlines():
        line = line.replace("MASTER_IP",masterName).strip()
        core.write(line+'\n')
    template.close()
    core.close()

    masters = open(opts.hadoop_dir+'/masters', 'w')
    masters.write(masterName+'\n')
    masters.close()

    slaves = open(opts.hadoop_dir+'/slaves', 'w')
    i=0
    for s in cluster:
        slaves.write(opts.prefix+"-"+str(i)+'\n')
        i=i+1
    slaves.close()

    i = 0 # 0-th node is the master
    for s in cluster:
        print "node = ",s
        log.info("Injecting files to node %s" % (s[1]))

        hosts = open(opts.hadoop_dir+'/hosts_'+str(i), 'w')
        hosts.write("127.0.0.1\tlocalhost\n")
        hosts.write("".join("127.0.1.1\t%s\n" % (hostnames[i])))
        hosts.write(hadoop_ip_list)
        for line in etc_hosts[3:]:
           hosts.write(line)
        hosts.close()

        time.sleep(1)

        cmd = "".join("rcp -o ServerAliveInterval=120 -o StrictHostKeyChecking=no %s/hosts_%d root@%s:/etc/hosts" % (opts.hadoop_dir, i, s[1]))
        retval = cmd_execute(cmd)
        #print "Returned value:", retval

        cmd = "".join("rcp -o StrictHostKeyChecking=no %s/masters root@%s:/usr/local/hadoop/conf" % (opts.hadoop_dir, s[1]))
        retval = cmd_execute(cmd)
        #print "Returned value:", retval

        cmd = "".join("rcp -o StrictHostKeyChecking=no %s/slaves root@%s:/usr/local/hadoop/conf" % (opts.hadoop_dir, s[1]))
        retval = cmd_execute(cmd)
        #print "Returned value:", retval

        cmd = "".join("rcp -o StrictHostKeyChecking=no %s/mapred-site.xml root@%s:/usr/local/hadoop/conf" % (opts.hadoop_dir, s[1]))
        retval = cmd_execute(cmd)
        #print "Returned value:", retval

        cmd = "".join("rcp -o StrictHostKeyChecking=no %s/core-site.xml root@%s:/usr/local/hadoop/conf" % (opts.hadoop_dir, s[1]))
        retval = cmd_execute(cmd)
        print "Returned value:", retval

        i = i+1

if __name__ == "__main__":
    sys.exit(main())

