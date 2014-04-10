#!/usr/bin/env python

"""
Create a multi-node Hadoop cluster on top of ~okeanos
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
import json 
from base64 import b64encode
from datetime import datetime

from kamaki.clients.astakos import AstakosClient
from kamaki.clients.compute import ComputeClient
from kamaki.clients.cyclades import CycladesClient
from kamaki.clients.cyclades import CycladesNetworkClient 

DEFAULT_PREFIX = "hadoop"
DEFAULT_FLAVOR_ID, DEFAULT_IMAGE_ID = 13, '71ff6db1-f439-47c9-918f-32fc4f762efd'
DEFAULT_AUTHENTICATION_URL = 'https://accounts.okeanos.grnet.gr/identity/v2.0/'

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

def parse_arguments(args):
    """ Parse command-line arguments, get values """
    from optparse import OptionParser

    kw = {}
    kw["usage"] = "%prog [options]"
    kw["description"] = \
        "%prog creates and configures a Hadoop multi-node cluster on top of ~okeanos."

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
    parser.add_option("--extend",
                      action="store_true", dest="extend",
                      help="Extends an existing cluster with new nodes",
                      default=False)
    parser.add_option("--cyclades",
                      action="store", type="string", dest="cyclades",
                      help=("The API URI to use to reach the Cyclades API " \
                            "(default: %s)" % DEFAULT_AUTHENTICATION_URL),
                      default=DEFAULT_AUTHENTICATION_URL)
    parser.add_option("--token",
                      action="store", type="string", dest="token",
                      help="The token to use for authentication to the API",
                      default=None)
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

    (opts, args) = parser.parse_args(args)

    # Verify arguments
    if opts.delete_stale:
        opts.show_stale = True

    if not opts.show_stale:
        if opts.imageid is None:
            print >>sys.stderr, "The --image-id argument is mandatory."
            parser.print_help()
            sys.exit(1)

        if opts.token is None:
            print >>sys.stderr, "The --token argument is mandatory."
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
    """ Cleanup stale servers """
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
    log.info(cmd)
    proc = subprocess.Popen(cmd, shell=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    ret = proc.wait()
    for line in proc.stdout:
        print("stdout: " + line.rstrip())
    for line in proc.stderr:
        print("stderr: " + line.rstrip())
    return ret


def create_machine(opts, c, i):
    """ Create the i-th Hadoop node (vm) """
    servername = "%s-%d" % (opts.prefix, i)
    personality = [{"path": "/root/.ssh/authorized_keys",
                    "owner": "root", "group": "root",
                    "mode": 0600,
                    "contents": b64encode(open(DEFAULT_SSH_PUB).read())},
                   {"path": "/root/.ssh/config",
                    "owner": "root", "group": "root",
                    "mode": 0600,
                    "contents": b64encode("StrictHostKeyChecking no")}]


    log.info("\nCreating node %s", servername)

    myNetworks = my_network_client.list_networks()
    NetWork_free = parseNetwork(myNetworks,'public')

    if i==0:
        # Get a (or create a new) floating ip 
        FoundIp = None
        for item in my_network_client.list_floatingips():
            if my_network_client.get_floatingip_details(item['id'])['instance_id']==None: 
                FoundIp = item
             
        if FoundIp:
            myIp = FoundIp
        else:
            myIp = my_network_client.create_floatingip(NetWork_free)
        LastIp = myIp.get("floating_ip_address")
        server = c.create_server(servername, opts.flavorid, opts.imageid, 
			     networks=[
				{'uuid': myIp['floating_network_id'], 'fixed_ip': LastIp},
				{'uuid': my_vnat_network['id']}],
                             personality=personality)
    else:
        server = c.create_server(servername, opts.flavorid, opts.imageid, networks=[{'uuid': my_vnat_network['id']}],
                             personality=personality)

    # Wait until server is up and running
    while True:
        done = False
        while not done:
            try:
                servers = c.list_servers(detail=True)
                done = True
            except:
                log.info("Will retry...")
                pass
        cluster = [s for s in servers if s["name"].startswith(opts.prefix)]
        time.sleep(1)

        active = [s for s in cluster if s["status"] == "ACTIVE"]
        build = [s for s in cluster if s["status"] == "BUILD"]
        error = [s for s in cluster if s["status"] not in ("ACTIVE", "BUILD", "STOPPED")]
        if error:
            log.fatal("\nServer failed.")
	    print "error = ", error
            return {}
        for n in cluster:
            if n["name"] == servername: progress = n["progress"]
        print '\rBuilding vm: %s%% progress' % str(progress),
        sys.stdout.flush()
        if len(build) == 0:
            break
        time.sleep(2)

    time.sleep(5)
    ip = ''
    adminPass = ''
    vm = [s for s in c.list_servers(detail=True) if s["name"] == servername]
    item = vm[0]
    if "attachments" in item:
        if "ipv4" in item["attachments"][1]:
            ip = item["attachments"][1]["ipv4"]
        if i==0: 
            if "ipv4" in item["attachments"][2]:
                ip = item["attachments"][2]["ipv4"]
    if ip=='':
        log.info("Error locating server ip. Execution aborted.")
        log.info("Password:"+adminPass)
        return {}

    # Ping machine until it comes alive
    cmd = "".join("ping -c 2 -w 3 %s" % (ip))
    while True:
        retval = cmd_execute(cmd)
        if (retval == 0):
            break    
        time.sleep(16)
    retval = cmd_execute(cmd)

    # Install necessary software for ansible
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(ip, username = 'root')
        # update apt sources to retrieve packages from an ipv6 site
        ssh_cmd = 'sed -i \'1ideb http:\/\/ftp.debian.org\/debian\/ wheezy main\' /etc/apt/sources.list'
        log.info("ssh as root@%s succeeded", ip)
        log.info("Executing: %s", ssh_cmd)
        stdin, stdout, stderr = ssh.exec_command(ssh_cmd)
        time.sleep(2)
        output = stdout.readlines()
        ssh_cmd = 'sed -i \'1ideb-src http:\/\/ftp.debian.org\/debian\/ wheezy-updates main\' /etc/apt/sources.list'
        log.info("Executing: %s", ssh_cmd)
        stdin, stdout, stderr = ssh.exec_command(ssh_cmd)
        time.sleep(2)
        output = stdout.readlines()
        ssh_cmd = 'sed -i \'1ideb http:\/\/ftp.debian.org\/debian\/ wheezy-updates main\' /etc/apt/sources.list'
        log.info("Executing: %s", ssh_cmd)
        stdin, stdout, stderr = ssh.exec_command(ssh_cmd)
        time.sleep(2)
        output = stdout.readlines()

        # install python
        ssh_cmd = 'apt-get update; apt-get -y install python python-apt'
        log.info("Executing: %s", ssh_cmd)
        stdin, stdout, stderr = ssh.exec_command(ssh_cmd)
        time.sleep(2)
        output = stdout.readlines()
        # for vnat: change the hostname
        ssh_cmd = 'hostname '+item["name"]
        log.info("Executing: %s", ssh_cmd)
        stdin, stdout, stderr = ssh.exec_command(ssh_cmd)
        time.sleep(2)
        output = stdout.readlines()
        # for vnat: add the relevant entry in /etc/hosts
#        ssh_cmd = 'echo \''+ip+'\t'+item["name"]+'\''+' >> \/etc\/hosts'
#        log.info("Executing: %s", ssh_cmd)
#        stdin, stdout, stderr = ssh.exec_command(ssh_cmd)
#        time.sleep(2)
#        output = stdout.readlines()
        ssh.close()
    except Exception, e:
        log.info(e)
        log.info("SSH error. Execution aborted.")
        #print "root password: " + adminPass
        return {}

    # enable ssh login
    cmd = 'ssh-keyscan -H '+ ip + ' >> ~/.ssh/known_hosts'
    os.system(cmd)
    # write master name in master.j2
    if i==0:
        masterf = open(opts.hadoop_dir+'/vnat/masters.j2', 'w')
        masterf.write(item["name"]+'\n')
        masterf.close()
        
    # CHECK!
    return server

def enable_ssh_login(master_ip, slave_ip_list):
    """Enable passwordless ssh login from master to slaves"""

    ssh = paramiko.SSHClient()
    print "master_ip=",master_ip
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(master_ip, username = 'root')
        log.info("Logged in.")
        for slave_ip in slave_ip_list:
            print "m=", master_ip, "s=", slave_ip
            ssh_cmd = "su - hduser -c \""+"ssh-keyscan -H "+slave_ip+ " >> ~/.ssh/known_hosts\""
            log.info("Executing: %s", ssh_cmd)
            stdin, stdout, stderr = ssh.exec_command(ssh_cmd)
            time.sleep(1)
            output = stdout.readlines()
        ssh.close()
    except Exception, e:
        print e
        return 

def parseAstakosEndpoints(decoded_response,itemToSearch):
    """ parse the endpoints and get the publicURL of the selected service """
    json.dumps(decoded_response,sort_keys=True,indent=4, separators=(',', ': '))
    jsonData = decoded_response["access"]['serviceCatalog']
    for item in jsonData:
        name = item.get("name")
        endpoints=item.get("endpoints");
        for details in endpoints:
            PUBLIC_URL = details.get("publicURL")
        if name == itemToSearch:
            return PUBLIC_URL


def parseNetwork(decoded_response,itemToSearch):
    """ Parse the endpoints and get the publicURL of the selected service """
    jsonData = decoded_response
    for item in jsonData:
        name = item.get("public")
        if name == itemToSearch:
            return item.get("id")

def main():
    """Parse arguments, use kamaki to create cluster, setup using ansible playbooks"""

    (opts, args) = parse_arguments(sys.argv[1:])

    global CYCLADES, TOKEN, my_vnat_network, my_network_client

    AUTHENTICATION_URL = opts.cyclades
    TOKEN = opts.token

    # Cleanup stale servers from previous runs
    if opts.show_stale:
        cleanup_servers(prefix=opts.prefix, delete_stale=opts.delete_stale)
        return 0

    # Initialize a kamaki instance, get endpoints
    user = AstakosClient(AUTHENTICATION_URL, TOKEN)
    my_accountData = user.authenticate()
    endpoints = user.get_endpoints() 
    cyclades_endpoints = user.get_endpoints('compute')
    cyclades_base_url = parseAstakosEndpoints(endpoints, 'cyclades_compute')
    cyclades_network_base_url = parseAstakosEndpoints(endpoints, 'cyclades_network')
    my_cyclades_client = CycladesClient(cyclades_base_url, TOKEN)
    my_compute_client = ComputeClient(cyclades_base_url, TOKEN)
    my_network_client = CycladesNetworkClient(cyclades_network_base_url, TOKEN) 

    my_vnat_network = {}

    # check if 'Hadoop' vnat is created...
    hadoop_vnat_created = False
    my_network_dict = my_network_client.list_networks()
    for n in my_network_dict:
        if n['name'] == 'Hadoop': 
            hadoop_vnat_created = True
            my_vnat_network = n

    # ...else create it
    if hadoop_vnat_created == False:
        log.info("Creating vNAT")
        my_vnat_network = my_network_client.create_network(type='MAC_FILTERED', name='Hadoop');
        my_subnet = my_network_client.create_subnet(network_id=my_vnat_network['id'], cidr='192.168.0.0/24');

    cnt = int(opts.clustersize)	# calculate size of cluster into 'cnt'
    # Initialize
    nodes = []
    masterName = ''
    # Create a file to store the root password for later use
    pass_fname = opts.hadoop_dir+'/bak/adminPass'+str(datetime.now())[:19].replace(' ', '')
    adminPass_f = open(pass_fname, 'w')

    initialClusterSize = 0
    server = {}
    if opts.extend == False:
        # Create master node (0th node)
        server = create_machine(opts, my_cyclades_client, 0)
        if server == {}:
            return
    else:
        servers = my_cyclades_client.list_servers(detail=True)
        cluster = [s for s in servers if s["name"].startswith(opts.prefix)]
        initialClusterSize = len(cluster)
        if initialClusterSize==0:
            log.info("Cluster cannot be expanded: it does not exist.")
            return

    servername = "%s-0" % (opts.prefix)
    masterName = servername
    nodes.append(server)

    # Create slave (worker) nodes
    if cnt>1 or opts.extend:
        startingOffset = 1
        if opts.extend: startingOffset = initialClusterSize
        for i in xrange(startingOffset, initialClusterSize+cnt):
            server = {}
            server = create_machine(opts, my_cyclades_client, i)
            if server == {}:
                return;
            nodes.append(server)
            servername = "%s-%d" % (opts.prefix, i)
            # Write the root password to a file
            adminPass_f.write('machine = %s, password = %s\n' % (servername, server['adminPass']))

    adminPass_f.close()

    # Setup Hadoop files and settings on all cluster nodes
    # Create the 'cluster' dictionary out of servers, with only Hadoop-relevant keys (name, ip, integer key)
    servers = my_cyclades_client.list_servers(detail=True)
    cluster = [s for s in my_cyclades_client.list_servers(detail=True) if s["name"].startswith(opts.prefix)]
    cluster0 = [(s["name"], s["attachments"], int(s["name"][s["name"].find('-')+1:])) for s in cluster]
    cluster0 = sorted(cluster0, key=lambda cluster0: cluster0[2])
    cluster = [(cluster0[0][0], cluster0[0][1][2]["ipv4"], cluster0[0][2])]	# master IP, different index 
    cluster2 = [(s[0], s[1][1]['ipv4'], int(s[2])) for s in cluster0[1:]]	# slave IPs
    cluster += cluster2

    # Prepare Ansible-Hadoop config files (hosts, conf/slaves. vnat/etchosts)
    hosts = open(opts.hadoop_dir+'/hosts', 'w')
    hosts.write('[master]\n')
    etchosts = open(opts.hadoop_dir+'/vnat/etchosts', 'w')
    for i in xrange(0, initialClusterSize+cnt):
        for s in cluster:
            if s[0] == opts.prefix+"-"+str(i):
                if s[0] == masterName:
                    hosts.write(s[1]+'\n\n'+'[slaves]\n')
                else:
                    hosts.write(s[1]+'\n')
                etchosts.write(s[1]+'\t'+s[0]+'\n')
    hosts.close()
    etchosts.close()

    slaves = open(opts.hadoop_dir+'/vnat/slaves', 'w')
    for s in cluster[1:]:
        slaves.write(s[0]+'\n')
    slaves.close()

    # Execute respective ansible playbook
    if (opts.extend==False):
        cmd = "ansible-playbook hadoop_vnat.yml -i hosts -vv --extra-vars \""+"is_master=True, master_node="+cluster[0][0]+" master_ip="+cluster[0][1]+"\""+" -l master"
        print cmd
        retval = os.system(cmd)
        cmd = "ansible-playbook hadoop_vnat.yml -i hosts -vv --extra-vars \""+"is_slave=True, master_node="+cluster[0][0]+" master_ip="+cluster[0][1]+"\""+" -l slaves"
        print cmd
        retval = os.system(cmd)
        slave_ip_list = []
        for i in xrange(1, cnt):
            slave_ip_list.append(cluster[i][0])
        enable_ssh_login(cluster[0][1], [cluster[0][0]])
        enable_ssh_login(cluster[0][1], slave_ip_list)
    else:
        hosts_latest = open(opts.hadoop_dir+'/hosts.latest', 'w')
        hosts_latest.write('[master]\n')
        hosts_latest.write(cluster[0][1]+'\n\n'+'[slaves]\n')
        for i in xrange(initialClusterSize, initialClusterSize+cnt):
            hosts_latest.write(cluster[i][1]+'\n')
        hosts_latest.close()
        # update etc/hosts in all nodes - TODO: de-duplicate entries
        cmd = "ansible-playbook hadoop_vnat.yml -i hosts -vv --extra-vars \""+"is_master=True, master_ip="+cluster[0][1]+"\""+" -t etchosts"
        print cmd
        retval = os.system(cmd) 
        cmd = "ansible-playbook hadoop_vnat.yml -i hosts.latest -vv --extra-vars \""+"is_slave=True, master_node="+cluster[0][0]+" master_ip="+cluster[0][1]+"\""+" -l slaves"
        print cmd
        retval = os.system(cmd) 
        slave_ip_list = []
        for i in xrange(initialClusterSize, initialClusterSize+cnt):
            slave_ip_list.append(cluster[i][0])
        print "slave_ip_list=", slave_ip_list 
        enable_ssh_login(cluster[0][1], slave_ip_list)

    # Update conf/slaves in master
    cmd = "ansible-playbook hadoop_vnat.yml -i hosts -vv --extra-vars \""+"is_master=True, master_ip="+cluster[0][1]+"\""+" -l master -t slaves"
    print cmd
    retval = os.system(cmd)

    log.info("Done.")

if __name__ == "__main__":
    sys.exit(main())

