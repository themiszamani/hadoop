#!/usr/bin/env python

"""
Executes commands on hadoop nodes
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
DEFAULT_USER = "root"
DEFAULT_HADOOP_DIR = os.path.expanduser(os.path.join("~", "hadoop"))

CYCLADES = None

# Setup logging
logging.basicConfig(format="%(message)s")
log = logging.getLogger("okeanos-cluster")
log.setLevel(logging.INFO)

def parse_arguments(args):
    from optparse import OptionParser

    kw = {}
    kw["usage"] = "%prog [options]"
    kw["description"] = \
        "%prog executes commands on ~okeanos nodes through ssh."

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
    parser.add_option("--start",
                      action="store", type="string", dest="start",
                      help="Starting node of cluster (mandatory argument)",
                      default=None)
    parser.add_option("--end",
                      action="store", type="string", dest="end",
                      help="Ending node of cluster (mandatory argument)",
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
    parser.add_option("--user",
                      action="store", type="string", dest="user",
                      help="The user to login to remote machine",
                      default=DEFAULT_USER)
    parser.add_option("--command",
                      action="store", type="string", dest="ssh_command",
                      help="Command to execute on all nodes",
                      default="")

    (opts, args) = parser.parse_args(args)

    # Verify arguments
    if opts.start is None:
        print >>sys.stderr, "The --start argument is mandatory."
        parser.print_help()
        sys.exit(1)

    if opts.end is None:
        print >>sys.stderr, "The --end argument is mandatory."
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
    print cmd
    proc = subprocess.Popen(cmd, shell=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    ret = proc.wait()
    #for line in proc.stdout: print("stdout: " + line.rstrip())
    #for line in proc.stderr: print("stderr: " + line.rstrip())
    return ret


def main():
    """Parse arguments, connect to each vm, execute command"""

    (opts, args) = parse_arguments(sys.argv[1:])

    global CYCLADES, TOKEN

    CYCLADES = opts.cyclades
    TOKEN = opts.token

    # Initialize a kamaki instance
    c = CycladesClient(CYCLADES, TOKEN)

    cnt = int(opts.end)
    servers = c.list_servers(detail=True)
    cluster = [s for s in servers if s["name"].startswith(opts.prefix)]
    cluster = [(s["name"], s["attachments"]["values"][0]["ipv4"], int(s["name"][s["name"].find('-')+1:])) for s in cluster]
    cluster = sorted(cluster, key=lambda cluster: cluster[2])
    print "Cluster:", cluster

    for i in xrange(int(opts.start), cnt):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ip = cluster[i][1]
        print "Executing ["+opts.ssh_command+"] on node "+str(i)+"/"+str(cnt)+" ("+ip+")"
        try:
            ssh.connect(ip, username = opts.user)
            stdin, stdout, stderr = ssh.exec_command(opts.ssh_command)
            output = stdout.readlines()
#	    if i % 10 == 0: print
        except:
            log.info("SSH error. Execution aborted.")
            return {}
        ssh.close()
    log.info("Done.")

if __name__ == "__main__":
    sys.exit(main())

