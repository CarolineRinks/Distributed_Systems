# zk_client.py
import os
import sys
import time
import argparse
from kazoo.client import KazooClient, KazooState
import logging

def listener4state(state):
    if state == KazooState.LOST:
        print("Current state is now = LOST")
    elif state == KazooState.SUSPENDED:
        print("Current state is now = SUSPENDED")
    elif state == KazooState.CONNECTED:
        print("Current state is now = CONNECTED")
    else:
        print("Current state now = UNKNOWN !! Cannot happen")

class ZK_Driver():
    """The ZooKeeper Driver Class."""
    def __init__(self):
        self.zk = None
        self.zkIPAddr = "127.0.0.1"
        self.zkPort = 2181
        self.zkName = ''
        self.zkVal = ''

    def dump(self):
        print("=================================")
        print("Server IP: {}, Port: {}; Path = {} and Val = {}".format(
            self.zkIPAddr, self.zkPort, self.zkName, self.zkVal))
        print("=================================")

    def init_driver(self):
        try:
            self.dump()
            hosts = f"{self.zkIPAddr}:{self.zkPort}"
            print("Driver::init_driver -- instantiate zk obj: hosts = {}".format(hosts))
            self.zk = KazooClient(hosts)
            self.zk.add_listener(listener4state)
            print("Driver::init_driver -- state after connect = {}".format(self.zk.state))
            logging.getLogger("kazoo.client").setLevel(logging.WARNING)
        except Exception as e:
            print("Unexpected error in init_driver:", sys.exc_info()[0])
            raise e

    def start_session(self):
        try:
            self.zk.start()
        except Exception as e:
            print("Exception thrown in start():", sys.exc_info()[0])
            raise e

    def stop_session(self):
        try:
            self.zk.stop()
        except Exception as e:
            print("Exception thrown in stop():", sys.exc_info()[0])
            raise e

    def create_znode(self, role, name, ip, port):
        """Create an ephemeral znode for a given role."""
        try:
            path = ""
            if role == "publisher":
                path = f"/root/pubs/{name}-"
            elif role == "subscriber":
                path = f"/root/subs/{name}"
            else:
                print("Invalid role", sys.exc_info()[0])
                return
            value = f"{ip}:{port}"
            value_bytes = value.encode('utf-8')
            print(f"Creating {role} znode with value '{value}'")
            created_path = self.zk.create(path, value=value_bytes, ephemeral=True, sequence=True, makepath=True)
            print("Done creating znode")
            return created_path
        except Exception as e:
            print("Exception thrown in create():", sys.exc_info()[0])
            return None

    def update_lease(self, lease_path, lease_duration):
        """Write a lease expiration timestamp into lease_path."""
        expiry_time = time.time() + lease_duration
        expiry_str = str(expiry_time).encode('utf-8')
        if not self.zk.exists(lease_path):
            self.zk.create(lease_path, expiry_str, makepath=True)
        else:
            self.zk.set(lease_path, expiry_str)
        print(f"Lease updated at {lease_path} to expire at {expiry_time}")

    def get_lease_expiry(self, lease_path):
        """Return the lease expiry time as a float."""
        if self.zk.exists(lease_path):
            data, stat = self.zk.get(lease_path)
            return float(data.decode('utf-8'))
        return None
