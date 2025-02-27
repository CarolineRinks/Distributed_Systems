from mininet.net import Mininet
from mininet.topo import SingleSwitchTopo
from mininet.node import CPULimitedHost, Controller
from mininet.cli import CLI

# Create a simple topology with one switch and three hosts
topo = SingleSwitchTopo(k=3)
net = Mininet(topo=topo, host=CPULimitedHost, controller=Controller)

# Start the network
net.start()

# Assign IPs to the hosts and start the CLI for interaction
host1 = net.get('h1')  # Publisher
host2 = net.get('h2')  # Subscriber
host3 = net.get('h3')  # Discovery service

# You can now access the Mininet CLI to run your applications or use these hosts to SSH into VMs.
CLI(net)

# Stop the network when done
net.stop()
