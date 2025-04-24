# broker_mw.py
import zmq
import logging
import time
import threading
from collections import defaultdict
from CS6381_MW import discovery_pb2
from .zkclient import ZK_Driver
from .zk_broker_client import BrokerLeaderElection

class BrokerMW:
    """Middleware for the broker with warm-passive replication and quorum logic."""

    def __init__(self, logger, args):
        self.logger = logger
        self.context = zmq.Context()
        self.poller = zmq.Poller()
        self.group = args.group
        
        # will hold { sub_id: { topic: pub_id, … }, … }
        #self.matchings = {}

        # will hold { pub_id: { topic: max_history, … }, … }
        self.publisher_limits = {}
        # nested buffer: history[pub_id][topic] → [ msg, … ]
        self.history = defaultdict(lambda: defaultdict(list))
        # matchings[sub_id][topic] = {"pub": pub_id, "n": num_requested}
        self.matchings = defaultdict(dict)
        # matchings[pub_id][topic] = {"sub": sub_id, "n": num_requested}
        self.matchings_pub = defaultdict(dict)
        # will hold { pub_id: ownership, … }
        self.ownerships = {}
        # topic_history_requirement[topic] = max N requested so far
        self.topic_history_requirement = defaultdict(int)

        self.discovery_req = None
        self.discovery_primary_endpoint = None
        self.quorum_active = True   # If < 3 broker replicas, block dissemination

        self.broker_ip = args.addr
        self.frontend_port = args.pub_port
        self.backend_port = args.sub_port

        self.zk_obj = ZK_Driver()
        self.zk_obj.init_driver() 
        self.zk_obj.start_session()

        self.configure()

        # Start ephemeral-sequential election for Broker
        self.broker_election = BrokerLeaderElection(
            self.zk_obj,
            self,  # pass self so that BrokerLeaderElection can call broker_mw.register_as_leader()
            ip=self.broker_ip,
            front_port=self.frontend_port,
            back_port=self.backend_port,
            election_path=f"/root/broker/group{str(self.group)}/replica",
            primary_path=f"/root/broker/group{str(self.group)}/primary",
            lease_path=f"/root/broker/group{str(self.group)}/lease",
            lease_duration=200
        )
        self.broker_election.start()

        # Watch ephemeral-sequential broker replicas to maintain quorum
        @self.zk_obj.zk.ChildrenWatch(f"/root/broker/group{str(self.group)}/replica")
        def watch_broker_replicas(children):
            count = len(children)
            if count < 3:
                self.logger.warning(f"Broker Quorum lost: only {count} replica(s). Stopping dissemination.")
                self.quorum_active = False
            else:
                self.logger.info(f"Broker Quorum restored with {count} replicas. Resuming dissemination.")
                self.quorum_active = True

        # Watch publishers to maintain publisher info (highest ownership strength and history size offered)
        @self.zk_obj.zk.ChildrenWatch("/root/pubs")
        def watch_publishers(children):
            self.logger.info(f"BrokerMW::watch_publishers - Detected change in /root/pubs")
            if (self.discovery_req is not None):
                time.sleep(5)   # sleep to allow state znode to update first
                self.refresh_publisher_info()
                self.prune_dead_publishers()
                # now re-match every existing subscriber
                for sub_id, topics in list(self.matchings.items()):
                    for topic, info in list(topics.items()):
                        requested_n = info["n"]
                        new_pub = self.get_best_publisher(topic, requested_n)
                        if new_pub is None:
                            # self.logger.info(
                            #     f"WARNING:::No publisher can serve {requested_n} msgs for {topic}; "
                            #     f"keeping old match {info['pub']}"
                            # )
                            continue
                        elif new_pub != info["pub"]:
                            self.logger.info(
                                f"Subscriber {sub_id} topic {topic}: "
                                f"switching from {info['pub']} → {new_pub}"
                            )
                            self.matchings[sub_id][topic]["pub"] = new_pub
                            self.matchings_pub[new_pub][topic]["sub"] = sub_id
            else:
                self.logger.warning("BrokerMW::watch_publishers - Skipping lookup discovery req not ready.")

    def configure(self):
        """Configure the broker and connect to Discovery."""
        self.logger.info("BrokerMW::configure - Setting up Broker")

        # Create REQ socket to Discovery
        self.discovery_req = self.context.socket(zmq.REQ)
        try:
            # Connect to the primary discovery service via zookeeper
            primary_data, _ = self.zk_obj.zk.get(f"/root/discovery/group{str(self.group)}/primary")
            primary_str = primary_data.decode('utf-8')
            self.discovery_primary_endpoint = f"tcp://{primary_str}"
            self.discovery_req.connect(self.discovery_primary_endpoint)
            self.logger.info(f"BrokerMW::configure - Connected to Discovery at {self.discovery_primary_endpoint}")
        except Exception as e:
            self.logger.info("ERROR:::BrokerMW::configure - error retrieving primary discovery pointer: " + str(e))

        # Watch for changes to the Discovery primary
        @self.zk_obj.zk.DataWatch(f"/root/discovery/group{str(self.group)}/primary")
        def watch_primary(data, stat, event):
            if data:
                new_primary = data.decode('utf-8')
                new_connect_str = f"tcp://{new_primary}"
                if new_connect_str != self.discovery_primary_endpoint:
                    self.logger.info("BrokerMW::watch_primary - discovery primary updated to " + new_connect_str)
                    if self.discovery_primary_endpoint:
                        try:
                            self.discovery_req.disconnect(self.discovery_primary_endpoint)
                        except Exception as e:
                            self.logger.info("ERROR:::BrokerMW::watch_primary - error disconnecting: " + str(e))
                    self.discovery_req.connect(new_connect_str)
                    self.discovery_primary_endpoint = new_connect_str

        # Create the frontend (SUB) socket for receiving publisher messages
        # and backend (PUB) socket for sending to subscribers
        self.frontend = self.context.socket(zmq.SUB)
        self.backend = self.context.socket(zmq.PUB)
        self.frontend.bind(f"tcp://{self.broker_ip}:{self.frontend_port}")
        self.frontend.setsockopt_string(zmq.SUBSCRIBE, "")      # subscribe to all topics
        self.backend.bind(f"tcp://{self.broker_ip}:{self.backend_port}")

        # Register frontend socket only
        self.poller.register(self.frontend, zmq.POLLIN)

        # Create history REP socket to serve history requests from subscribers
        hist_port = self.backend_port + 1
        self.hist_rep = self.context.socket(zmq.REP)
        self.hist_rep.bind(f"tcp://{self.broker_ip}:{hist_port}")
        self.logger.info(f"BrokerMW::configure - history REP bound to {hist_port}")

        t = threading.Thread(target=self._serve_history, daemon=True)
        t.start()

        self.logger.info("BrokerMW::configure completed")

    def refresh_publisher_info(self):
        self.logger.info(f"BrokerMW::refresh_publisher_info")
        # build a lookup-all-publishers request (leave lookup.topiclist empty)
        lookup = discovery_pb2.LookupPubByTopicReq()
        req = discovery_pb2.DiscoveryReq(
            msg_type=discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC,
            lookup_req=lookup
        )
        self.discovery_req.send(req.SerializeToString())
        reply = self.discovery_req.recv()
        resp  = discovery_pb2.DiscoveryResp()
        resp.ParseFromString(reply)

        new_limits = {}
        new_strengths = {}
        for pub in resp.lookup_resp.publishers:
            # pub.topics is a repeated field, same order as pub.history_size
            limits = { t: pub.history_size[i] for i, t in enumerate(pub.topics) }
            new_limits[pub.id] = limits
            new_strengths[pub.id] = pub.ownership

        self.publisher_limits = new_limits
        self.ownerships = new_strengths
        self.logger.info(f"BrokerMW::refresh_publisher_info - updated publisher info: {self.publisher_limits}")

    def prune_dead_publishers(self):
        alive = set(self.publisher_limits)
        # 1) remove stale history buffers
        for pub_id in list(self.history):
            if pub_id not in alive:
                del self.history[pub_id]

        # 2) drop any matchings that pointed at a dead pub
        for sub_id, topics in list(self.matchings.items()):
            for topic, info in list(topics.items()):
                if info["pub"] not in alive:
                    self.matchings[sub_id][topic] = None
                    self.matchings_pub[info["pub"]][topic] = None

                    self.logger.info(
                        f"Pruned matching: sub={sub_id}, topic={topic} (publisher left)"
                    )

    def get_best_publisher(self, topic, n):
        """
        Return the pub_id with the lowest ownership_strength
        among those offering ≥ n history for `topic`.
        """
        # filter candidates
        candidates = [
            pub_id
            for pub_id, limits in self.publisher_limits.items()
            if limits.get(topic, 0) >= n
        ]
        if not candidates:
            return None

        # pick min by strength
        best_pub = min(
            candidates,
            key=lambda pid: self.ownerships.get(pid, float('inf'))
        )
        return best_pub

    def _serve_history(self):
        '''History serving loop'''
        while True:
            req = self.hist_rep.recv_json()   # expects {"topic": T, "num": N, "sub_id": name}
            topic = req["topic"]
            n = int(req["num"])
            sub_id = req["sub_id"]
            # record the fact that someone wants up to n msgs of this topic
            self.topic_history_requirement[topic] = max(self.topic_history_requirement[topic], n)

            # pick publisher with lowest ownership_strength
            best_pub = self.get_best_publisher(topic, n)
            if best_pub is None:
                # no one can serve that much history
                self.hist_rep.send_json({
                    "error": f"No publisher has ≥{n} history for topic {topic}"
                })
                continue
            # record matching
            self.matchings[sub_id][topic] = {"pub": best_pub, "n": n}
            self.matchings_pub[best_pub][topic] = {"sub": sub_id, "n": n}

            # Retrieve pub's history from dictionary and send to sub over hist_rep socket
            hist = self.history.get(best_pub, {}).get(topic, [])[-n:]
            self.hist_rep.send_json({ "history": hist })
            self.logger.info(f"BrokerMW::_serve_history - replied {len(hist)} msgs for {best_pub}:{topic}")

    def register_as_leader(self):
        """
        Called by BrokerLeaderElection when this broker becomes the new leader.
        Sends a REGISTER_BROKER request to Discovery.
        """
        self.logger.info("BrokerMW::register_as_leader - Registering new leader broker with Discovery.")

        # Build the REGISTER_BROKER message.
        broker_info = discovery_pb2.BrokerInfo()
        broker_info.id = "LeaderBroker"  # or assign a unique ID as needed
        broker_info.addr = self.broker_ip
        broker_info.front_port = int(self.frontend_port)
        broker_info.back_port = int(self.backend_port)

        register_broker_req = discovery_pb2.RegisterBrokerReq()
        register_broker_req.broker.CopyFrom(broker_info)

        disc_req = discovery_pb2.DiscoveryReq()
        disc_req.msg_type = discovery_pb2.TYPE_REGISTER_BROKER
        disc_req.register_broker_req.CopyFrom(register_broker_req)

        # Retry loop: keep trying until registration succeeds.
        attempt = 1
        disc_resp = discovery_pb2.DiscoveryResp()  # initialize an empty response
        while True:
            self.logger.info(f"BrokerMW::register_as_leader - Attempt {attempt} to register broker.")
            self.discovery_req.send(disc_req.SerializeToString())
            reply_bytes = self.discovery_req.recv()
            disc_resp.ParseFromString(reply_bytes)
            
            if disc_resp.register_resp.status == discovery_pb2.STATUS_SUCCESS:
                self.logger.info("BrokerMW::register_as_leader - Broker registration successful!")
                break
            else:
                self.logger.warning("BrokerMW::register_as_leader - Broker registration failed. Retrying...")
                attempt += 1
                time.sleep(5)

    def event_loop(self):
        """Forward publisher messages to subscribers, if quorum is active."""
        self.logger.info("BrokerMW::event_loop - in the broker event loop")
        while True:
            events = dict(self.poller.poll())
            if self.frontend in events:
                # If quorum is not active, skip message forwarding
                self.logger.info("BrokerMW::event_loop - Checking quorum status")
                if not self.quorum_active:
                    self.logger.info("BrokerMW::event_loop - Quorum lost, dropping publisher message.")
                    # Read and discard the message
                    self.frontend.recv_string()
                    continue

                # Otherwise, save and forward the message
                message = self.frontend.recv_string()
                self.logger.info("BrokerMW::event_loop - Received message: " + message)

                # Save message in history
                topic, ts, payload, pub_id = message.split(":", 3)
                buf = self.history[pub_id][topic]
                buf.append(message)
                # trim publisher's history based on the publisher's limit
                max_sz = self.publisher_limits.get(pub_id, {}).get(topic, None)
                if max_sz is not None and len(buf) > max_sz:
                    buf.pop(0)

                # Filter out message from publishers that don't have best ownership strength
                #n = self.matchings_pub[pub_id][topic]["n"]
                #n = self.matchings_pub.get(pub_id, {}).get(topic, None).get("n", None)
                
                # try:
                #     info = self.matchings_pub.get(pub_id, {}).get(topic)
                #     n = info.get("n") if info else None

                #     if n is None:
                #         self.logger.info(f"ERROR:::BrokerMW::event_loop {self.matchings_pub}")
                # except Exception as e:
                #     self.logger.info("ERROR:::BrokerMW::event_loop error getting n from matchings_pub" + str(e))
                #     self.logger.info(f"ERROR:::BrokerMW::event_loop {self.matchings_pub}")
                owner = self.get_best_publisher(topic, 1)
                if owner != pub_id:
                    self.logger.info(
                        f"Ignoring message from {pub_id} on '{topic}' (owner is {owner})"
                    )
                    continue


                # forward message to subscriber
                self.backend.send_string(message)
                self.logger.info("BrokerMW::event_loop - Forwarded message to subscribers")
