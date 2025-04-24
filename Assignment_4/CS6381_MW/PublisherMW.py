import zmq
import logging
import time
import threading
from CS6381_MW import discovery_pb2
from CS6381_MW.zkclient import ZK_Driver

class PublisherMW():
    def __init__(self, logger, group, history_sizes):
        self.logger = logger                # Logger instance
        self.req = None                     # REQ socket for Discovery service
        self.pub = None                     # PUB socket for dissemination
        self.hist_rep = None                # socket to request history from publisher in Direct mode
        self.poller = None                  # ZMQ Poller for event loop
        self.addr = None                    # Local advertised IP address
        self.port = None                    # Port number for publishing
        self.upcall_obj = None              # Pointer to the application-level object
        self.handle_events = True           # Controls the event loop
        self.dissemination = None           # Dissemination strategy ("Direct" or "ViaBroker")
        self.ownership = None               # Assigned ownership strength
        self.group = group                  # Assigned Load balancing group number
        self.history_size = history_sizes   # configured sizes of history for each topic
        self.history = {}                   # Buffer of past publications of size self.history_size
        
        # Attribute to hold current discovery primary endpoint (tcp://IP:port)
        self.discovery_primary_endpoint = None
        # Attribute to hold current broker endpoint for dissemination (tcp://IP:port)
        self.current_broker_endpoint = None

        self.zk_obj = ZK_Driver()
        self.zk_obj.init_driver() 
        self.zk_obj.start_session()

    def history_thread(self):
        self.logger.info("PublisherMW::history_thread")
        history_port = self.port + 1

        self.hist_rep = self.context.socket(zmq.REP)
        self.hist_rep.bind(f"tcp://*:{history_port}")
        self.logger.info(f"PublisherMW::configure - history REP bound to {history_port}")
        #self.poller.register(self.hist_rep, zmq.POLLIN) # don't register with poller in threading approach

        # launch a daemon thread to serve history requests
        t = threading.Thread(target=self._serve_history, daemon=True)
        t.start()

    def _serve_history(self):
        '''
        Dedicated loop to reply to history pulls.
        Blocks on recv_json(), so it won't interfere with the main event_loop.
        '''
        while self.handle_events:
            try:
                req = self.hist_rep.recv_json()    # { "topic": T, "num": N }
                topic = req.get("topic")
                num = int(req.get("num", 0))
                hist = self.history.get(topic, [])[-num:]
                # reply with exactly what the subscriber wants
                self.hist_rep.send_json({ "history": hist })
                self.logger.info(f"PublisherMW::_serve_history – replied {len(hist)} messages for '{topic}'")
            
            except Exception as e:
                self.logger.info("ERROR:::PublisherMW::_serve_history error: " + str(e))
                # if something goes wrong, at least unblock the REQ
                try:
                    self.hist_rep.send_json({ "history": [] })
                except:
                    pass

    def configure(self, args, dissemination=None):
        '''Initialize the PublisherMW.'''
        try:
            self.logger.info("PublisherMW::configure")
            self.port = args.port
            self.addr = args.addr
            self.dissemination = dissemination

            # Get the ZMQ context and poller.
            self.logger.info("PublisherMW::configure - obtaining ZMQ context")
            self.context = zmq.Context()
            self.poller = zmq.Poller()

            # Create and register REQ Socket for discovery
            self.logger.info("PublisherMW::configure - creating REQ and PUB sockets")
            self.req = self.context.socket(zmq.REQ)
            self.poller.register(self.req, zmq.POLLIN)
            # Create and register XPUB Socket
            self.pub = self.context.socket(zmq.XPUB)
            self.pub.setsockopt(zmq.XPUB_VERBOSE, 1) # tell XPUB to send you every subscribe message
            # register pub here (b4 binding) or below (after binding)?

            if self.dissemination == "Direct":
                # Bind and register pub socket
                bind_string = "tcp://*:" + str(self.port)
                self.pub.bind(bind_string)
                self.poller.register(self.pub, zmq.POLLIN)
                self.logger.info("PublisherMW::configure - Direct mode: bound XPUB socket to " + bind_string)
                
                # Bind and register history socket
                self.history_thread()

            self.new_broker_connect_str = None

            # Retrieve the primary discovery pointer from ZooKeeper.
            try:
                primary_data, _ = self.zk_obj.zk.get(f"/root/discovery/group{self.group}/primary")
                primary_str = primary_data.decode('utf-8')
                new_connect_str = f"tcp://{primary_str}"
                self.discovery_primary_endpoint = new_connect_str
                self.logger.info("PublisherMW::configure - connecting to primary discovery at " + new_connect_str)
                self.req.connect(new_connect_str)
            except Exception as e:
                self.logger.info("ERROR:::PublisherMW::configure - error retrieving primary discovery pointer: " + str(e))

            # Set a watch on the primary discovery pointer so that if it changes, we reconnect.
            @self.zk_obj.zk.DataWatch(f"/root/discovery/group{self.group}/primary")
            def watch_primary(data, stat, event):
                if data:
                    new_primary = data.decode('utf-8')
                    new_connect_str = f"tcp://{new_primary}"
                    if new_connect_str != self.discovery_primary_endpoint:
                        self.logger.info("PublisherMW::watch_primary - primary updated to " + new_connect_str)
                        if self.discovery_primary_endpoint:
                            try:
                                self.req.disconnect(self.discovery_primary_endpoint)
                            except Exception as ex:
                                self.logger.info("ERROR:::PublisherMW::watch_primary - error disconnecting from old endpoint: " + str(ex))
                        self.req.connect(new_connect_str)
                        self.discovery_primary_endpoint = new_connect_str

            # Set a watch on the broker primary so that when the broker leader changes, we update our connection.
            if self.dissemination == "ViaBroker":
                self.logger.info("PublisherMW::configure - ViaBroker mode: publisher will connect to broker for dissemination.")
                @self.zk_obj.zk.DataWatch(f"/root/broker/group{str(self.group)}/primary")
                def watch_broker_primary(data, stat, event):
                    if data:
                        new_broker = data.decode('utf-8')
                        new_broker_endpoint = f"tcp://{new_broker.split(':')[0]}:{new_broker.split(':')[1]}"
                        self.logger.info("PublisherMW::watch_broker_primary - Broker Primary updated to " + new_broker_endpoint)
                        # Update our PUB socket connection:
                        if self.current_broker_endpoint is None:
                            self.logger.info("PublisherMW::watch_broker_primary - No current broker endpoint; connecting to " + new_broker_endpoint)
                            self.pub.connect(new_broker_endpoint)
                            self.current_broker_endpoint = new_broker_endpoint
                        elif new_broker_endpoint != self.current_broker_endpoint:
                            self.logger.info("PublisherMW::watch_broker_primary - Broker endpoint changed from " +
                                            self.current_broker_endpoint + " to " + new_broker_endpoint)
                            try:
                                self.pub.disconnect(self.current_broker_endpoint)
                            except Exception as ex:
                                self.logger.info("ERROR:::PublisherMW::watch_broker_primary - error disconnecting from old broker endpoint: " + str(ex))
                            self.pub.connect(new_broker_endpoint)
                            self.current_broker_endpoint = new_broker_endpoint                

            # Create publisher znode in Zookeeper
            self.logger.info(f"Zookeeper: Creating znode for Publisher {args.name} with addr {self.addr}, port {self.port}")
            created_path = self.zk_obj.create_znode("publisher", args.name, self.addr, self.port)
            # Get the publishers ownership strength from the name automatically generated by Zookeeper
            self.ownership = int(created_path.rsplit("/", 1)[1].split("-", 1)[1])
            self.logger.info(f"Zookeeper: Created Publisher {args.name} with ownership {self.ownership}")

            self.logger.info("PublisherMW::configure completed")

        except Exception as e:
            raise e

    # (The remaining methods (event_loop, handle_reply, register, lookup_broker, disseminate, etc.) remain unchanged.)
    def event_loop(self, timeout=None):
        try:
            self.logger.info("PublisherMW::event_loop - run the event loop")
            while self.handle_events:
                events = dict(self.poller.poll(timeout=timeout))
                self.logger.info("\n\n\n\n\n\npoll events = %r", events)
                    
                if self.req in events:
                    timeout = self.handle_reply()
                    continue
                # elif self.hist_rep in events:
                #     timeout = self.handle_history_request()
                #     continue
                elif self.pub in events:
                    self.logger.info("PublisherMW::event_loop - XPUB has sub/unsub event !!!!!!!!!!!!!!!!!!!!!!")
                    # XPUB has a subscription/unsubscription event
                    frame = self.pub.recv()         # raw bytes: [0x01|0x00] + topic
                    subscribe = (frame[0] == 1)     # 1 = subscribe, 0 = unsubscribe
                    topic = frame[1:].decode()
                    if subscribe:
                        # replay buffered messages for that topic
                        self.send_history(topic)
                    continue
                
                timeout = self.upcall_obj.invoke_operation()
                
            self.logger.info("PublisherMW::event_loop - out of the event loop")
        except Exception as e:
            raise e

    def handle_history_request(self):
        try:
            req = self.hist_rep.recv_json()           # expects {"topic": T, "num": N}
            topic = req["topic"]
            N = int(req["num"])
            hist = self.history.get(topic, [])[-N:]
            # send back a JSON object containing the list of strings
            self.hist_rep.send_json({ "history": hist })
            self.logger.info(f"PublisherMW::send_history REP → {len(hist)} msgs for '{topic}'")
        except Exception as e:
            self.logger.info("ERROR:::PublisherMW::handle_history_request " + str(e))
            # optional: send an empty or error reply
            self.hist_rep.send_json({ "history": [] })

    def handle_reply(self):
        try:
            self.logger.info("PublisherMW::handle_reply")
            bytesRcvd = self.req.recv()
            disc_resp = discovery_pb2.DiscoveryResp()
            disc_resp.ParseFromString(bytesRcvd)

            if disc_resp.msg_type == discovery_pb2.TYPE_REGISTER:
                timeout = self.upcall_obj.register_response(disc_resp.register_resp)
            elif disc_resp.msg_type == discovery_pb2.TYPE_LOOKUP_BROKER:
                self.logger.info("PublisherMW::handle_reply - Broker lookup response received.")
                broker_info = disc_resp.lookup_resp_broker.broker
                new_broker_endpoint = f"tcp://{broker_info.addr}:{broker_info.front_port}"
                # If we have no current connection, connect to the new broker.
                if self.current_broker_endpoint is None:
                    self.logger.info("PublisherMW::handle_reply - No current broker endpoint; connecting to " + new_broker_endpoint)
                    self.pub.connect(new_broker_endpoint)
                    self.current_broker_endpoint = new_broker_endpoint
                elif new_broker_endpoint != self.current_broker_endpoint:
                    self.logger.info("PublisherMW::handle_reply - Broker endpoint changed from " +
                                     self.current_broker_endpoint + " to " + new_broker_endpoint)
                    try:
                        self.pub.disconnect(self.current_broker_endpoint)
                    except Exception as ex:
                        self.logger.info("ERROR:::PublisherMW::handle_reply - error disconnecting from old broker: " + str(ex))
                    self.pub.connect(new_broker_endpoint)
                    self.current_broker_endpoint = new_broker_endpoint
                else:
                    self.logger.info("PublisherMW::handle_reply - Broker endpoint unchanged.")
                if hasattr(self.upcall_obj, "broker_lookup_response"):
                    timeout = self.upcall_obj.broker_lookup_response(broker_info)
                else:
                    timeout = 0
            else:
                raise ValueError("Unrecognized response message")
            return timeout
        except Exception as e:
            raise e

    def register(self, name, topiclist):
        try:
            self.logger.info("PublisherMW::register")
            reg_info = discovery_pb2.RegistrantInfo()
            reg_info.id = name
            reg_info.addr = self.addr
            reg_info.port = self.port
            reg_info.ownership = self.ownership
            # reg_info.history_size.extend(self.history_size.values())
            reg_info.history_size[:] = [self.history_size[t] for t in topiclist]


            register_req = discovery_pb2.RegisterReq()
            register_req.role = discovery_pb2.ROLE_PUBLISHER
            register_req.info.CopyFrom(reg_info)
            register_req.topiclist[:] = topiclist

            disc_req = discovery_pb2.DiscoveryReq()
            disc_req.msg_type = discovery_pb2.TYPE_REGISTER
            disc_req.register_req.CopyFrom(register_req)
            buf2send = disc_req.SerializeToString()
            
            self.logger.info("PublisherMW::register - sending: {}".format(buf2send))
            self.req.send(buf2send)
            self.logger.info("PublisherMW::register - registration message sent; awaiting reply")
        except Exception as e:
            raise e

    def lookup_broker(self):
        try:
            self.logger.info("PublisherMW::lookup_broker")
            req_msg = discovery_pb2.DiscoveryReq()
            req_msg.msg_type = discovery_pb2.TYPE_LOOKUP_BROKER
            buf2send = req_msg.SerializeToString()
            self.logger.info("PublisherMW::lookup_broker - sending lookup broker request: {}".format(buf2send))
            self.req.send(buf2send)
        except Exception as e:
            raise e

    def disseminate(self, id, topic, data):
        try:
            timestamp = time.time()
            #self.logger.info("PublisherMW::disseminate")

            send_str = f"{topic}:{timestamp}:{data}"
            # Store publication in publisher's history
            if self.history == {}:
                self.history[topic] = []
                self.history[topic].append(send_str)
            elif len(self.history[topic]) < (self.history_size[topic]):
                self.history[topic].append(send_str)
            else:
                self.history[topic].pop(0)             # remove earliest publication to maintain history size
                self.history[topic].append(send_str)
            # Send publication to subs
            self.logger.info("PublisherMW::disseminate - sending: {}".format(send_str))
            self.pub.send(bytes(send_str, "utf-8"))
            #self.logger.info("PublisherMW::disseminate complete")
        except Exception as e:
            raise e

    def send_history(self, topic):
        # try:
        #     self.logger.info("PublisherMW::send_history")
        #     #history_msg = f"{topic}"
        #     #final_send_str = "*****************HISTORY********************\n"
        #     #send_str = '\n'.join(self.history[topic])
        #     #history_msg = history_msg + final_send_str + send_str

        #     self.pub.send(bytes(history_msg, "utf-8"))
        #     self.logger.info("PublisherMW::send_history complete")
        # except Exception as e:
        #     raise e
        try:
            self.logger.info("PublisherMW::send_history")
            for msg in self.history[topic]:
                self.pub.send_string(msg)
                self.logger.info("PublisherMW::send_history complete")
        except Exception as e:
            self.logger.info("ERROR:::send_history: " + str(e))


    def set_upcall_handle(self, upcall_obj):
        self.logger.info("PublisherMW::set_upcall_handle")
        self.upcall_obj = upcall_obj

    def disable_event_loop(self):
        self.handle_events = False
