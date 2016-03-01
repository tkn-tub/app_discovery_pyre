from pyre import Pyre 
from pyre import zhelper 
import zmq 
import uuid
import logging
import sys
import json
import time

import wishful_upis as upis
import wishful_agent

__author__ = "Piotr Gawlowicz"
__copyright__ = "Copyright (c) 2015, Technische Universitat Berlin"
__version__ = "0.1.0"
__email__ = "{gawlowicz}@tkn.tu-berlin.de"


@wishful_agent.build_module
class PyreDiscoveryAgentModule(wishful_agent.AgentModule):
    def __init__(self, groupName="wishful"):
        super(PyreDiscoveryAgentModule, self).__init__()
        self.log = logging.getLogger('pyre_discovery_module.main')
        self.running = False
        self.controller_dl = None
        self.controller_ul = None
        self.groupName = groupName
        self.ctx = zmq.Context()


    @wishful_agent.loop()
    @wishful_agent.on_start()
    @wishful_agent.on_disconnected()
    def start_discovery_announcements(self):
        self.log.debug("Start discovery procedure".format())
        self.running = True
        self.discovery_pipe = zhelper.zthread_fork(self.ctx, self.discovery_task)

        while self.running:
            time.sleep(10)     


    @wishful_agent.on_exit()
    @wishful_agent.on_connected()
    def stop_discovery_announcements(self):
        self.log.debug("Stop discovery announcements".format())
        self.running = False
        self.discovery_pipe.send("$$STOP".encode('utf_8'))


    @wishful_agent.discover_controller()
    def get_controller(self):
        self.log.debug("Get Controller addresses: DL:{}, UL:{}".format(self.controller_dl, self.controller_ul))
        return [self.controller_dl, self.controller_ul]


    def discovery_task(self, ctx, pipe):
        n = Pyre(self.groupName)
        n.set_header("DISCOVERY_Header1","DISCOVERY_HEADER")
        n.join(self.groupName)
        n.start()

        poller = zmq.Poller()
        poller.register(pipe, zmq.POLLIN)
        poller.register(n.inbox, zmq.POLLIN)

        while(True):
            items = dict(poller.poll())

            if pipe in items and items[pipe] == zmq.POLLIN:
                message = pipe.recv()
                # message to quit
                if message.decode('utf-8') == "$$STOP":
                    break

            if n.inbox in items and items[n.inbox] == zmq.POLLIN:
                cmds = n.recv()
                self.log.debug("NODE_MSG CONT:{}".format(cmds))

                msg_type = cmds.pop(0)
                peer_uuid_bytes = cmds.pop(0)
                peer_uuid = uuid.UUID(bytes=peer_uuid_bytes)

                self.log.debug("NODE_MSG TYPE: {}".format(msg_type))
                self.log.debug("NODE_MSG PEER: {}".format(peer_uuid))

                if msg_type.decode('utf-8') == "SHOUT":
                    group_name = cmds.pop(0)
                    self.log.debug("NODE_MSG GROUP: {}".format(group_name))

                    group_name_2 = cmds.pop(0)
                    self.log.debug("NODE_MSG GROUP_2: {}".format(group_name_2))

                    discoveryMsg = cmds.pop(0)
                    self.log.debug("Discovery Msg : {}".format(discoveryMsg))

                    controller = json.loads(discoveryMsg)
                    self.controller_dl = str(controller["downlink"])
                    self.controller_ul = str(controller["uplink"])
                    self.log.error("DL: {}, UL: {}".format(self.controller_dl, self.controller_ul))

        n.stop()


if __name__ == '__main__':
    # Create a StreamHandler for debugging
    logger = logging.getLogger("pyre")
    logging.basicConfig(level=logging.ERROR)

    pyreModule = PyreDiscoveryAgentModule()

    try:
        pyreModule.start_discovery_announcements()
    except (KeyboardInterrupt, SystemExit):
        print "Module exits"
    finally:
        pyreModule.stop_discovery_announcements()