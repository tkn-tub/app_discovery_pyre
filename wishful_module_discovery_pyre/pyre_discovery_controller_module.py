from pyre import Pyre 
from pyre import zhelper 
import zmq 
import uuid
import logging
import sys
import json
import time

import wishful_upis as upis
import wishful_controller

__author__ = "Piotr Gawlowicz"
__copyright__ = "Copyright (c) 2015, Technische Universitat Berlin"
__version__ = "0.1.0"
__email__ = "{gawlowicz}@tkn.tu-berlin.de"


@wishful_controller.build_module
class PyreDiscoveryControllerModule(wishful_controller.ControllerModule):
    def __init__(self, downlink, uplink, groupName="wishful"):
        super(PyreDiscoveryControllerModule, self).__init__()
        self.log = logging.getLogger('pyre_discovery_module.main')
        self.running = False
        self.controller_dl = downlink
        self.controller_ul = uplink
        self.groupName = groupName
        self.ctx = zmq.Context()


    @wishful_controller.loop()
    @wishful_controller.on_start()
    def start_discovery_announcements(self):
        self.log.debug("Start discovery announcements".format())
        self.running = True
        self.discovery_pipe = zhelper.zthread_fork(self.ctx, self.discovery_task)
          
        while self.running:
            self.log.debug("Discovery Announcements, Downlink={}, Uplink={}".format(self.controller_dl, self.controller_ul))
            
            msg = json.dumps({'downlink': self.controller_dl,'uplink': self.controller_ul})
            self.discovery_pipe.send(msg.encode('utf_8'))
            time.sleep(5)


    @wishful_controller.on_exit()
    def stop_discovery_announcements(self):
        self.log.debug("Stop discovery announcements".format())
        if self.running:
            self.running = False
            self.discovery_pipe.send("$$STOP".encode('utf_8'))


    def discovery_task(self, ctx, pipe):
        n = Pyre(self.groupName)
        n.set_header("DISCOVERY_Header1","DISCOVERY_HEADER")
        n.join(self.groupName)
        n.start()

        poller = zmq.Poller()
        poller.register(pipe, zmq.POLLIN)

        while(True):
            items = dict(poller.poll())

            if pipe in items and items[pipe] == zmq.POLLIN:
                message = pipe.recv()
                # message to quit
                if message.decode('utf-8') == "$$STOP":
                    break

                n.shout(self.groupName, message)

        n.stop()


if __name__ == '__main__':
    # Create a StreamHandler for debugging
    logger = logging.getLogger("pyre")
    logging.basicConfig(level=logging.ERROR)

    pyreModule = PyreDiscoveryControllerModule("tcp://127.0.0.1:8989","tcp://127.0.0.1:8990")

    try:
        pyreModule.start_discovery_announcements()
    except (KeyboardInterrupt, SystemExit):
        print "Module exits"
    finally:
        pyreModule.stop_discovery_announcements()