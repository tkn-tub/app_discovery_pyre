from pyre import Pyre
from pyre import zhelper
import zmq
import logging
import json
import time

import wishful_framework

__author__ = "Piotr Gawlowicz"
__copyright__ = "Copyright (c) 2015, Technische Universitat Berlin"
__version__ = "0.1.0"
__email__ = "{gawlowicz}@tkn.tu-berlin.de"


@wishful_framework.build_module
class PyreDiscoveryControllerModule(wishful_framework.WishfulModule):
    def __init__(self, downlink,
                 uplink, iface, groupName="wishful"):
        super(PyreDiscoveryControllerModule, self).__init__()
        self.log = logging.getLogger('pyre_discovery_module.main')

        pyreLogger = logging.getLogger('pyre')
        pyreLogger.setLevel(logging.CRITICAL)

        self.running = False
        self.iface = iface
        self.controller_dl = downlink
        self.controller_ul = uplink
        self.groupName = groupName
        self.ctx = zmq.Context()

    @wishful_framework.run_in_thread()
    @wishful_framework.on_start()
    def start_discovery_announcements(self):
        self.log.debug("Start discovery announcements".format())
        self.running = True
        self.discovery_pipe = zhelper.zthread_fork(
            self.ctx, self.discovery_task)

        while self.running:
            self.log.debug("Discovery Announcements:"
                           " Downlink={}, Uplink={}"
                           .format(self.controller_dl, self.controller_ul))

            msg = json.dumps({'downlink': self.controller_dl,
                              'uplink': self.controller_ul})
            self.discovery_pipe.send(msg.encode('utf_8'))
            time.sleep(2)

    @wishful_framework.on_exit()
    def stop_discovery_announcements(self):
        self.log.debug("Stop discovery announcements".format())
        if self.running:
            self.running = False
            self.discovery_pipe.send("$$STOP".encode('utf_8'))

    def discovery_task(self, ctx, pipe):
        self.log.debug("Pyre on iface : {}".format(self.iface))
        n = Pyre(self.groupName, sel_iface=self.iface)
        n.set_header("DISCOVERY_Header1", "DISCOVERY_HEADER")
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
