#!/usr/bin/env python

import time
import sys
import os
import argparse
import kdb
import zmq
import dbus
import gobject
gobject.threads_init()  # important: initialize threads if gobject main loop is used
from dbus.mainloop.glib import DBusGMainLoop
import threading

from machinekit import service
from machinekit import config
from machinetalk.protobuf.message_pb2 import Container
from machinetalk.protobuf.types_pb2 import *
from machinetalk.protobuf.object_pb2 import ProtocolParameters

if sys.version_info >= (3, 0):
    import configparser
else:
    import ConfigParser as configparser


class ParamServer():
    def __init__(self, context, host='', svcUuid=None, debug=False,
                 pingInterval=2.0, loopback=False):
        self.debug = debug
        self.threads = []
        self.shutdown = threading.Event()
        self.running = False

        self.host = host
        self.loopback = loopback
        self.pingInterval = pingInterval

        self.rx = Container()
        self.tx = Container()
        self.txCommand = Container()
        self.subscriptions = set()
        self.fullUpdates = set()

        self.context = context
        self.baseUri = "tcp://"
        if self.loopback:
            self.baseUri += '127.0.0.1'
        else:
            self.baseUri += '*'
        self.paramSocket = context.socket(zmq.XPUB)
        self.paramSocket.setsockopt(zmq.XPUB_VERBOSE, 1)
        self.paramPort = self.paramSocket.bind_to_random_port(self.baseUri)
        self.paramDsname = self.paramSocket.get_string(zmq.LAST_ENDPOINT, encoding='utf-8')
        self.paramDsname = self.paramDsname.replace('0.0.0.0', self.host)
        self.commandSocket = context.socket(zmq.ROUTER)
        self.commandPort = self.commandSocket.bind_to_random_port(self.baseUri)
        self.commandDsname = self.commandSocket.get_string(zmq.LAST_ENDPOINT, encoding='utf-8')
        self.commandDsname = self.commandDsname.replace('0.0.0.0', self.host)

        self.paramService = service.Service(type='param',
                                   svcUuid=svcUuid,
                                   dsn=self.paramDsname,
                                   port=self.paramPort,
                                   host=self.host,
                                   loopback=self.loopback,
                                   debug=self.debug)
        self.commandService = service.Service(type='paramcmd',
                                   svcUuid=svcUuid,
                                   dsn=self.commandDsname,
                                   port=self.commandPort,
                                   host=self.host,
                                   loopback=self.loopback,
                                   debug=self.debug)

        self.connect_dbus()  # before publish to set mainloop
        self.publish()

        threading.Thread(target=self.process_sockets).start()
        self.running = True

    def connect_dbus(self):
        try:
            DBusGMainLoop(set_as_default=True)
            bus = dbus.SystemBus()  # may use session bus for user db
            bus.add_signal_receiver(self.elektra_dbus_key_changed_cb,
                                    signal_name="KeyChanged",
                                    dbus_interface="org.libelektra",
                                    path="/org/libelektra/configuration")
            bus.add_signal_receiver(self.elektra_dbus_key_added_cb,
                                    signal_name="KeyAdded",
                                    dbus_interface="org.libelektra",
                                    path="/org/libelektra/configuration")
            bus.add_signal_receiver(self.elektra_dbus_key_deleted_cb,
                                    signal_name="KeyDeleted",
                                    dbus_interface="org.libelektra",
                                    path="/org/libelektra/configuration")
        except dbus.DBusException, e:
            print(str(e))
            sys.exit(1)

    def elektra_dbus_key_changed_cb(self, key):
        if self.debug:
            print('key changed %s' % key)

        for s in self.subscriptions:
            if s in key:
                self.incremental_update(s, str(key))
                return

    def elektra_dbus_key_added_cb(self, key):
        if self.debug:
            print('key added %s' % key)

        for s in self.subscriptions:
            if s in key:
                self.incremental_update(s, str(key))
                return

    def elektra_dbus_key_deleted_cb(self, key):
        print('key deleted %s' % key)

    def process_sockets(self):
        poll = zmq.Poller()
        poll.register(self.paramSocket, zmq.POLLIN)
        poll.register(self.commandSocket, zmq.POLLIN)

        while not self.shutdown.is_set():
            s = dict(poll.poll(1000))
            if self.paramSocket in s and s[self.paramSocket] == zmq.POLLIN:
                self.process_param(self.paramSocket)
            if self.commandSocket in s and s[self.commandSocket] == zmq.POLLIN:
                self.process_command(self.commandSocket)

    def publish(self):
        # Zeroconf
        try:
            self.paramService.publish()
            self.commandService.publish()
        except Exception as e:
            print (('cannot register DNS service' + str(e)))
            sys.exit(1)

    def unpublish(self):
        self.paramService.unpublish()
        self.commandService.unpublish()

    def full_update(self, basekey):
        with kdb.KDB() as db:
            ks = kdb.KeySet()
            db.get(ks, basekey)
            for k in ks:
                key = self.tx.keys.add()
                key.name = k.name
                # TODO guess value type
        self.send_param_msg(basekey, MT_PARAM_FULL_UPDATE)

    def incremental_update(self, basekey, key):
        with kdb.KDB() as db:
            ks = kdb.KeySet()
            db.get(ks, basekey)
            k = ks[key]
            paramKey = self.tx.keys.add()
            paramKey.name = k.name
            # TODO guess value type
            print(basekey)
        self.send_param_msg(basekey, MT_PARAM_INCREMENTAL_UPDATE)

    def process_param(self, s):
        try:
            rc = s.recv()
            subscription = rc[1:]
            status = (rc[0] == "\x01")

            if status:
                self.subscriptions.add(subscription)
                self.full_update(subscription)
            else:
                self.subscriptions.remove(subscription)

            if self.debug:
                print(("process param called " + subscription + ' ' + str(status)))

        except zmq.ZMQError as e:
            printError('ZMQ error: ' + str(e))

    def add_pparams(self):
        parameters = ProtocolParameters()
        parameters.keepalive_timer = int(self.pingInterval * 1000.0)
        self.txCommand.pparams.MergeFrom(parameters)

    def send_param_msg(self, topic, msgType):
        if self.debug:
            print('sending param message')
        self.tx.type = msgType
        txBuffer = self.tx.SerializeToString()
        self.tx.Clear()
        self.paramSocket.send_multipart([topic, txBuffer], zmq.NOBLOCK)

    def send_command_msg(self, identity, msgType):
        self.txCommand.type = msgType
        txBuffer = self.txCommand.SerializeToString()
        self.commandSocket.send_multipart([identity, txBuffer], zmq.NOBLOCK)
        self.txCommand.Clear()

    def send_command_wrong_params(self, identity):
        self.txCommand.note.append('wrong parameters')
        self.send_command_msg(identity, MT_ERROR)

    def process_command(self, s):
        (identity, message) = s.recv_multipart()
        self.rx.ParseFromString(message)

        if self.debug:
            print("process command called, id: %s" % identity)

        if self.rx.type == MT_PING:
            self.send_command_msg(identity, MT_PING_ACKNOWLEDGE)

        else:
            self.txCommand.note.append("unknown command")
            self.send_command_msg(identity, MT_ERROR)

    def stop(self):
        self.shutdown.set()


def main():
    parser = argparse.ArgumentParser(description='paramserver is a Machinetalk based parameter server for Machinekit')
    parser.add_argument('-d', '--debug', help='Enable debug mode', action='store_true')

    args = parser.parse_args()
    debug = args.debug

    mkconfig = config.Config()
    mkini = os.getenv("MACHINEKIT_INI")
    if mkini is None:
        mkini = mkconfig.MACHINEKIT_INI
    if not os.path.isfile(mkini):
        sys.stderr.write("MACHINEKIT_INI " + mkini + " does not exist\n")
        sys.exit(1)

    mki = configparser.ConfigParser()
    mki.read(mkini)
    uuid = mki.get("MACHINEKIT", "MKUUID")
    remote = mki.getint("MACHINEKIT", "REMOTE")

    if remote == 0:
        print("Remote communication is deactivated, configserver will use the loopback interfaces")
        print(("set REMOTE in " + mkini + " to 1 to enable remote communication"))

    if debug:
        print(("announcing mklauncher"))

    context = zmq.Context()
    context.linger = 0

    hostname = '%(fqdn)s'  # replaced by service announcement
    param = ParamServer(context,
                        svcUuid=uuid,
                        host=hostname,
                        loopback=(not remote),
                        debug=debug)

    loop = gobject.MainLoop()
    try:
        loop.run()
    except KeyboardInterrupt:
        loop.quit()

    print("stopping threads")
    param.stop()

    # wait for all threads to terminate
    while threading.active_count() > 1:
        time.sleep(0.1)

    print("threads stopped")
    sys.exit(0)

if __name__ == "__main__":
    main()
