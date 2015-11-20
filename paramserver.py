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
from machinetalk.protobuf.param_pb2 import *

if sys.version_info >= (3, 0):
    import configparser
else:
    import ConfigParser as configparser


class ParamServer():
    def __init__(self, context, host='', svcUuid=None, debug=False,
                 pingInterval=2.0, loopback=False):
        self.debug = debug
        self.threads = []
        self.timerLock = threading.Lock()
        self.paramTimer = None
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

        self.start_param_heartbeat()
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
            if key.startswith(s):
                self.incremental_update(str(key), s)
                return

    def elektra_dbus_key_added_cb(self, key):
        if self.debug:
            print('key added %s' % key)

        for s in self.subscriptions:
            if key.startswith(s):
                self.incremental_update(str(key), s)
                return

    def elektra_dbus_key_deleted_cb(self, key):
        if self.debug:
            print('key deleted %s' % key)
        self.incremental_update(key, delete=True)

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

    # type guessing helpers from http://stackoverflow.com/questions/7019283/automatically-type-cast-parameters-in-python
    def boolify(self, s):
        s = s.lower()
        if s == 'true':
            return True
        if s == 'false':
            return False
        raise ValueError('Not Boolean Value!')

    def estimateType(self, var):
        '''guesses the str representation of the variables type'''
        casters = [[self.boolify, PARAM_BOOLEAN],
                   [int, PARAM_INTEGER],
                   [float, PARAM_DOUBLE],
                   [str, PARAM_STRING]]
        for caster in casters:
            try:
                return (caster[0](var), caster[1])
            except ValueError:
                pass
        return (var, PARAM_STRING)

    def convert_key(self, paramKey, elektraKey):
        paramKey.name = elektraKey.name
        # get meta check/type before trying to convert
        if elektraKey.isBinary():
            paramKey.type = PARAM_BINARY
            paramKey.parambinary = bytes(elektraKey.binary)
        else:
            (value, valueType) = self.estimateType(str(elektraKey.value))
            paramKey.type = valueType
            if valueType == PARAM_STRING:
                paramKey.paramstring = str(value)
            elif valueType == PARAM_BOOLEAN:
                paramKey.parambool = bool(value)
            elif valueType == PARAM_INTEGER:
                paramKey.paramint = int(value)
            elif valueType == PARAM_DOUBLE:
                paramKey.paramdouble = float(value)
            else:
                print('Warning: type guessing failed')

    def full_update(self, basekey):
        with kdb.KDB() as db:
            ks = kdb.KeySet()
            db.get(ks, basekey)
            for k in ks:
                key = self.tx.key.add()
                self.convert_key(key, k)
        self.add_pparams()
        self.send_param_msg(basekey, MT_PARAM_FULL_UPDATE)

    def incremental_update(self, key, basekey='', delete=False):
        if not delete:
            with kdb.KDB() as db:
                ks = kdb.KeySet()
                db.get(ks, basekey)
                k = ks[key]
                paramKey = self.tx.key.add()
                self.convert_key(paramKey, k)
        else:
            paramKey = self.tx.key.add()
            paramKey.name = key
            paramKey.deleted = True
        self.send_param_msg(basekey, MT_PARAM_INCREMENTAL_UPDATE)

    def ping_param(self):
        for s in self.subscriptions:
            self.send_param_msg(s, MT_PING)

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
        self.tx.pparams.MergeFrom(parameters)

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
            print(str(self.rx))

        if self.rx.type == MT_PING:
            self.send_command_msg(identity, MT_PING_ACKNOWLEDGE)

        elif self.rx.type == MT_PARAM_SET:
            if len(self.rx.key) == 0:
                self.send_command_wrong_params(identity)
                return

            for key in self.rx.key:
                if not self.set_key(key):
                    self.send_command_wrong_params(identity)
                    return
            # self.send_ack() TODO

        elif self.rx.type == MT_PARAM_DELETE:
            pass

        else:
            self.txCommand.note.append("unknown command")
            self.send_command_msg(identity, MT_ERROR)

    def set_key(self, key):
        if not key.HasField('name'):
            return False
        name = str(key.name)
        value = None
        if key.type == PARAM_STRING:
            if not key.HasField('paramstring'):
                return False
            value = str(key.paramstring)
        elif key.type == PARAM_BINARY:
            if not key.HasField('parambinary'):
                return False
            value = str(key.parambinary)
        elif key.type == PARAM_INTEGER:
            if not key.HasField('paramint'):
                return False
            value = str(key.paramint)
        elif key.type == PARAM_DOUBLE:
            if not key.HasField('paramdouble'):
                return False
            value = str(key.paramdouble)
        elif key.type == PARAM_DOUBLE:
            if not key.HasField('paramlist'):
                return False
            return False  # TODO: implement
        for s in self.subscriptions:
            if name.startswith(s):
                with kdb.KDB() as db:
                    ks = kdb.KeySet()
                    db.get(ks, s)
                    ks[name].string = value
                    db.set(ks, s)
                return True
        return False

    def param_timer_tick(self):
        self.ping_param()
        self.paramTimer = threading.Timer(self.pingInterval,
                                             self.param_timer_tick)
        self.paramTimer.start()  # rearm timer

    def start_param_heartbeat(self):
        self.timerLock.acquire()
        if self.paramTimer:
            self.paramTimer.cancel()

        if self.pingInterval > 0:
            self.paramTimer = threading.Timer(self.pingInterval,
                                               self.param_timer_tick)
            self.paramTimer.start()
        self.timerLock.release()

    def stop_param_heartbeat(self):
        self.timerLock.acquire()
        if self.paramTimer:
            self.paramTimer.cancel()
            self.paramTimer = None
        self.timerLock.release()

    def refresh_param_heartbeat(self):
        self.timerLock.acquire()
        if self.paramTimer:
            self.paramTimer.cancel()
            self.paramTimer = threading.Timer(self.pingInterval,
                                               self.param_timer_tick)
            self.paramTimer.start()
        self.timerLock.release()

    def stop(self):
        self.shutdown.set()
        self.stop_param_heartbeat()


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
