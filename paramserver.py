#!/usr/bin/env python

import time
import sys
import kdb
import dbus
import gobject
gobject.threads_init()
from dbus.mainloop.glib import DBusGMainLoop

import threading


class ParamServer():
    def __init__(self, debug=False):
        self.debug = debug
        try:
            DBusGMainLoop(set_as_default=True)
            self.bus = dbus.SystemBus()
            self.bus.add_signal_receiver(self.elektra_dbus_cb,
                                         signal_name="KeyChanged",
                                         dbus_interface="org.libelektra",
                                         path="/org/libelektra/configuration")
        except dbus.DBusException, e:
            print(str(e))
            sys.exit(1)

    def elektra_dbus_cb(self, key):
        print('key changed %s' % key)


#with kdb.KDB() as db:
#    pass

def main():
    #gobject.threads_init()  # important: initialize threads if gobject main loop is used
    param = ParamServer()
    loop = gobject.MainLoop()
    try:
        loop.run()
    except KeyboardInterrupt:
        loop.quit()

    print("stopping threads")
    #basic.stop()

    # wait for all threads to terminate
    while threading.active_count() > 1:
        time.sleep(0.1)

    print("threads stopped")
    sys.exit(0)

if __name__ == "__main__":
    main()
