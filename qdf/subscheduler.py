__author__ = 'immesys'

import configobj
import sys
from pymongo import MongoClient
import os
import quasar
import uuid
import qdf
from twisted.internet import defer, protocol, reactor

EXIT_BADCONF = 2
EXIT_SKIP = 3
EXIT_UNK  = 4
EXIT_CODE = None

def setexit(code):
    global EXIT_CODE
    EXIT_CODE = code

_client = MongoClient(os.environ["QDF_MDB_HOST"])
db = _client.qdf

def dload(name):
    mod = __import__(name[:name.rindex(".")])
    components = name.split('.')
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod

def load_config(c):
    rv = []
    if "global" not in c:
        print "Missing 'global' section"
        sys.exit(EXIT_BADCONF)
    if "enabled" not in c["global"]:
        print "Missing global/enabled"
        sys.exit(EXIT_BADCONF)
    if not c["global"]["enabled"]:
        sys.exit(EXIT_SKIP)
    try:
        klass = dload(c["global"]["algorithm"])
        for k in c:
            if k == "global":
                continue
            print "Loading instance '%s'" % k
            i = klass()

            i.deps = dict(c[k]["deps"])
            i.params = dict(c[k]["params"])
            i._conf_outputs = {ki : c[k]["outputs"][ki] for ki in c[k]["outputs"]}
            i._paramver = int(c[k]["paramver"])
            i._mintime = qdf.QDF2Distillate.date(c[k]["mintime"])
            i._maxtime = qdf.QDF2Distillate.date(c[k]["maxtime"])
            if "runonce" in c[k]:
                i._runonce = bool(c[k]["runonce"])
            else:
                i._runonce = False
            print "deps are: ", repr(i.deps)
            rv.append(i)

    except KeyError as e:
        print "Bad config, missing key", e
        setexit(EXIT_BADCONF)
        reactor.stop()
        return None
    except ImportError as e:
        print "Could not locate driver: ",e
        setexit(EXIT_BADCONF)
        reactor.stop()
        return None
    except Exception as e:
        reactor.stop()
        raise

    return rv

def onFail(param):
    print "Encountered error: ", param

@defer.inlineCallbacks
def process(qsr, algs):
    print "Entered process:", repr(qsr), repr(algs)
    try:
        for a in algs:
            a.bind_databases(db, qsr)
            a.initialize(**a.params)
            yield a._process()

        setexit(0)
        reactor.stop()
    except Exception as e:
        reactor.stop()
        raise

def entrypoint():
    print "in entrypoint"
    cfg = configobj.ConfigObj(sys.stdin)
    algs = load_config(cfg)
    if algs is None:
        return
    d = quasar.connectToArchiver(os.environ["QDF_QUASAR_HOST"], int(os.environ["QDF_QUASAR_PORT"]))
    d.addCallback(process, algs)
    d.addErrback(onFail)

if __name__ == "__main__":
    print "beginning main"
    reactor.callWhenRunning(entrypoint)
    reactor.run()
    print "EXIT CODE:", EXIT_CODE
    if EXIT_CODE is None:
        EXIT_CODE = EXIT_UNK
    sys.exit(EXIT_CODE)
