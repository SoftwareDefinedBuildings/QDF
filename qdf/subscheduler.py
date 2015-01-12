__author__ = 'immesys'

import configobj
import sys
from pymongo import MongoClient
import os
import quasar
import uuid
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
            i = klass(**c[k]["params"])
            i.deps = dict(c[k]["deps"])
            i.uid = c[k]["uuid"]
            _ = uuid.UUID(i.uid)
            print "deps are: ", repr(i.deps)
            print "uid is: ", repr(i.uid)
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
        raise e

    return rv

def get_last_version(alg_uid, dep_uid):
    r = db.dep_versions.find_one({"alg_uuid":alg_uid, "dep_uuid":dep_uid})
    if r is None:
        return 0
    return r["dep_ver"]

def onFail(param):
    print "Encountered error: ", param

@defer.inlineCallbacks
def process(qsr, algs):
    print "Entered process:", repr(qsr), repr(algs)
    try:
        for a in algs:
            # get the dependency past versions
            lver = {}
            for k, uid in a.deps:
                lver[uid] = get_last_version(a.uid, uid)

            # get the dependency current versions (freeze)
            cver_keys = [k for k in a.deps]
            cver_uids = [a.deps[k] for k in a.deps]
            uid_keymap = {cver_uids[i] : cver_keys[i] for i in xrange(len(cver_keys))}
            v = yield qsr.queryVersion(cver_uids)
            cver = {cver_uids[i] : v[i] for i in xrange(len(cver_uids))}

            # get changed ranges
            chranges = []
            for k in lver:
                cr = yield qsr.queryChangedRanges(k, lver[k], cver[k])
                chranges.append[(k, uid_keymap[k], )]

            # TODO chunk changed ranges

            # get adjusted ranges
            prereqs = a.prereqs(chranges)
            print "prereqs are ", repr(prereqs)

            # query data
            data = []

            # TODO query data

            # process
            results = {}
            a.compute(chranges, data, results)

        setexit(0)
        reactor.stop()
    except Exception as e:
        reactor.stop()
        raise e

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
