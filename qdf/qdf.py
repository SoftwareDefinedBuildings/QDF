from quasar import *
import sys
import array
from twisted.internet import defer, protocol, reactor

def _fullname(o):
    return o.__module__ + "." + o.__class__.__name__

def _get_commit():
    return "commitstr"

def onFail(param):
    reactor.stop()
    raise Exception("Encountered error: ", param)

class StreamData (object):
    __slots__ = ["times", "values", "uid", "bounds_start", "bounds_end"]
    def __init__(self, uid):
        self.uid = uid
        self.times = array.array("L")
        self.values = array.array("d")
        self.bounds_start = array.array("L")
        self.bounds_end = array.array("L")

    def addreading(self, time, value):
        self.values.append(value)
        self.times.append(time)

    def addbounds(self, start, end):
        self.bounds_start.append(start)
        self.bounds_end.append(end)

class RunReport (object):
    __slots__ = ["streams"]
    def __init__(self):
        self.streams = {}

    def addstream(self, uid, name):
        s = StreamData(uid)
        self.streams[name] = s
        return s

    def output(self, name):
        return self.streams[name]

class QDF2Distillate (object):

    #set by scheduler to {name:uuid}
    deps = []

    def __init__(self):
        self.superinit()

    def superinit(self):
        self._outputs = []
        self.inputs = []
        self._metadata = {}

    def prereqs(self, changed_ranges):
        """

        :param changed_ranges: a list of (name, uuid, [start_time, end_time])
        :return: a list of (name, uuid, [start_time, end_time]) tuples.
        """
        return changed_ranges

    @staticmethod
    def expand_prereqs_parallel(changed_ranges):
        ranges  = []
        for s in changed_ranges:
            ranges += s[2]

        combined_ranges = []
        num_streams = len(ranges)
        while True:
            progress = False
            combined = False
            #find minimum range
            minidx = 0
            for idx in xrange(num_streams):
                if len(ranges[idx]) == 0:
                    continue
                progress = True
                if ranges[idx][0][0] < ranges[minidx][0][0]:
                    minidx = idx
            #Now see if any of the other ranges starts lie before it's end
            for idx in xrange(num_streams):
                if len(ranges[idx]) == 0:
                    continue
                if idx == minidx:
                    continue
                if ranges[idx][0][0] <= ranges[minidx][0][1]:
                    if ranges[idx][0][1] > ranges[minidx][0][1]:
                        ranges[minidx][0][1] = ranges[idx][0][1]
                    ranges[idx] = ranges[idx][1:]
                    combined = True
            if not progress:
                break
            if not combined:
                combined_ranges.append(ranges[minidx][0])
                ranges[minidx] = ranges[minidx][1:]

        rv = []
        for s in changed_ranges:
            rv.append(s[0], s[1], combined_ranges[:])
        print "Combined ranges input: ", repr(changed_ranges)
        print "Combined ranges output: ", repr(rv)
        return rv

    def _sync_metadata(self):
        for skey in self._outputs:
            path = "/%s/%s/%s" % (self._section, self._name, skey)
            print "deps: ", repr(self.deps)
            #deps = ",".join("%s" % self.deps[ky] for ky in self.deps)
            doc = self.mdb.metadata.find_one({"Path":path})
            if doc is not None and doc["Metadata"]["Version"] != self._version:
                print "Rewriting metadata: version bump"
                self.mdb.metadata.remove({"Path":path})
                doc = None
            if doc is None:
                uid = self._outputs[skey][0]
                ndoc = {
                    "Path" : path,
                    "Metadata" :
                    {
                        "SourceName" : "Distillates",
                        "Instrument" :
                        {
                            "ModelName" : "Distillate Generator",
                        },
                        "Algorithm": _fullname(self),
                        "Commit": _get_commit(),
                        "Version": self._version,
                        "Name": self._name,
                    },
                    "Dependencies" :
                    {

                    },
                    "uuid" : uid,
                    "Properties" :
                    {
                        "UnitofTime" : "ns",
                        "Timezone" : "UTC",
                        "UnitofMeasure" : self._outputs[skey]["unit"],
                        "ReadingType" : "double"
                    }
                }
                for k in self._metadata:
                    ndoc["Metadata"][k] = self._metadata[k]
                for k in self.deps:
                    ndoc["Dependencies"][k] = self.deps[k]+"::0"
                self.mdb.metadata.save(ndoc)
            else:
                if int(doc["Metadata"]["Version"]) < self._version:
                    print "stream mismatch: ", int(doc["Metadata"]["Version"]), self._version
                    self._old_streams.append(skey)

                #self._streams[skey]["uuid"] = doc["uuid"]
                sdoc = {"Metadata.Version":self._version, "Metadata.Commit": get_commit()}
                for k in self._metadata:
                    sdoc["Metadata."+k]= self._metadata[k]
                for k in self._dep_vers:
                    sdoc["Dependencies."+k] = self.deps[k]+"::"+self._dep_vers[k]
                self.mdb.metadata.update({"Path":path},{"$set":sdoc},upsert=True)
                print "we inserted new version"

    def get_last_version(self, sname, dep_uid):
        path = "/%s/%s/%s" % (self._section, self._name, sname)
        r = self.mdb.metadata.find_one({"Path":path})
        if r is None:
            return 0
        return int(r["Dependencies"][sname].split("::")[1])

    def insertstreamdata(self, s):
        """
        :param s: a StreamData class
        :return: a deferred
        """
        BATCHSIZE=4000
        idx = 0
        total = len(s.times)
        dlist = []
        while idx < total:
            chunklen = BATCHSIZE if total - idx > BATCHSIZE else total-idx
            d = self._db.insertValuesEx(s.uid, s.times, idx, s.values, idx, chunklen)
            def rv((stat, arg)):
                print "Insert rv:", stat, arg
            d.addCallback(rv)
            d.addErrback(onFail)
            dlist.append(d)
            idx += chunklen
        return defer.DeferredList(dlist)

    def compute(self, changed_ranges, input_streams, params, report):
        """

        :param changed_ranges: a list of (name, uuid, [start_time, end_time])
        :param input_streams: a dictionary of {name: [(time, value)]}
        :return: a RunReport
        """
        pass

    def set_version(self, ver):
        self._version = ver

    def set_section(self, section):
        self._section = section

    def set_name(self, name):
        self._name = name

    def register_output(self, name, unit):
        self._outputs.append([None, name, unit])

    def register_input(self, name):
        self.inputs.append([None, name])

    def set_metadata(self, key, value):
        self._metadata[key] = value

    @staticmethod
    def date(dst):
        """
        This parses a modified isodate into nanoseconds since the epoch. The date format is:
        YYYY-MM-DDTHH:MM:SS.NNNNNNNNN
        Fields may be omitted right to left, but do not elide leading or trailing zeroes in any field
        """
        idate = dst[:26]
        secs = (isodate.parse_datetime(idate) - datetime.datetime(1970,1,1)).total_seconds()
        nsecs = int(secs*1000000) * 1000
        nanoS = dst[26:]
        if len(nanoS) != 3 and len(nanoS) != 0:
            raise Exception("Invalid date string!")
        if len(nanoS) == 3:
            nsecs += int(nanoS)
        return nsecs
