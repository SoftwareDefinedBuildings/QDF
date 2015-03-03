#!/usr/bin/env python
import pymongo
import time
import datetime
import os
import subprocess
import sys

_client = pymongo.MongoClient(os.environ["QDF_MDB_HOST"])
db = _client.qdf
sys.path.append(os.environ["QDF_ALGBASE"])

statusmap = {0: "OK: Changes made", 1:"ERR: Internal", 2:"ERR: Bad config file", 3:"OK: Disabled in config", 4:"ERR: Unknown error", 5:"OK: No change in data"}


def do_run():
    start_time = time.time()
    run_id = time.strftime("%y.%m.%d.%H.%M")
    print "Beginning run", run_id
    print "================================================="

    #enumerate all instance files
    files = []

    for root, dirs, ff in os.walk(os.environ["QDF_INIBASE"]):
        for f in ff:
            if f.endswith(".ini"):
                files.append(f)

    print "TOTAL OF %d INI FILES" % len(files)

    all_instances = []
    for instance in files:
        then = time.time()
        instance_name = instance[:-4] #strip .ini
        print "Processing instance: ", instance_name
        logname = "%s_%s.txt" % (run_id, instance_name)
        logfile = open(os.path.join(os.environ["QDF_LOGBASE"], logname), "w")
        n = datetime.datetime.now()
        rec = {"logname":logname, "run_id":run_id, "instance":instance_name, "started":n, "retcode":-1, "retcode_human":"incomplete", "time":-1, "status":"incomplete"}
        rec["syear"] = n.year
        rec["smonth"] = n.month
        rec["sday"] = n.day
        rec["shour"] = n.hour
        rec["sminute"] = n.minute
        db.runs.save(rec)
        instancefile = open(os.path.join(os.environ["QDF_INIBASE"], instance),"r")
        proc = subprocess.Popen(["../qdf/subscheduler.py"], stdin=instancefile, stdout=logfile, stderr=subprocess.STDOUT)
        all_instances.append((proc, run_id, instance_name, then, logfile, instancefile))
    for tpl in all_instances:
        proc, run_id, instance_name, then, logfile, instancefile = tpl
        print "Waiting for: ", instance_name
        retcode = proc.wait()
        postrun_record = db.runs.find_one({"run_id":run_id, "instance":instance_name})
        postrun_record["retcode"] = retcode
        postrun_record["retcode_human"] = statusmap[retcode]
        postrun_record["status"] = "complete"
        postrun_record["time"] = time.time()-then
        postrun_record["ok"] = retcode in [0, 3, 5]
        db.runs.save(postrun_record)
        logfile.close()
        instancefile.close()
        print "complete - %.2fs, status: %s " % (postrun_record["time"], statusmap[retcode])

    print "== RUN COMPLETE == "
    time.sleep(10)

if __name__ == "__main__":
    print "starting"
    while True:
	do_run()
