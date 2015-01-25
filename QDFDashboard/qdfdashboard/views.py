from pyramid.view import view_config
import pymongo
import os
import calendar
_client = pymongo.MongoClient(os.environ["QDF_MDB_HOST"])
db = _client.qdf

@view_config(route_name='latest', renderer='qdfdashboard:templates/latest.mako')
def latest(request):
    try:
        offset = int(request.GET.getone("off"))
    except KeyError:
        offset = 0
    dat = db.runs.find().sort("started", -1).skip(offset).limit(200)
    dat = list(dat)

    return {'runs':list(dat),'noffset':offset+200, 'hasnoffset':(len(dat) == 200)}

@view_config(route_name='logfile', renderer='string')
def logfile(request):
    name = request.matchdict["id"]
    with open(os.path.join(os.environ["QDF_LOGBASE"], name), "r") as f:
        contents = f.read()
        return contents

@view_config(route_name='archive', renderer='qdfdashboard:templates/archive.mako')
def archive(request):
    instances = db.runs.distinct("instance")
    dat = []
    for i in instances:
        years = db.runs.find({"instance":i}).distinct("syear", )
        dat_i = {"name": i, "rows":[]}
        for y in years:
            months = db.runs.find({"instance":i,"syear":y}).distinct("smonth")
            dat_y = {"name": "%04d" % y, "rows":[], "ok":True}
            for m in months:
                dat_m = {"name": "%02d" % m, "rows":[], "ok":True}
                days = db.runs.find({"instance":i, "syear":y, "smonth":m}).distinct("sday")


                for d in days:
                    r = {"name":"%02d" % d, "ok":True}
                    bad = db.runs.find_one({"instance":i, "syear":y, "smonth":m,"sday":d,"ok":False})
                    if bad is not None:
                        dat_m["ok"] = False
                        dat_y["ok"] = False
                        r["ok"] = False
                    dat_m["rows"].append(r)
                dat_y["rows"].append(dat_m)
            dat_i["rows"].append(dat_y)
        dat.append(dat_i)
    return {"instances":dat}

@view_config(route_name='daylist', renderer='qdfdashboard:templates/daylist.mako')
def daylist(request):

    y = int(request.matchdict["year"])
    m = int(request.matchdict["month"])
    d = int(request.matchdict["day"])
    i = request.matchdict["inst"]
    dat = db.runs.find({"syear":y, "smonth":m, "sday":d, "instance":i}).sort("started", -1)
    dat = list(dat)

    return {'runs':list(dat)}