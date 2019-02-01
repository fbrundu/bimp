import cherrypy
from cherrypy import log
from lib import get_parameters, handle_error
import datetime
import json
import os
import dbms as db
import tempfile as tf
import zipfile as zf

# load configuration into global dictionary
with open("conf/conf.json", "r") as cfile:
    pbc = json.load(cfile)

QUERY_VERSION = 1


# Ping microservice
class Ping(object):
    exposed = True

    def GET(self, **params):

        try:
            return handle_error(200, "Pong")
        except:
            return handle_error(500, "Internal Server Error")


# GetJSON microservice
class GetJSON(object):
    exposed = True

    def GET(self, *paths, **params):

        try:
            # get buildings as JSON array
            result, internal_log = get_json(params)

            if internal_log:
                log.error(msg=internal_log, context='HTTP')

            # result to json
            response = json.dumps(result, indent=2).encode("utf8")

            # return response
            ctype = "application/json;charset=utf-8"
            cherrypy.response.headers["Content-Type"] = ctype

            return response
        except:
            return handle_error(500, "Internal Server Error")


# GetIFC microservice
class GetIFC(object):
    exposed = True

    def GET(self, *paths, **params):

        try:
            # get zip of ifcs
            response = get_resources(params, "ifc")

            # set response header for zip
            cherrypy.response.headers["Content-Type"] = "application/zip"
            cdisp = 'attachment; filename="resp.zip"'
            cherrypy.response.headers["Content-Disposition"] = cdisp

            return response
        except:
            return handle_error(500, "Internal Server Error")


# GetGBXML microservice
class GetGBXML(object):
    exposed = True

    def GET(self, *paths, **params):

        try:
            # get zip of gbxmls
            response = get_resources(params, "xml")

            # set response header for zip
            cherrypy.response.headers["Content-Type"] = "application/zip"
            cdisp = 'attachment; filename="resp.zip"'
            cherrypy.response.headers["Content-Disposition"] = cdisp

            return response
        except:
            return handle_error(500, "Internal Server Error")


# GetFBX  microservice
class GetFBX(object):
    exposed = True

    def GET(self, *paths, **params):

        try:
            # get zip of gbxmls
            response = get_resources(params, "fbx")

            # set response header for zip
            cherrypy.response.headers["Content-Type"] = "application/zip"
            cdisp = 'attachment; filename="resp.zip"'
            cherrypy.response.headers["Content-Disposition"] = cdisp

            return response
        except:
            return handle_error(500, "Internal Server Error")


# GetRVT microservice
class GetRVT(object):
    exposed = True

    def GET(self, *paths, **params):

        try:
            # get zip of rvts
            response = get_resources(params, "rvt")

            # set response header for zip
            cherrypy.response.headers["Content-Type"] = "application/zip"
            cdisp = 'attachment; filename="resp.zip"'
            cherrypy.response.headers["Content-Disposition"] = cdisp

            return response
        except:
            return handle_error(500, "Internal Server Error")


# Query microservice
class Query(object):
    exposed = True

    def GET(self, *paths, **params):

        try:
            # do query on buildings' database
            result, qdesc, qparams, internal_log = db.query(params, pbc)

            if internal_log:
                log.error(msg=internal_log, context='HTTP')

            # create response
            response = {
                "r_ver": QUERY_VERSION,
                "q_ts": datetime.datetime.now().isoformat(),
                "q_desc": qdesc,
                "q_par": qparams,
                "q_res": result
            }

            # result to json
            response = json.dumps(response).encode("utf8")
            ctype = "application/json;charset=utf-8"
            cherrypy.response.headers["Content-Type"] = ctype

            # return response
            return response
        except:
            return handle_error(500, "Internal Server Error")


# to start the Web Service
def start():

    # start Web Service with some configuration
    if pbc["stage"] == "production":
        global_conf = {
               "global":    {
                              "server.environment": "production",
                              "engine.autoreload.on": True,
                              "engine.autoreload.frequency": 5,
                              "server.socket_host": "0.0.0.0",
                              "server.socket_port": 8082,
                              "log.screen": False,
                              "log.access_file": "bimp.log",
                              "log.error_file": "bimp.log"
                            }
        }
        cherrypy.config.update(global_conf)
    conf = {
        "/": {
            "request.dispatch": cherrypy.dispatch.MethodDispatcher()
        }
        # "/bimp/getjson": {
        #     "tools.encode.on": True,
        # }
    }

    cherrypy.tree.mount(Ping(), '/bimp/ping', conf)
    cherrypy.tree.mount(GetJSON(), '/bimp/getjson', conf)
    cherrypy.tree.mount(GetIFC(), '/bimp/getifc', conf)
    cherrypy.tree.mount(GetGBXML(), '/bimp/getgbxml', conf)
    cherrypy.tree.mount(GetFBX(), '/bimp/getfbx', conf)
    cherrypy.tree.mount(GetRVT(), '/bimp/getrvt', conf)
    cherrypy.tree.mount(Query(), '/bimp/query', conf)

    # activate signal handler
    if hasattr(cherrypy.engine, "signal_handler"):
        cherrypy.engine.signal_handler.subscribe()

    # start serving pages
    cherrypy.engine.start()
    cherrypy.engine.block()


# get zip of resources by extension
def get_resources(params, ext):

    # for rvt files, revit's version is needed
    version = ""
    if ext == "rvt":
        version = "_" + str(get_parameters(params, "version")[0])

    # map buildings to filenames
    filenames = [b + version + "." + ext
                 for b in get_parameters(params, "building")]

    # zip files
    data = zip_files(filenames)

    # return zip
    return data


def get_json(params):

    # get array of buildings
    data, internal_log = db.serialize(params, pbc)

    # return array of buildings
    return data, internal_log


# zip files from filenames
def zip_files(filenames):

    # zip in a temporary file
    with tf.SpooledTemporaryFile() as tmp:
        with zf.ZipFile(tmp, 'w', zf.ZIP_DEFLATED) as archive:

            # create zip
            for f in filenames:
                try:
                    archive.write(os.path.join(pbc["respath"], f), arcname=f)
                except Exception:
                    # FIXME notification in case of error
                    pass

        # reset file pointer
        tmp.seek(0)

        # save data to variable
        data = tmp.read()

    # return data
    return data
