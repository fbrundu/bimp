import cherrypy
import collections
from lib import get_parameters
from db_common import *
import sqlserver as db
import traceback


def query(params, pbc):

    log = ""

    # get buildings' names
    building_names = get_parameters(params, "building")

    # connect to default database
    conn, cur = db.connect(pbc["dbhost"], pbc["dbuser"], pbc["dbpass"])

    # get query name
    qname = params["qname"]

    # prepare query parameters dictionary (for response)
    qparams = {}

    # get available buildings in the database
    available_buildings = db.get_dbnames(cur)

    # building wildcard
    if "*" in building_names:
        building_names = available_buildings

    # initialize result list
    result = []

    # for each building
    for building in building_names:

        # if building is not present on database
        if building not in available_buildings:
            log += "Building not found: " + building

        # connect to building database
        bconn, bcur = db.connect(pbc["dbhost"], pbc["dbuser"], pbc["dbpass"], building)

        # initialize building dictionary
        b_dict = {
            "b_id": building,
            "b_res": []
        }

        try:
            # get number of walls for current building
            if qname == "getnwalls":
                typeid = None

                # in case, get only the number of walls of a certain type
                if "typeid" in params:
                    typeid = params["typeid"]
                    if "typeid" not in qparams:
                        qparams["typeid"] = typeid
                b_dict["b_res"] += db.get_nwallstype(bcur, typeid=typeid)

            # get ids of the windows present in a wall
            elif qname == "getwindowsinwall":
                if "wallid" in params:
                    wallid = params["wallid"]
                    if "wallid" not in qparams:
                        qparams["wallid"] = wallid
                    b_dict["b_res"] += db.get_windowsinwall(bcur, wallid)
                else:
                    raise cherrypy.HTTPError("422 Unprocessable entity", "Parameter 'wallid' not given.")

            # get id of the wall which host a window
            elif qname == "gethostingwall":
                if "windowid" in params:
                    windowid = params["windowid"]
                    if "windowid" not in qparams:
                        qparams["windowid"] = windowid
                    b_dict["b_res"] += db.get_hostingwall(bcur, windowid)
                else:
                    raise cherrypy.HTTPError("422 Unprocessable entity", "Parameter 'windowid' not given.")

            # get type of the walls present in a building
            elif qname == "getwalltypes":
                b_dict["b_res"] += db.get_walltypes(bcur)

            # get building typology
            elif qname == "gettypology":

                typology = db.get_buildingtypology(bcur)

                b_dict["b_res"] += typology

            # get heating supply
            elif qname == "getheatingsupply":

                heating = db.get_heatingsupply(bcur)

                b_dict["b_res"] += heating

            # get age of construction
            elif qname == "getageofconstruction":

                age = db.get_ageofconstruction(bcur)

                b_dict["b_res"] += age

            # get occupancy
            elif qname == "getoccupancy":

                occupancy = db.get_occupancy(bcur)

                b_dict["b_res"] += occupancy

            elif qname == "getbuildinguse":

                b_result = db.get_buildinguse(bcur)

                b_dict["b_res"] += b_result

            elif qname == "getelectricitysupply":

                b_result = db.get_electricitysupply(bcur)

                b_dict["b_res"] += b_result

            elif qname == "getlocation":

                b_result = db.get_location(bcur)

                b_dict["b_res"] += b_result

            elif qname == "getname":

                b_result = db.get_name(bcur)

                b_dict["b_res"] += b_result

            elif qname == "getorientation":

                b_result = db.get_orientation(bcur)

                b_dict["b_res"] += b_result

            elif qname == "getrenewable":

                b_result = db.get_renewable(bcur)

                b_dict["b_res"] += b_result

            elif qname == "getsv":

                b_result = db.get_sv(bcur)

                b_dict["b_res"] += b_result

            elif qname == "getcasename":

                b_result = db.get_casename(bcur)

                b_dict["b_res"] += b_result

            elif qname == "getcasenumber":

                b_result = db.get_casenumber(bcur)

                b_dict["b_res"] += b_result

            elif qname == "getuse":

                b_result = db.get_use(bcur)

                b_dict["b_res"] += b_result

            elif qname == "gettcsensor":

                b_result = db.get_tcsensor(bcur)

                b_dict["b_res"] += b_result

            elif qname == "gethostid":

                b_result = db.get_hostid(bcur)

                b_dict["b_res"] += b_result

            elif qname == "getvirtualmeterid":

                b_result = db.get_virtualmeterid(bcur)

                b_dict["b_res"] += b_result

            elif qname == "getopaque":

                b_result = db.get_opaquesurf(bcur)

                b_dict["b_res"] += b_result

            elif qname == "gettransparent":

                b_result = db.get_transpsurf(bcur)

                b_dict["b_res"] += b_result

            elif qname == "getmeasuredcons":

                b_result = db.get_annualmeascons(bcur)

                b_dict["b_res"] += b_result

            elif qname == "getsimulatedcons":

                b_result = db.get_annualsimulcons(bcur)

                b_dict["b_res"] += b_result

            elif qname == "getsensors":

                pwhere = None

                try:
                    sensor_id = get_parameters(params, "sensor_id")

                    if sensor_id:
                        pwhere = ["Id", str(sensor_id[0])]
                except:
                    pwhere = None

                b_result = db.get_sensors(bcur, pwhere=pwhere)

                b_dict["b_res"] += b_result

            # FIXME temp
            elif qname == "getshared":

                shared = db.get_shared(bcur)

                b_dict["b_res"] += shared

            # query not implemented
            else:

                # close connection to building database
                db.close(bconn, bcur)

                log += "Query not implemented: " + qname

                raise

            # insert building dictionary
            if b_dict["b_res"]:
                result += [b_dict]

        except Exception as e:

            log += traceback.format_exc()

        # close connection to building database
        db.close(bconn, bcur)

    # close connection to default database
    db.close(conn, cur)

    # return query result in a dictionary
    return result, qname, qparams, log


# serialize buildings to an array
def serialize(params, pbc):

    log = ""

    # get buildings' names
    building_names = get_parameters(params, "building")

    # initialize data array
    data = []

    # connect to default database
    conn, cur = db.connect(pbc["dbhost"], pbc["dbuser"], pbc["dbpass"])

    # get available buildings on database
    available_buildings = db.get_dbnames(cur)

    # building wildcard
    if "*" in building_names:
        building_names = available_buildings

    # for each building
    for building in building_names:

        # if building is not present send HTTP error
        if building not in available_buildings:
            log += "Building not found: " + building

        # connect to building database
        bconn, bcur = db.connect(pbc["dbhost"], pbc["dbuser"], pbc["dbpass"], building)

        # create building dictionary with metadata
        building_dict = {"b_id": building, "b_model": None}

        # initialize model dictionary
        model = {}

        # get tables from the database for the current building
        for tn in db.get_tablenames(bcur, building):

            # get tables data from the database for the current building
            model[tn] = db.get_tabledata(bcur, tn)

        # close connection to building database
        db.close(bconn, bcur)

        # save building model
        building_dict["b_model"] = collections.OrderedDict(sorted(model.items()))

        # sort data tables
        data.append(building_dict)

    # close connection to default database
    db.close(conn, cur)

    # return buildings' array
    return data, log
