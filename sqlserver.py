import pymssql
from db_common import *


# connect to database
def connect(server, user, password, dbname=None):

    if dbname is None:
        dbname = "dimmer"
    conn = pymssql.connect(server, user, password, dbname)
    cur = conn.cursor(as_dict=True)
    return conn, cur


# close connection to database
def close(conn, cur):

    cur.close()
    conn.close()


# create new database
def create_db(conn, dbname):

    connection.autocommit(True)
    cur = conn.cursor()
    cur.execute("""CREATE database %s""" % str(dbname))
    connection.autocommit(False)


# get names of the databases in the server instance
def get_dbnames(cur):

    pselect = """name"""
    pfrom = """master..sysdatabases"""
    pwhere = """name NOT IN ('master', 'tempdb', 'model', 'msdb') AND
                name NOT IN ('dimmer') AND name NOT LIKE '%ReportServer%'"""
    dbnames = generic_query(cur, pfrom, pselect, pwhere)
    dbnames = sorted([e["name"] for e in dbnames])
    return dbnames


# get names of the tables in the database
def get_tablenames(cur, dbname):

    pselect = """table_name"""
    pfrom = """information_schema.tables"""
    pwhere = """table_type = 'BASE TABLE' AND
                table_catalog = '%s'""" % str(dbname)
    table_names = generic_query(cur, pfrom, pselect, pwhere)
    table_names = sorted([e["table_name"] for e in table_names])
    return table_names


# get table data for given schema and table
def get_tabledata(cur, table_name, schema_name="dbo"):

    pfrom = '"' + schema_name + '"."' + table_name + '"'
    return generic_query(cur, pfrom)


# get number of walls in a building, optionally of a particular type
def get_nwallstype(cur, schema_name="dbo", typeid=None):

    pselect = 'COUNT(*) as Count'
    pfrom = schema_name + "." + '"Walls"'
    pwhere = None
    if typeid is not None:
        pwhere = '"TypeId" = %s' % typeid
    result = generic_query(cur, pfrom, pselect, pwhere)
    return [str(result[0]["Count"])]


# get types of walls in a building
def get_walltypes(cur, schema_name="dbo"):

    pselect = 'DISTINCT "TypeId"'
    pfrom = schema_name + "." + '"Walls"'
    pwhere = None
    result = generic_query(cur, pfrom, pselect, pwhere)
    return [str(r["TypeId"]) for r in result]


# get id of windows in a wall
def get_windowsinwall(cur, wallid, schema_name="dbo"):

    pselect = '"Id"'
    pfrom = schema_name + "." + '"Windows"'
    pwhere = '"HostId" = %s' % wallid
    result = generic_query(cur, pfrom, pselect, pwhere)
    return [str(r["Id"]) for r in result]


# get id of wall hosting a window
def get_hostingwall(cur, windowid, schema_name="dbo"):

    pselect = '"HostId"'
    pfrom = schema_name + "." + '"Windows"'
    pwhere = '"Id" = %s' % windowid
    result = generic_query(cur, pfrom, pselect, pwhere)
    return [str(result[0]["HostId"])]


# get building typology of building
def get_buildingtypology(cur, schema_name="dbo"):

    field_name = "Building Typology"

    return get_field(cur, field_name, schema_name)


# get heating supply of building
def get_heatingsupply(cur, schema_name="dbo"):

    field_name = "Heating Supply"

    return get_field(cur, field_name, schema_name)


# get age of construction of building
def get_ageofconstruction(cur, schema_name="dbo"):

    field_name = "Construction Period"

    return get_field(cur, field_name, schema_name)


# get occupancy of building
def get_occupancy(cur, schema_name="dbo"):

    field_name = "Occupancy Number"

    return get_field(cur, field_name, schema_name)


# get building energy certification
def get_certification(cur, schema_name="dbo"):

    field_name = "Building Energy Certification"

    return get_field(cur, field_name, schema_name)


def get_buildinguse(cur, schema_name="dbo"):

    field_name = "Building Use"

    return get_field(cur, field_name, schema_name)


def get_electricitysupply(cur, schema_name="dbo"):

    field_name = "Electricity Supply"

    return get_field(cur, field_name, schema_name)


def get_location(cur, schema_name="dbo"):

    field_name = "Location"

    return get_field(cur, field_name, schema_name)


def get_name(cur, schema_name="dbo"):

    field_name = "Name"

    return get_field(cur, field_name, schema_name)


def get_orientation(cur, schema_name="dbo"):

    field_name = "Orientation"

    return get_field(cur, field_name, schema_name)


def get_renewable(cur, schema_name="dbo"):

    field_name = "Renewable energy"

    return get_field(cur, field_name, schema_name)


def get_sv(cur, schema_name="dbo"):

    field_name = "S/V"

    return get_field(cur, field_name, schema_name)


def get_casename(cur, schema_name="dbo"):

    field_name = "CASE_NAME"

    return get_field(cur, field_name, schema_name)


def get_casenumber(cur, schema_name="dbo"):

    field_name = "CASE NUMBER"

    return get_field(cur, field_name, schema_name)


def get_use(cur, schema_name="dbo"):

    field_name = "USE"

    return get_field(cur, field_name, schema_name)


def get_tcsensor(cur, schema_name="dbo"):

    field_name = "TC_sensor"

    return get_field(cur, field_name, schema_name)


def get_hostid(cur, schema_name="dbo"):

    field_name = "HOST_ID"

    return get_field(cur, field_name, schema_name)


def get_virtualmeterid(cur, schema_name="dbo"):

    field_name = "VIRTUAL_METER_ID"

    return get_field(cur, field_name, schema_name)


def get_opaquesurf(cur, schema_name="dbo"):

    field_name = "% opaque surface"

    return get_field(cur, field_name, schema_name)


def get_transpsurf(cur, schema_name="dbo"):

    field_name = "% transparent surface"

    return get_field(cur, field_name, schema_name)


def get_annualmeascons(cur, schema_name="dbo"):

    field_name = "Total annual measured energy consumption (kWh)"

    return get_field(cur, field_name, schema_name)


def get_annualsimulcons(cur, schema_name="dbo"):

    field_name = "Total annual simulated energy consumption (kWh)"

    return get_field(cur, field_name, schema_name)


def get_sensors(cur, schema_name="dbo", pwhere=None):

    field_name = "TC_SENSOR"

    return get_field_list(cur, field_name, schema_name=schema_name, pwhere=pwhere)


def get_field(cur, field_name, schema_name="dbo"):

    pselect = '"' + field_name + '"'
    pfrom = schema_name + "." + '"Mass"'
    result = generic_query(cur, pfrom, pselect)
    result = [r for r in result if r[field_name] is not None]

    if result:
        result = [str(result[0][field_name])]
    else:
        result = []

    return result


def get_field_list(cur, field_name, schema_name="dbo", pwhere=None):

    pselect = '"' + field_name + '"'
    pfrom = schema_name + "." + '"Mass"'
    if pwhere is not None:
        pwhere = pwhere[0] + "=" + pwhere[1]
    result = generic_query(cur, pfrom, pselect, pwhere)
    result = [r for r in result if r[field_name] is not None]

    if result:
        result = [str(r[field_name]) for r in result]
    else:
        result = []

    return result


# FIXME temp get shared
def get_shared(cur, schema_name="dbo"):

    pselect = '*'
    pfrom = schema_name + "." + '"Mass"'
    result = generic_query(cur, pfrom, pselect)
    return [{str(k): str(result[0][k]) for k in result[0].keys()}]
