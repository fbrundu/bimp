# generic (pseudo-)private method for a database query
# TODO: more clauses to add (e.g. GROUPBY)
def generic_query(cur, pfrom, pselect=None, pwhere=None):

    if pselect is None:
        pselect = "*"
    if pwhere is not None:
        qcmd = """SELECT %s FROM %s WHERE %s""" % (pselect, pfrom, pwhere)
    else:
        qcmd = """SELECT %s FROM %s""" % (pselect, pfrom)
    cur.execute(qcmd)
    return cur.fetchall()
