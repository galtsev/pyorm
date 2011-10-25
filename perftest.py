
import psycopg2 as pg
from time import time
from model import Model, Property, init_models
from pg_session import PgSession

class Event(Model):
    _key = 'id'
    _table_name = '$(schema).mm_event'
    id = Property()
    ticket_id = Property()
    event_date = Property()
    event_type = Property()
    account_id = Property()
    assigned = Property()
    event_period = Property()

init_models(Event)

def run():
    con = pg.connect("dbname=mmdb host=egbackup")
    ds = PgSession(con, 'egroup2')
    t1 = time()
    grp = {}
    cnt = 0
    dt_from, dt_to = '2011-09-01', '2011-10-01'
    for r in Event.query(ds).filter('event_date >', dt_from).filter('event_date <', dt_to).filter('event_type', 'respond'):
        gcnt, gtime = grp.get(r.account_id, (0,0))
        grp[r.account_id] = (gcnt+1, gtime+(r.event_period or 0))
        cnt += 1
    print '='*20
    print 'cnt:', cnt
    for k, v in grp.iteritems():
        print k, v
    t2 = time()
    print t2-t1
    grp1 = {}
    cnt1 = 0
    for account_id, event_period in ds.execute("select account_id, event_period from $(schema).mm_event where event_date between %(dt_from)s and %(dt_to)s and event_type='respond'", dict(dt_from=dt_from, dt_to = dt_to)):
        gcnt, gtime = grp1.get(account_id, (0,0))
        grp1[account_id] = (gcnt+1, gtime + (event_period or 0))
        cnt1 += 1
    print '='*20
    print 'cnt1:', cnt1
    for k, v in grp1.iteritems():
        print k, v
    t3 = time()
    print t3-t2

run()


