
import pg_session 
from model import Model, Property, init_models
import psycopg2 as pg

class Ticket(Model):
    _table_name = 'mm_ticket'
    _key = 'id'
    id = Property()
    status = Property(fieldname='state')
    subject = Property()
    from_email = Property()
    account_id = Property()

class UserAbility(Model):
    _table_name = 'mm_user_abilities_category0'
    _key = 'category0,username'
    category0 = Property()
    username = Property()

init_models(Ticket, UserAbility)

def run():
    con = pg.connect("dbname=mmdb")
    ds = pg_session.PgSession(con, 'galtsev')
    print [ab.category0 for ab in UserAbility.query(ds).filter('username', 'one')]
    #Ticket.get(session, 26).delete()
    #ticket = Ticket.get(session, 26)
    #print [t.id for t in Ticket.query(session).filter('id <', 30).order('id')]
    #for r in Ticket.query(session).filter('subject like', 'test%').order('-id').fetch(10):
    #    print r
    #ticket.subject = "updated subject"
    #ticket.delete()
    con.commit()

run()

