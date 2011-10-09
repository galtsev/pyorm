
import pg_session 
from model import Model, Property, init_model
import psycopg2 as pg

class Ticket(Model):
    _table_name = 'mm_ticket'
    _key = 'id'
    id = Property()
    status = Property(fieldname='state')
    subject = Property()
    from_email = Property()
    account_id = Property()

init_model(Ticket)

def run():
    con = pg.connect("dbname=gdv")
    session = pg_session.PgSession(con, 'gdv')
    #ticket = Ticket.get(con, 26)
    #print ticket
    #for r in Ticket.query(session).filter('subject like', 'test%').order('-id').fetch(10):
    #    print r
    ticket = Ticket.get(session, 32)
    print ticket
    #ticket.subject = "updated subject"
    ticket.delete()
    con.commit()

run()

