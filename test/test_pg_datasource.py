
import test_base
from pyorm.pg_datasource import DataSet

import psycopg2 as pg
import psycopg2.extensions as ex

ex.register_type(ex.UNICODE)
ex.register_adapter(list, ex.SQL_IN)

def create_schema(schemaname):
     return """
    create schema sss;
    create sequence sss.gen_ticket_id;
    select setval('sss.gen_ticket_id', 100);
    create table sss.user(
        username text,
        full_name text,
        email text,
        department text
    );
    create table sss.agent(
        username text,
        full_name text,
        email text,
        department text,
        position text
    );
    create table sss.tt_group(
        groupname text
    );
    create table sss.group_member(
        username text,
        groupname text
    );
    create table sss.ticket(
        id int,
        state text,
        subject text,
        assigned text,
        date_opened timestamp,
        priority int
    );
    """.replace('sss',schemaname)

class TestDataSet(DataSet):
    def gen_id(self, gen_name):
        sql = "select nextval('%s.%s')" % (self.schema, gen_name)
        cur = self.execute(sql, {})
        return cur.fetchone()[0]

class AllTests(test_base.BaseTests):
    def setUp(self):
        con = self.con = pg.connect("dbname=pyorm_test")
        con.set_client_encoding('utf8')
        self.dsa = TestDataSet(con, 'orma')
        self.dsa.execute(create_schema('orma'))
        self.populate_dataset(self.dsa)
        self.dsa.commit()
    def tearDown(self):
        cur = self.con.cursor()
        cur.execute('drop schema orma cascade')
        self.con.commit()
        self.con.close()
    def testRawOrder(self):
        ds = self.dsa
        ticket_ids = [t.id for t in test_base.Ticket.query(ds).raw_order('lower(subject)')]
        self.assert_(ticket_ids == [1,2,3,4])
    def testRawFilter(self):
        ds = self.dsa
        ticket = test_base.Ticket.query(ds).raw_filter('lower(subject)=%(subject)s', {'subject': 'subj a'}).fetchone()
        self.assert_(ticket.id == 2)
