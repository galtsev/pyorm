
import psycopg2 as pg
import psycopg2.extensions
from model import Model, Property
from pg_datasource import DataSet
import unittest
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
    insert into sss.tt_group(groupname) values ('gr1');
    insert into sss.tt_group(groupname) values ('gr2');
    insert into sss.user(username, full_name, email, department) values ('usr1', 'user 1', 'usr1@example.com', 'a');
    insert into sss.user(username, full_name, email, department) values ('usr2', 'user 2', 'usr2@example.com', 'a');
    insert into sss.user(username, full_name, email, department) values ('usr3', 'user 3', 'usr3@example.com', 'a');
    insert into sss.user(username, full_name, email, department) values ('usr4', 'user 4', 'usr4@example.com', 'b');
    insert into sss.agent(username, full_name, email, department, position) values ('dir', 'Director', 'dir@example.com', 'b', 'director');
    insert into sss.agent(username, full_name, email, department, position) values ('manager', 'Manager', 'manager@example.com', 'b', 'manager');
    insert into sss.group_member(username, groupname) values ('usr1', 'gr1');
    insert into sss.group_member(username, groupname) values ('usr2', 'gr1');
    insert into sss.group_member(username, groupname) values ('usr3', 'gr1');
    insert into sss.group_member(username, groupname) values ('usr1', 'gr2');
    insert into sss.group_member(username, groupname) values ('usr4', 'gr2');
    """.replace('sss',schemaname)

def orma_add(schemaname):
    return """
    /* grop gr3 and user usera exists in schema orma only */
    insert into sss.tt_group(groupname) values ('gr3');
    insert into sss.user(username, full_name, email, department) values ('usra', 'user a', 'usra@example.com', 'b');
    insert into sss.ticket(id, state, subject, assigned, date_opened, priority) values (1, 'Open', 'foo', 'usr1', '2011-10-02 12:01', 3);
    insert into sss.ticket(id, state, subject, assigned, date_opened, priority) values (2, 'Open', 'subj a', 'usr1', '2011-10-03 12:01', 3);
    insert into sss.ticket(id, state, subject, assigned, date_opened, priority) values (3, 'Open', 'subj b', 'usra', '2011-10-02 12:01', 5);
    insert into sss.ticket(id, state, subject, assigned, date_opened, priority) values (4, 'Hold', 'subj c', 'usra', '2011-10-04 12:01', 3);
    """.replace('sss', schemaname)

class VerifyError(Exception): pass

class User(Model):
    _key = 'username'
    _table_name = '$(schema).user'
    username = Property()
    full_name = Property()
    email = Property()
    department = Property()

# check model inheritance
class Agent(User):
    _table_name = '$(schema).agent'
    position = Property()

class Group(Model):
    _key = 'groupname'
    _table_name = '$(schema).tt_group'
    groupname = Property()

class GroupMember(Model):
    _table_name = '$(schema).group_member'
    _key = 'username,groupname'
    username = Property()
    groupname = Property()

class Ticket(Model):
    _table_name = '$(schema).ticket'
    _key = 'id'
    id = Property()
    state = Property()
    subject = Property()
    assigned = Property()
    date_opened = Property()
    priority = Property(default=3)
    v_state = Property(fieldname='lower(state)', virtual=True)
    def before_save(self):
        if self.state not in ('Open', 'Hold', 'Spam'):
            raise VerifyError("Wrong status: %s" % self.state)
        if self.id is None:
            self.id = self.session.gen_id('gen_ticket_id')

# composite query
class TicketUser(Model):
    id = Property()
    state = Property(fieldname='t.state')
    username = Property()
    full_name = Property()
    @classmethod
    def get_from(cls):
        return """from $(schema).ticket t join $(schema).user u on (t.assigned=u.username)"""

class TestDataSet(DataSet):
    def gen_id(self, gen_name):
        sql = "select nextval('%s.%s')" % (self.schema, gen_name)
        cur = self.execute(sql, {})
        return cur.fetchone()[0]

class AllTests(unittest.TestCase):
    def setUp(self):
        con = self.con = pg.connect("dbname=pyorm_test")
        con.set_client_encoding('utf8')
        cur = con.cursor()
        cur.execute(create_schema('orma'))
        cur.execute(create_schema('ormb'))
        cur.execute(orma_add('orma'))
        con.commit()
        self.dsa = TestDataSet(con, 'orma')
        self.dsb = TestDataSet(con, 'ormb')
    def tearDown(self):
        cur = self.con.cursor()
        cur.execute('drop schema orma cascade')
        cur.execute('drop schema ormb cascade')
        self.con.commit()
        self.con.close()
    def testSelect(self):
        cur = self.con.cursor()
        cur.execute("select email from sss.user where username='usr1'".replace('sss', 'orma'))
        users = cur.fetchall()
        self.assert_(len(users)==1)
        self.assert_(users[0][0] == 'usr1@example.com')
    def testGet(self):
        ds = self.dsa
        # single-field key
        user = User.get(ds, 'usr1')
        self.assertEqual(user.full_name, 'user 1')
        must_be_none = User.get(ds, 'not_existing_user')
        self.assert_(must_be_none is None)
        # multi-field key
        member = GroupMember.get(ds, ('usr1','gr1'))
        self.assertEqual(member.username, 'usr1')
        none_member = GroupMember.get(ds, ('usr3', 'gr2'))
        self.assert_(none_member is None)
        # check access to separate schemas
        dsb = TestDataSet(self.con, 'ormb')
        self.assert_(User.get(ds, 'usra'))
        self.assert_(User.get(dsb, 'usra') is None)
    def testQuery(self):
        ds = self.dsa
        # test order desc
        usernames = [u.username for u in User.query(ds).order('-username')]
        self.assertEqual(usernames, 'usra usr4 usr3 usr2 usr1'.split(' '))
        # test filter
        users = [u.username for u in User.query(ds).filter('department', 'a')]
        users.sort()
        self.assertEqual(users, 'usr1 usr2 usr3'.split())
    def testInsert(self):
        ds = self.dsa
        user = User(ds, username='newuser1', full_name = 'new user 1', email='newuser1@example.com', department='a')
        user.save()
        self.assert_(User.query(ds).filter('username', 'newuser1').count() == 1)
    def testUpdate(self):
        ds = self.dsa
        user = User.get(ds, 'usra')
        self.assert_(user.email == 'usra@example.com')
        user.email = 'updated@email.address'
        # original record in database is not changed yet, and can be retrieved 
        self.assert_(User.get(ds, 'usra').email == 'usra@example.com')
        user.save()
        self.assert_(user.email == 'updated@email.address')
        self.assert_(User.get(ds, 'usra').email == 'updated@email.address')
        cnt = 0
        for ticket in Ticket.query(ds).filter('assigned', 'usr1'):
            ticket.assigned = 'usra'
            ticket.save()
            cnt += 1
        self.assert_(cnt == 2)
        self.assert_(Ticket.query(ds).filter('assigned', 'usra').count() == 4)
        self.assert_(Ticket.get(ds, 2).assigned == 'usra')
    def testDeleteModel(self):
        ds = self.dsa
        Ticket.get(ds, 2).delete()
        self.assert_(Ticket.get(ds, 2) is None)
        self.con.commit()
        self.assert_(Ticket.get(ds, 2) is None)
    def testDeleteQuery(self):
        ds = self.dsa
        Ticket.query(ds).filter('assigned', 'usr1').delete()
        self.assert_(Ticket.query(ds).count() == 2)
        self.assert_(Ticket.query(ds).filter('assigned', 'usr1').count() == 0)
    def testUnicode(self):
        ds = self.dsa
        ticket = Ticket.get(ds, 2)
        self.assert_(isinstance(ticket.state, unicode))
        ticket.subject = u'\u043f\u0440\u0438\u0432\u0435\u0442'
        ticket.save()
        ds.commit()
        t = Ticket.query(ds).filter('id', 2).fetchone()
        self.assert_(t.subject == u'\u043f\u0440\u0438\u0432\u0435\u0442')
    def testDefault(self):
        ds = self.dsa
        ticket = Ticket(ds, state='Open')
        self.assert_(ticket.priority == 3)
        self.assert_(ticket.id is None)
        ticket.id = 12
        ticket.subject = 'foobar'
        ticket.save()
        t1 = Ticket.get(ds, 12)
        self.assert_(t1.subject == 'foobar')
        self.assert_(t1.priority == 3)
        ds.commit()
        t2 = Ticket.get(ds, 12)
        self.assert_(t2.subject == 'foobar')
        self.assert_(t2.priority == 3)
    def testBeforeSave(self):
        ds = self.dsa
        ticket = Ticket(ds, state='Open', subject="unbar276")
        ticket.save()
        self.assert_(ticket.id is not None)
        self.assert_(Ticket.get(ds, ticket.id).subject == "unbar276")
        ticket.state = 'Bazz'
        self.assertRaises(VerifyError, ticket.save)
    def testIn(self):
        ds = self.dsa
        usernames = [u.username for u in User.query(ds).filter('username', ['usr1','usr2','usr4'])]
        self.assert_('usr1' in usernames)
        self.assert_('usr3' not in usernames)
    def testBatchUpdate(self):
        ds = self.dsa
        open = [t.id for t in Ticket.query(ds).filter('state', 'Open')]
        Ticket.query(ds).filter('state', 'Open').update({'state':'Closed'})
        closed = [t.id for t in Ticket.query(ds).filter('state', 'Closed')]
        self.assert_(set(open).issubset(set(closed)))
        self.assert_(Ticket.get(ds, open[0]).state == 'Closed')
    def testRawFilter(self):
        ds = self.dsa
        self.assert_(Ticket.query(ds).raw_filter('lower(state)=%(r1)s', dict(r1='open')).count() == 3)
    def testVirtualField(self):
        ds = self.dsa
        ticket = Ticket.get(ds, 1)
        self.assert_(ticket.v_state is None)
        self.assert_(ticket)
        self.assert_('v_state' not in ticket.data)
        self.assert_(Ticket.query(ds).filter(v_state='open').count() == 3)
    def testJoin(self):
        ds = self.dsa
        ticket = TicketUser.query(ds).filter(id=1).fetchone()
        self.assert_(ticket.username == u'usr1')
        self.assert_(ticket.full_name == u'user 1')
    def testModelInheritance(self):
        ds = self.dsa
        dir = Agent.get(ds, 'dir')
        self.assert_(dir.full_name == u'Director')
        self.assert_(dir.position == u'director')
        self.assert_(Agent.query(ds).filter(position='manager').count() == 1)

if __name__ == "__main__":
    #unittest.main()
    suite = unittest.makeSuite(AllTests)
    unittest.TextTestRunner(verbosity=2).run(suite)
