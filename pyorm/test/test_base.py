
from pyorm.model import Model, Property
import unittest

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

def populate_dataset(ds):
    groups = [ 'gr1','gr2' ]
    for group in groups:
        Group(ds, groupname=group).save()

    users = [
                dict(username='usr1',full_name='user 1', email = 'usr1@example.com', department='a'),
                dict(username='usr2',full_name='user 2', email = 'usr2@example.com', department='a'),
                dict(username='usr3',full_name='user 3', email = 'usr3@example.com', department='a'),
                dict(username='usr4',full_name='user 4', email = 'usr4@example.com', department='b'),
            ]
    for user in users:
        User(ds, **user).save()

    agents = [
                dict(username='dir',full_name='Director', email = 'dir@example.com', department='b', position = 'director'),
                dict(username='manager',full_name='Manager', email = 'manager@example.com', department='b', position = 'manager'),
             ]
    for agent in agents:
        Agent(ds, **agent).save()

    group_members = {
                        'gr1': ['usr1','usr1','usr3'],
                        'gr2': ['usr1','usr4']
                    }
    for gr, members in group_members.items():
        for username in members:
            GroupMember(ds, groupname=gr, username = username).save()


class BaseTests(unittest.TestCase):
    def populate_dataset(self, ds):
        populate_dataset(ds)
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
        dsb = self.dsb
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
    def testVirtualField(self):
        ds = self.dsa
        ticket = Ticket.get(ds, 1)
        self.assert_(ticket.v_state is None)
        self.assert_(ticket)
        self.assert_('v_state' not in ticket.data)
        self.assert_(Ticket.query(ds).filter(v_state='open').count() == 3)
    def testModelInheritance(self):
        ds = self.dsa
        dir = Agent.get(ds, 'dir')
        self.assert_(dir.full_name == u'Director')
        self.assert_(dir.position == u'director')
        self.assert_(Agent.query(ds).filter(position='manager').count() == 1)

