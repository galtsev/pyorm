
from model import Model, Property
from memory_datasource import DataSet

class User(Model):
    _key = 'username'
    _table_name = 'user'
    username = Property()
    email = Property()
    position = Property()
    department = Property()

data = [
    ('director','director@example.com', 'director','office'),
    ('manager', 'manager@example.com', 'manager', 'office'),
    ('manager2', 'manager2@example.com', 'manager', 'office'),
    ('stockkeeper', 'stockkeeper@example.com', 'stockkeeper', 'stock')
]

def run():
    ds = DataSet('')
    for username, email, position, dep in data:
        user = User(ds)
        user.username = username
        user.email = email
        user.position = position
        user.department = dep
        user.save()
    print User.get(ds, 'manager').email
    for user in User.query(ds).filter(department='office'):
        print user.username

run()
