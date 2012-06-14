# Python ORM without magic

## Key features and limitations:

 - no caching on orm level.
 - no global connection. You need to specify connection on every request.
 - transparent multitenancy.
 - explicitly defined database schema in Django/GAE style.
 - one schema definition for different databases (Postgresql and in-memory mockup for unittests
   provided out of the box).
 - client-side triggers (before_save method of the model).

##Example

    import psycopg2
    from pyorm import Model, Property
    from pyorm import pg_datasource

    class User(Model):
        _table_name = 'user'
        _key = 'username'
        username = Property()
        email = Property()
        department = Property()

    con = psycopg2.connect("dbname=mydb")
    ds = pg_datasource.DataSet(con, schema="tenant1")

    # create new entity
    user = User(ds)
    user.username = 'John'
    user.email = 'john@company.com'
    user.department = 'dev'
    user.save()

    # create another entity, populate attributes in constructor)
    user1 = User(ds, username='Sarah', email='sarah@company.com', department='support').save()

    # get entity by primary key (_key attribute of the model)
    chief = User.get(ds, 'John')
    print chief.email

    # query
    for user in User.query(ds).filter(department='dev').order('username'):
        print user.email
