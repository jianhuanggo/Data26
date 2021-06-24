from sqlalchemy import and_, or_, not_, Integer, String
from sqlalchemy.exc import CompileError, DatabaseError, IntegrityError, DataError
from sqlalchemy.sql.expression import cast
from sqlalchemy import func
from sqlalchemy.sql import table, column


t1 = table('t1')
t2 = table('t2')
print(t1.insert().from_select(t2.select().where(t2.c.y == 5)))
