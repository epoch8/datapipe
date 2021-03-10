#%%
import sqlalchemy as s
# %%
metadata = s.MetaData()

tbl = s.schema.Table(
    'test', metadata,
    s.schema.Column('id', s.Integer)
)

sql = s.sql.select([tbl]).where(tbl.c.id == [1,2,3])
# %%
str(sql)
# %%
sql.params()
# %%
