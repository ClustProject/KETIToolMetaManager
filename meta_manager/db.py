import click
from flask import current_app, g
from flask.cli import with_appcontext
from KETIToolMetaManager.mongo_management import mongo_crud as mg

def get_db(db_info):

    if 'db' not in g:
        g.db=mydb = mg.MongoCRUD(db_info)
    
    return g.db

def close_db(e=None):
    db = g.pop('db',None)
    if db is not None:
        db.close()

def init_db():
    db = get_db()