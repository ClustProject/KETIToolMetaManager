import click
from flask import current_app, g
from flask.cli import with_appcontext
from KETIToolMetaManager.mongo_management import mongo_crud as mg
from influxdb import InfluxDBClient

def get_mongo_db(db_info=None):
    if(db_info==None and 'mongodb' not in g) :
        g.mongodb = mg.MongoCRUD(current_app.config["MONGO_DB_INFO"])

    return g.mongodb

def get_influx_db(db_info=None):
    if(db_info==None and 'influxdb' not in g) :
        info = current_app.config["INFLUX_DB_INFO"]
        g.influxdb = InfluxDBClient(info["host_"],info["port_"],info["user_"],info["pass_"])
    
    return g.influxdb

def close_db(e=None):
    mongodb = g.pop('mongodb',None)
    if mongodb is not None:
        mongodb.close()
    influxdb = g.pop('influxdb',None)
    # if influxdb is not None:
    #     influxdb.close()

def init_db():
    mongodb = get_mongo_db()
    influxdb = get_influx_db()

@click.command('init-db')
@with_appcontext
def init_db_command():
    """
    Clear the existing data and create new tables
    """
    init_db()
    click.echo("Initialized the database")
    
def init_app(app):
    # tells Flask to call that function 
    # when cleaning up after returning the response.
    #init_db(app.config["MONGO_DB_INFO"])
    app.teardown_appcontext(close_db)
    app.cli.add_command(init_db_command)
    # adds a new command that can be called with the flask command.
    #app.cli.add_command(init_db_command)