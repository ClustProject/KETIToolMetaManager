#from . import app
#app.log_setup()
from flask import Flask, g
import logging
import json
# /home/teamgold/KETI/CLUST/CLUSTPROJECT/instance

def log_setup():
    formatter = logging.Formatter('%(asctime)s - %(message)s')
    termlog_handler = logging.StreamHandler()
    termlog_handler.setFormatter(formatter)
    logger = logging.getLogger()
    logger.addHandler(termlog_handler)
    logger.setLevel(logging.INFO)

def create_app(config=None):
    log_setup()
    app = Flask(__name__,instance_relative_config=True)

    app.config.from_mapping(
        SECRET_KEY='dev'
    )

    if config is None:
        with open('./config.json') as f:
            config = json.load(f)

        app.config.update(config)
    else:
        app.config.from_mapping(config)
    
    from . import db
    db.init_app(app)
    #print(getattr(g, 'db', '111'))
    #print(db.get_db())
    #print(app.config)
    from . import meta_data
    app.register_blueprint(meta_data.bp)


    return app
