#from . import app
#app.log_setup()
from flask import Flask
import os 
import json
# /home/teamgold/KETI/CLUST/CLUSTPROJECT/instance
def create_app(config=None):
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
    
    print(app.config)
    return app
