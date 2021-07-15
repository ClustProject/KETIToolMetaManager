from . import app

app.log_setup()

def create_app():
    return app.app
