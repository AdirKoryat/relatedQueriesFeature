import datetime
import logging
from flask import Flask, render_template, request

from db.db_operations import get_value_by_key
from utils.constants import HTML_PATH, QUERY_PARAMETER
from utils.functions import serve_request

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@app.route('/')
def root() -> str:
    return render_template(HTML_PATH)


@app.route('/related')
def set_variable() -> str:
    query_name = request.args.get(QUERY_PARAMETER)
    start = datetime.datetime.now()
    output = serve_request(query_name)
    end = datetime.datetime.now()
    delta = end - start
    logger.info(f'call to db in {int(delta.total_seconds() * 1000)} milliseconds')
    logger.info(f'cache info: {get_value_by_key.cache_info()}')
    return render_template(HTML_PATH, output=output)


if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    # Flask's development server will automatically serve static files in
    # the "static" directory. See:
    # http://flask.pocoo.org/docs/1.0/quickstart/#static-files. Once deployed,
    # App Engine itself will serve those files as configured in app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)