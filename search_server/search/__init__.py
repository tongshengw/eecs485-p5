"""Search server Flask application."""

import flask
import search.config

app = flask.Flask(__name__)
app.config.from_object(search.config)

# Import views to register routes
import search.views  # noqa: E402, F401, pylint: disable=wrong-import-position
