import flask 
import index

app = flask.Flask(__name__)

@app.route("/api/v1/", methods=["GET"])
def index(): 
    return flask.jsonify({
        "hits": "/api/v1/hits/",
        "url": "/api/v1/"
    })


@app.route("/api/v1/hits/", methods=["GET"])
def hits(): 
    query = request.args.get("q")
    weight = request.args.get("w")



def load_index():
    # TODO: load inverted index, stopwords, and pagerank into memory
    pass