"""Api main."""

import math
import pathlib
import re

import flask
import index


@index.app.route("/api/v1/", methods=["GET"])
def get_index():
    """Get index."""
    return flask.jsonify({"hits": "/api/v1/hits/", "url": "/api/v1/"})


def _parse_query_and_filter_stopwords(query):
    """Parse and filter stopwords from the query."""
    query = re.sub(r"[^a-zA-Z0-9 ]+", "", query)
    query = query.casefold()
    query_terms = query.split()

    terms = [
        word for word in query_terms if word not in index.stopwords
    ]
    return terms


def _calculate_query_vector(terms):
    """Calculate the query vector."""
    q_vec = {}
    for term in terms:
        if term not in index.inverted_index:
            return None

        idf = index.inverted_index[term]["idf"]
        tf = terms.count(term)
        q_vec[term] = tf * idf
    return q_vec


def _process_postings(q_vec):
    """Process postings to calculate dot products."""
    doc_scores = {}
    for term, q_val in q_vec.items():
        postings = index.inverted_index[term]["postings"]
        for posting in postings:
            doc_id = posting["doc_id"]
            if doc_id not in doc_scores:
                doc_scores[doc_id] = {
                    "dot_product": 0.0,
                    "doc_norm": posting["norm"],
                    "terms_found": 0,
                }
            idf = index.inverted_index[term]["idf"]
            d_val = posting["tf"] * idf
            doc_scores[doc_id]["dot_product"] += q_val * d_val
            doc_scores[doc_id]["terms_found"] += 1
    return doc_scores


def _calculate_final_scores_and_sort(doc_scores, q_vec, weight):
    """Calculate final scores and sort the results."""
    q_norm = math.sqrt(sum(w**2 for w in q_vec.values()))
    if q_norm == 0:
        return []

    results = []
    num_terms = len(q_vec)
    for doc_id, data in doc_scores.items():
        if data["terms_found"] < num_terms:
            continue

        doc_norm = data["doc_norm"]
        if doc_norm == 0:
            cosine = 0.0
        else:
            dot_product = data["dot_product"]
            cosine = dot_product / (q_norm * doc_norm)

        pagerank = index.pagerank.get(doc_id, 0.0)
        score = weight * pagerank + (1 - weight) * cosine

        results.append({"docid": doc_id, "score": score})

    results.sort(key=lambda x: x["score"], reverse=True)
    return results


@index.app.route("/api/v1/hits/", methods=["GET"])
def hits():
    """Find hits."""
    query = flask.request.args.get("q")
    weight = flask.request.args.get("w", 0.5, type=float)

    terms = _parse_query_and_filter_stopwords(query)
    q_vec = _calculate_query_vector(terms)

    if not q_vec:
        return flask.jsonify({"hits": []})

    doc_scores = _process_postings(q_vec)

    results = _calculate_final_scores_and_sort(
        doc_scores, q_vec, weight
    )

    return flask.jsonify({"hits": results})


def load_index():
    """Load index."""
    # Load stopwords
    stopwords_path = pathlib.Path(index.app.root_path) / "stopwords.txt"
    index.stopwords = set()
    with open(stopwords_path, "r", encoding="utf-8") as f:
        for line in f:
            index.stopwords.add(line.strip())

    # Load pagerank
    pagerank_path = pathlib.Path(index.app.root_path) / "pagerank.out"
    index.pagerank = {}
    with open(pagerank_path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(",")
            if len(parts) == 2:
                index.pagerank[int(parts[0])] = float(parts[1])

    # Load inverted index
    index_path = index.app.config["INDEX_PATH"]
    index.inverted_index = {}

    if not pathlib.Path(index_path).exists():
        print(f"Index file not found at {index_path}")
        return

    with open(index_path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) < 2:
                continue

            term = parts[0]
            idf = float(parts[1])

            postings = []
            # Postings are triplets: doc_id tf norm
            i = 2
            while i < len(parts):
                doc_id = int(parts[i])
                tf = int(parts[i + 1])
                norm = float(parts[i + 2])
                postings.append({"doc_id": doc_id, "tf": tf, "norm": norm})
                i += 3

            index.inverted_index[term] = {"idf": idf, "postings": postings}
