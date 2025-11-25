"""Search index view."""

import json
import threading
import urllib.parse

import flask
import requests
import requests.exceptions
import search
import search.model


def _validate_weight(weight_str):
    """Validate and convert the weight to a float."""
    try:
        weight_float = float(weight_str)
    except (ValueError, TypeError):
        weight_float = 0.5
    return max(0.0, min(1.0, weight_float))


def _fetch_and_merge_hits(query, weight_float, api_urls):
    """Fetch hits concurrently from all index servers and merge them."""
    thread_results = {}
    threads = []

    def fetch_hits_thread_target(url, thread_id):
        """Fetch hits from a single index server."""
        try:
            response = requests.get(
                url, params={"q": query, "w": weight_float}, timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                thread_results[thread_id] = data.get("hits", [])
            else:
                thread_results[thread_id] = []
        except (
            requests.exceptions.RequestException,
            json.JSONDecodeError,
        ):
            thread_results[thread_id] = []

    for i, url in enumerate(api_urls):
        thread = threading.Thread(
            target=fetch_hits_thread_target, args=(url, i)
        )
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    merged_hits = []
    for thread_id in sorted(thread_results.keys()):
        merged_hits.extend(thread_results[thread_id])

    merged_hits.sort(key=lambda x: x.get("score", 0), reverse=True)
    return merged_hits[:10]


def _get_document_details_for_hits(top_hits):
    """Fetch document details for a list of top hits."""
    results = []
    for hit in top_hits:
        docid = hit.get("docid")
        if docid:
            doc = search.model.get_document(docid)
            if doc:
                url = doc["url"]
                url_decoded = urllib.parse.unquote(url)
                url_encoded = urllib.parse.quote(
                    url_decoded, safe=":/?#[]@!$&'()*+,;="
                )
                results.append(
                    {
                        "docid": doc["docid"],
                        "title": doc["title"],
                        "summary": (
                            doc["summary"]
                            if doc["summary"]
                            else "No summary available"
                        ),
                        "url_encoded": url_encoded,
                        "url_decoded": url_decoded,
                    }
                )
    return results


@search.app.route("/", methods=["GET"])
def show_index():
    """Display search page with results."""
    query = flask.request.args.get("q", "")
    weight_str = flask.request.args.get("w", "0.5", type=str)

    weight_float = _validate_weight(weight_str)
    weight = str(weight_float)

    results = []
    if query:
        api_urls = search.app.config["SEARCH_INDEX_SEGMENT_API_URLS"]
        top_hits = _fetch_and_merge_hits(query, weight_float, api_urls)
        results = _get_document_details_for_hits(top_hits)

    context = {"query": query, "weight": weight, "results": results}
    return flask.render_template("index.html", **context)
