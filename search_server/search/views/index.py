"""Index view for search server."""

import threading
import urllib.parse

import flask
import requests
import search
import search.model


@search.app.route("/", methods=["GET"])
def show_index():
    """Display search page with results."""
    query = flask.request.args.get("q", "")
    weight = flask.request.args.get("w", "0.5", type=str)

    # Convert weight to float, default to 0.5
    try:
        weight_float = float(weight)
    except (ValueError, TypeError):
        weight_float = 0.5

    # Ensure weight is between 0 and 1
    weight_float = max(0.0, min(1.0, weight_float))
    weight = str(weight_float)

    results = []
    if query:
        # Make concurrent requests to all index servers
        api_urls = search.app.config["SEARCH_INDEX_SEGMENT_API_URLS"]
        threads = []
        thread_results = {}

        def fetch_hits(url, thread_id):
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
            except Exception:
                thread_results[thread_id] = []

        # Start threads for concurrent requests
        for i, url in enumerate(api_urls):
            thread = threading.Thread(target=fetch_hits, args=(url, i))
            thread.start()
            threads.append(thread)

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Merge results from all index servers using heapq.merge
        all_hits = []
        for thread_id in sorted(thread_results.keys()):
            all_hits.append(thread_results[thread_id])

        # Merge sorted lists by score (descending)
        merged = []
        for hits_list in all_hits:
            merged.extend(hits_list)

        # Sort by score descending
        merged.sort(key=lambda x: x.get("score", 0), reverse=True)

        # Get top 10 results
        top_hits = merged[:10]

        # Fetch document details from database
        for hit in top_hits:
            docid = hit.get("docid")
            if docid:
                doc = search.model.get_document(docid)
                if doc:
                    url = doc["url"]
                    # First decode to get canonical form
                    url_decoded = urllib.parse.unquote(url)
                    # Then encode for href (percent-encoded)
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

    context = {"query": query, "weight": weight, "results": results}
    return flask.render_template("index.html", **context)
