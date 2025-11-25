import math
import pathlib
import re
import flask
import index

@index.app.route("/api/v1/", methods=["GET"])
def get_index(): 
    return flask.jsonify({
        "hits": "/api/v1/hits/",
        "url": "/api/v1/"
    })


@index.app.route("/api/v1/hits/", methods=["GET"])
def hits(): 
    query = flask.request.args.get("q")
    weight = flask.request.args.get("w", 0.5, type=float)
    
    # Parse Query string 
    query = re.sub(r"[^a-zA-Z0-9 ]+", "", query)
    query = query.casefold()
    query_terms = query.split()
    
    # Filter stopwords
    terms = []
    for word in query_terms: 
        if word not in index.stopwords: 
            terms.append(word)
            
    # doc_id -> {score components}
    doc_scores = {}
    
    # q_vec: term -> weight
    q_vec = {}
    for term in terms:
        if term in index.inverted_index:
            idf = index.inverted_index[term]["idf"]
            tf = terms.count(term)
            q_vec[term] = tf * idf
        else:
            return flask.jsonify({"hits": []})
            
    q_norm = math.sqrt(sum(w**2 for w in q_vec.values()))
    
    if q_norm == 0:
        return flask.jsonify({"hits": []})

    # Process postings
    for term, q_val in q_vec.items():
        postings = index.inverted_index[term]["postings"]
        for posting in postings:
            doc_id = posting["doc_id"]
            doc_tf = posting["tf"]
            doc_norm = posting["norm"]
            idf = index.inverted_index[term]["idf"]
            
            # doc weight for this term
            d_val = doc_tf * idf
            
            if doc_id not in doc_scores:
                doc_scores[doc_id] = {"dot_product": 0.0, "doc_norm": doc_norm, "terms_found": 0}
            
            doc_scores[doc_id]["dot_product"] += q_val * d_val
            doc_scores[doc_id]["terms_found"] += 1
            
    # Final score calculation
    results = []
    num_terms = len(q_vec)
    for doc_id, data in doc_scores.items():
        
        if data["terms_found"] < num_terms:
            continue
            
        dot_product = data["dot_product"]
        doc_norm = data["doc_norm"]
        
        # Cosine similarity
        if doc_norm == 0:
            cosine = 0.0
        else:
            cosine = dot_product / (q_norm * doc_norm)
            
        # PageRank
        pagerank = index.pagerank.get(doc_id, 0.0)
        
        # Weighted score
        score = weight * pagerank + (1 - weight) * cosine
        
        results.append({
            "docid": doc_id,
            "score": score
        })
        
    # Sort by score descending
    results.sort(key=lambda x: x["score"], reverse=True)
    
    return flask.jsonify({"hits": results})


def load_index():
    # Load stopwords
    stopwords_path = pathlib.Path(index.app.root_path) / "stopwords.txt"
    index.stopwords = set()
    with open(stopwords_path, "r") as f:
        for line in f:
            index.stopwords.add(line.strip())
            
    # Load pagerank
    pagerank_path = pathlib.Path(index.app.root_path) / "pagerank.out"
    index.pagerank = {}
    with open(pagerank_path, "r") as f:
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

    with open(index_path, "r") as f:
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
                tf = int(parts[i+1])
                norm = float(parts[i+2])
                postings.append({
                    "doc_id": doc_id,
                    "tf": tf,
                    "norm": norm
                })
                i += 3
                
            index.inverted_index[term] = {
                "idf": idf,
                "postings": postings
            }