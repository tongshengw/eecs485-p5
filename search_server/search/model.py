"""Database model for search server."""

import pathlib
import sqlite3


def get_db():
    """Get database connection."""
    db_path = pathlib.Path("var/search.sqlite3")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def get_document(docid):
    """Get document by docid."""
    conn = get_db()
    cur = conn.execute(
        "SELECT docid, title, summary, url FROM documents WHERE docid = ?",
        (docid,),
    )
    row = cur.fetchone()
    conn.close()
    return row
