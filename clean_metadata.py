"""   Clean the Kaggle arXiv metadata snapshot into a Parquet dataset, tagged by field

Use case:
        python clean_metadata.py arxiv-metadata-oai-snapshot.json arxiv_clean.parquet

Reads the JSON-lines file one record at a time (it's 5GB, so can't load it whole)
and writes the cleaned data to Parquet in batches.

arXiv is basically all STEM, so by default this keeps every field. To drop one,
remove it from INCLUDE below (e.g. drop "econ" if it doesn't count).
"""

import json
import sys
# Handy tool for tallying items (like counting how many papers belong to each field)
from collections import Counter
from datetime import datetime

# Formats and organizes data into clean columns in memory (RAM)
import pyarrow as pa
# Compresses and saves that organized data into a .parquet file on disk
import pyarrow.parquet as pq

# paper is kept if ANY of its categories is in one.
INCLUDE = {"physics", "math", "cs", "stat", "qbio", "qfin", "eess", "econ"}

# Translates old arXiv archives and category prefixes into broad field names (e.g. "hep-th" -> "physics")
ARCHIVE_TO_FIELD = {
    # physics
    "astro-ph": "physics", "cond-mat": "physics", "gr-qc": "physics", "hep-ex": "physics",
    "hep-lat": "physics", "hep-ph": "physics", "hep-th": "physics", "math-ph": "physics",
    "nlin": "physics", "nucl-ex": "physics", "nucl-th": "physics", "physics": "physics",
    "quant-ph": "physics",
    "acc-phys": "physics", "ao-sci": "physics", "atom-ph": "physics", "chem-ph": "physics",
    "plasm-ph": "physics", "supr-con": "physics", "mtrl-th": "physics",
    "adap-org": "physics", "chao-dyn": "physics", "comp-gas": "physics",
    "patt-sol": "physics", "solv-int": "physics",
    # math
    "math": "math", "alg-geom": "math", "dg-ga": "math", "funct-an": "math", "q-alg": "math",
    # cs
    "cs": "cs", "cmp-lg": "cs",
    # stats
    "stat": "stat", "bayes-an": "stat",
    # the rest
    "q-bio": "qbio", "q-fin": "qfin", "eess": "eess", "econ": "econ"
}

# The master blueprint: defines the column names and data types for the output Parquet file
# (names here must match the keys in rows.append below exactly)
SCHEMA = pa.schema([
    ("arxiv_id", pa.string()),  # data in this col must be stored as plain text
    ("title", pa.string()),
    ("abstract", pa.string()),
    ("categories", pa.list_(pa.string())),  # all categories
    ("primary_category", pa.string()),  # single main category
    ("fields", pa.list_(pa.string())),  # eg: "cs", "math", "stat"
    ("primary_field", pa.string()),  # eg: "cs"
    ("submitted", pa.date32()),  # date of v1
    ("updated", pa.date32()),  # snapshot's update_date
    # digital object identifier, unique permanent web link, citation matching
    ("doi", pa.string()),
    ("journal_ref", pa.string()),
])

BATCH = 50_000  # process in chunks of 50k to avoid running out of RAM


def field_of(cat):
    # Extracts the prefix before any dot and returns its broad field name "cs.AI" -> "cs"
    return ARCHIVE_TO_FIELD.get(cat.split(".")[0])


def clean(s):
    # Strips extra whitespace, newlines, and tabs into clean single-spaced text (returns None if empty)
    return " ".join(s.split()) if s else None


def v1_date(versions):
    try:
        # e.g. "Mon, 2 Apr 2007 19:18:42 GMT"
        return datetime.strptime(versions[0]["created"], "%a, %d %b %Y %H:%M:%S %Z").date()
    except (KeyError, IndexError, ValueError, TypeError):
        return None


def iso_date(s):
    # Converting "2008-11-26" text -> a real date
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def main(inp, out):
    writer = pq.ParquetWriter(out, SCHEMA, compression="zstd")
    rows, n_in, n_out = [], 0, 0
    counts, unknown = Counter(), Counter()

    with open(inp, encoding="utf-8-sig") as f:
        for line in f:
            n_in += 1
            if n_in % 500_000 == 0:
                print(f"read {n_in:,} kept {n_out:,}", flush=True)

            r = json.loads(line)
            cats = r.get("categories", "").split()

            fields = set()
            for c in cats:
                fld = field_of(c)
                if fld is None:
                    unknown[c.split(".")[0]] += 1
                else:
                    fields.add(fld)

            # everything below runs once per paper, not once per category
            if not fields & INCLUDE:
                continue

            rows.append({
                "arxiv_id": r["id"],
                "title": clean(r.get("title")),
                "abstract": clean(r.get("abstract")),
                "categories": cats,
                "primary_category": cats[0] if cats else None,
                "fields": sorted(fields),
                "primary_field": field_of(cats[0]) if cats else None,
                "submitted": v1_date(r.get("versions")),
                "updated": iso_date(r.get("update_date")),
                "doi": r.get("doi"),
                "journal_ref": clean(r.get("journal-ref")),
            })
            counts.update(fields)
            n_out += 1
            if len(rows) >= BATCH:
                writer.write_table(pa.Table.from_pylist(rows, SCHEMA))
                rows = []

    if rows:
        writer.write_table(pa.Table.from_pylist(rows, SCHEMA))
    writer.close()

    print(f"\ndone: read {n_in:,}, kept {n_out:,} -> {out}")
    for k, v in counts.most_common():
        print(f"  {k:<8} {v:,}")

    print("(cross-listed papers count in every field they touch, so these overlap)")
    if unknown:
        print(
            "unmapped archives (add to ARCHIVE_TO_FIELD if you want them):", dict(unknown))


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("usage: python clean_metadata.py <snapshot.json> <out.parquet>")
    main(sys.argv[1], sys.argv[2])