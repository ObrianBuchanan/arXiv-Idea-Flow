# arXiv Idea Flow

Dataset of arXiv paper metadata (titles, abstracts, dates, categories) across STEM fields,
with citation links to be added from Semantic Scholar.

## Data source
- arXiv metadata snapshot from Kaggle: https://www.kaggle.com/datasets/Cornell-University/arxiv
- Version: 1
- Downloaded: Oct 27, 2026

The raw data isn't in this repo because it's too big. Download it from the link above
and put `arxiv-metadata-oai-snapshot.json` in the project folder.

## Steps
1. Clean the metadata:
   python clean_metadata.py arxiv-metadata-oai-snapshot.json arxiv_clean.parquet

   Output: ~3.2M papers with arxiv_id, title, abstract, categories, primary_category,
   fields, primary_field, submitted, updated, doi, journal_ref.

2. Citations: coming next (Semantic Scholar Datasets)

## Requirements
- Python 3, pyarrow, pandas
