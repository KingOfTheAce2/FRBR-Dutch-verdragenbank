# Dutch Treaty Database - Verdragenbank Open Data

This project fetches daily data from the Verdragenbank SRU endpoint provided by the Dutch government and stores them as JSONL shards. The same crawler structure that was used for the Tuchtrecht repository is reused here for the treaties collection.

The crawler performs the following steps:

1. Saves the XML to this repository.
2. The GitHub Actions workflow copies the generated JSONL shards to a Hugging Face dataset.

## Setup

Use Python 3.11 which has pre-built wheels for all dependencies:

```bash
python3.11 -m pip install -r requirements.txt
```

## Daily Fetch Script

Run manually (the crawler processes up to 250 records per run by default):

```bash
python -m crawler.main
```

During execution the crawler prints each processed URL so progress is visible in
the GitHub Actions log.

Use `python -m crawler.main --reset` to ignore the last run timestamp and crawl the
entire backlog. The `--max-records` option controls how many records are
processed in a single run.

Each run appends new JSONL files under `data/`. The timestamp of the last
successful crawl is stored in `.last_update` so consecutive runs only fetch new
data.

Or add to cron to automate daily.

## Hugging Face

Set the following environment variables before running the fetch script:

* `HF_TOKEN` – an access token with write permissions
* `HF_DATASET_REPO` – Hugging Face dataset repository name
* `HF_PRIVATE` – set to `true` to create a private dataset (optional)

The dataset will be created under `HF_DATASET_REPO`, for example
`vGassen/Dutch-Open-Data-Verdragenbank`.

## GitHub Actions

A workflow is included to automate fetching. It runs every Sunday and can also
be triggered manually from the Actions tab. Configure the `HF_TOKEN` and
`HF_DATASET_REPO` secrets in your repository settings so the workflow can push
the latest JSONL shards to your Hugging Face dataset.
