# Dutch Treaty Database - Verdragenbank Open Data

This project provides a simple crawler for the Dutch "Verdragenbank" SRU endpoint hosted at `https://repository.overheid.nl/sru`.  The crawler collects records from the `vd` product area and uploads them as JSONL files to a Hugging Face dataset.

## Setup

Install the dependencies with Python 3.11 or later:

```bash
python3.11 -m pip install -r requirements.txt
```

## Running the crawler

The crawler can be executed manually.  It processes up to 250 records per run by default:

```bash
python crawler.py
```

Use the `--reset` flag to ignore any incremental state and crawl the full backlog.  The `--max-records` option controls how many records are processed during a single run.

The following environment variables must be set:

* `HF_TOKEN` – access token with write permissions.
* `HF_DATASET_REPO` – name of the Hugging Face dataset repository to push data to.
* `HF_PRIVATE` – set to `true` to create the dataset as private (optional).

Every run uploads a new JSONL shard directly to the configured Hugging Face dataset.  No data is stored locally.

## GitHub Actions

A workflow is provided to automate crawling every Sunday.  Configure the `HF_TOKEN` and `HF_DATASET_REPO` secrets in your repository so the workflow can push the shards directly to your Hugging Face dataset.

## License

This project is released under the [MIT License](LICENSE).
