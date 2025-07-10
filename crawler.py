import os
import sys
from pathlib import Path
from datetime import datetime, timezone
import jsonlines
import argparse
import requests
import re
import xml.etree.ElementTree as ET
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Iterator, Dict, Any, Optional
from tempfile import NamedTemporaryFile
from huggingface_hub import HfApi, HfFolder

# --- From utils.py ---
def get_session() -> requests.Session:
    """Return a requests session with retry policy for transient errors."""
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

# Global session for HTTP requests
_SESSION = get_session()

# --- From sru_client.py ---
# The Verdragenbank dataset is served under the `vd` product area. The SRU
# service itself is available at `https://repository.overheid.nl/sru`. Specifying
# the product area in the query ensures the correct collection is returned.
BASE_URL = "https://repository.overheid.nl/sru"
PAGE_SIZE = 100 # As per SRU documentation, max is 1000, but we'll use a smaller size

def get_records(query: str, start_date: str = None) -> Iterator[Dict[str, Any]]:
    """
    Fetches records from the SRU endpoint using pagination.

    Args:
        query: The base CQL query.
        start_date: An optional ISO 8601 date string to get modified records.

    Yields:
        A dictionary representing a single record from the SRU response.
    """
    start_record = 1

    if start_date:
        query = f"({query}) AND dt.modified>={start_date}"

    while True:
        params = {
            'operation': 'searchRetrieve',
            'version': '2.0',
            'query': query,
            'startRecord': start_record,
            'maximumRecords': PAGE_SIZE,
            'recordSchema': 'gzd',
            'httpAccept': 'application/xml',
        }

        try:
            response = _SESSION.get(BASE_URL, params=params)
            response.raise_for_status()

            import xmltodict
            data = xmltodict.parse(response.content)

            search_retrieve_response = data.get('srw:searchRetrieveResponse', {})
            records = search_retrieve_response.get('srw:records', {}).get('srw:record', [])

            if not records:
                break

            # If there's only one record, it's not in a list
            if not isinstance(records, list):
                records = [records]

            for record in records:
                yield record

            start_record += PAGE_SIZE

        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from SRU endpoint: {e}")
            break
        except Exception as e:
            print(f"An error occurred while processing SRU response: {e}")
            break

# --- From parser.py ---
def get_full_text(url: str) -> Optional[str]:
    """
    Fetches the full text content from a given URL using a session with retry.

    Args:
        url: The URL of the XML file.

    Returns:
        The extracted full text as a string, or None if fetching fails.
    """
    try:
        response = _SESSION.get(url)
        response.raise_for_status()
        # We assume the content is XML and needs parsing to extract text.
        # This is a simple text extraction. More complex XML structures might need a more robust parser.
        root = ET.fromstring(response.content)
        # Concatenate all text from all elements
        return "".join(root.itertext())
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch full text from {url}: {e}")
    except ET.ParseError as e:
        print(f"Failed to parse XML from {url}: {e}")
    return None

def parse_record(record: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """
    Parses a single SRU record to extract URL, content, and source.

    Args:
        record: A dictionary representing a single SRU record.

    Returns:
        A dictionary with "URL", "Content", and "Source", or None if parsing fails.
    """
    try:
        record_data = record.get('srw:recordData', {}).get('gzd:gzd', {})
        enriched_data = record_data.get('gzd:enrichedData', {})

        # Prefer XML URL for full text extraction
        item_urls = enriched_data.get('gzd:itemUrl', [])
        if not isinstance(item_urls, list):
            item_urls = [item_urls]

        # Prefer Dutch XML (xml-nl) if available, otherwise fall back to plain XML
        xml_url = None
        for item in item_urls:
            if item.get('@manifestation') == 'xml-nl':
                xml_url = item.get('#text')
                break
        if not xml_url:
            for item in item_urls:
                if item.get('@manifestation') == 'xml':
                    xml_url = item.get('#text')
                    break

        pdf_url = None
        if not xml_url:
            for item in item_urls:
                if item.get('@manifestation') == 'pdf':
                    pdf_url = item.get('#text')
                    break

        target_url = xml_url or pdf_url or enriched_data.get('gzd:url')

        if not target_url:
            return None

        content = get_full_text(target_url) if xml_url else "Content from non-XML source, e.g., PDF, not extracted."

        if not content:
            return None

        return {
            "URL": target_url,
            "Content": content,
            "Source": "Verdragenbank"
        }
    except Exception as e:
        print(f"Error parsing record: {e}")
        return None

# --- From scrubber.py ---
# Patterns for common titles followed by personal names (e.g. "mr. Jansen")
_TITLE_NAME_PATTERN = re.compile(
    r"(?i)\b(mr\.?|prof\.?|dr\.?|ir\.?)\s+((?:[A-Z]\.)+\s*)?"
    r"[A-ZÀ-ÖØ-öø-ÿ][A-Za-zÀ-ÖØ-öø-ÿ.'`-]+(?:\s+[A-ZÀ-ÖØ-öø-ÿ][A-Za-zÀ-ÖØ-öø-ÿ.'`-]+){0,2}"
)

# "klager" and "verweerder" parties followed by a name
_PARTY_PATTERN = re.compile(
    r"(?i)\b(klager|verweerder)\s+((?:[A-Z]\.)?\s*[A-ZÀ-ÖØ-öø-ÿ][A-Za-zÀ-ÖØ-öø-ÿ.'`-]+)"
)

# Courtesy titles such as "de heer" or "mevrouw" followed by a name
_COURTESY_PATTERN = re.compile(
    r"(?i)\b(de\s+heer|mevrouw|mevr\.?)\s+((?:[A-Z]\.)+\s*)?"
    r"[A-ZÀ-ÖØ-öø-ÿ][A-Za-zÀ-ÖØ-öø-ÿ.'`-]+(?:\s+[A-ZÀ-ÖØ-öø-ÿ][A-Za-zÀ-ÖØ-öø-ÿ.'`-]+){0,2}"
)

# Simple gemachtigde pattern: match a few tokens following the keyword
_GEMACHTIGDE_PATTERN = re.compile(
    r"(?i)(gemachtigde[^\n]{0,10}(?:mr\.\s*)?)((?:[A-Za-zÀ-ÖØ-öø-ÿ.'`-]+\s*){1,5})"
)

def scrub_title_names(text: str) -> str:
    """Replace titles followed by names with a placeholder."""
    if not text:
        return text
    return _TITLE_NAME_PATTERN.sub(lambda m: f"{m.group(1)} NAAM", text)

def scrub_party_names(text: str) -> str:
    """Replace 'klager' or 'verweerder' names with a placeholder."""
    if not text:
        return text
    return _PARTY_PATTERN.sub(lambda m: f"{m.group(1)} NAAM", text)

def scrub_courtesy_names(text: str) -> str:
    """Replace courtesy titles followed by names with a placeholder."""
    if not text:
        return text
    return _COURTESY_PATTERN.sub(lambda m: f"{m.group(1)} NAAM", text)

def scrub_gemachtigde_names(text: str) -> str:
    """Replace names following 'gemachtigde' with 'NAAM'."""
    if not text:
        return text
    return _GEMACHTIGDE_PATTERN.sub(lambda m: f"{m.group(1)}NAAM", text)

def scrub_text(text: str) -> str:
    """Apply all available name scrubbing rules."""
    if not text:
        return text
    text = scrub_title_names(text)
    text = scrub_party_names(text)
    text = scrub_courtesy_names(text)
    text = scrub_gemachtigde_names(text)
    return text

# --- From main.py ---
# Main entry point for the Verdragenbank crawler.

# Ensure the package is importable when executed directly as a script.
# (This part is less relevant for a single file, but kept for context if this were to be split again)
ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

BASE_QUERY = "c.product-area==vd"
DEFAULT_MAX_RECORDS = 250


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Run the Verdragenbank crawler")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Ignore the last update timestamp and crawl the full backlog",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=DEFAULT_MAX_RECORDS,
        help="Maximum number of records to process in a single run",
    )
    return parser.parse_args()


def main() -> None:
    """Main function to run the crawler."""
    args = parse_args()

    if args.reset:
        print("Reset flag specified - fetching full backlog.")

    print(f"Maximum records this run: {args.max_records}")

    records_iterator = get_records(BASE_QUERY, start_date=None)

    hf_token = os.environ.get("HF_TOKEN") or HfFolder.get_token()
    dataset_repo = os.environ.get("HF_DATASET_REPO")
    if not dataset_repo:
        raise RuntimeError("HF_DATASET_REPO environment variable is required")
    private = os.environ.get("HF_PRIVATE", "false").lower() == "true"

    api = HfApi()
    api.create_repo(repo_id=dataset_repo, repo_type="dataset", token=hf_token, private=private, exist_ok=True)

    processed_count = 0
    with NamedTemporaryFile("w+", delete=False) as tmp:
        writer = jsonlines.Writer(tmp)
        for record in records_iterator:
            parsed = parse_record(record)
            if parsed:
                parsed["Content"] = scrub_text(parsed["Content"])
                writer.write(parsed)
                processed_count += 1
                print(f"Saved record {processed_count}: {parsed['URL']}")
                if processed_count >= args.max_records:
                    print(
                        f"Reached max-records limit ({args.max_records}). Stopping early."
                    )
                    break
        writer.close()
        tmp_path = tmp.name

    if processed_count > 0:
        shard_name = f"verdragenbank_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.jsonl"
        api.upload_file(
            path_or_fileobj=tmp_path,
            path_in_repo=shard_name,
            repo_id=dataset_repo,
            repo_type="dataset",
            token=hf_token,
        )
        print(f"Uploaded {shard_name} with {processed_count} records to {dataset_repo}.")
        os.unlink(tmp_path)

    print(f"Processed {processed_count} records.")


if __name__ == "__main__":
    main()
