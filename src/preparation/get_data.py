import time
import json
import logging
import pathlib
import argparse
from datetime import datetime, timedelta, timezone
from functools import partial, wraps
from itertools import pairwise

import requests
import pandas as pd
from tqdm import tqdm


API_ENDPOINT = "https://api.pushshift.io/reddit/search/comment/"
QUERY_STRING = "-moscopole -moscone mosco*|kremlin*|russia*|putin*"
SUBREDDIT = "europe"
COMMENTS_FILENAME = "comments.jsonl"
METADATA_FILENAME = "metadata.json"


class TqdmLoggingHandler(logging.StreamHandler):
    """Logging stream-like handler to make sure logging works with tqdm
    https://github.com/tqdm/tqdm/issues/193
    """

    def __init__(self):
        logging.StreamHandler.__init__(self)

    def emit(self, record):
        msg = self.format(record)
        tqdm.tqdm.write(msg)


def timeit(f):
    @wraps(f)
    def wrap(*args, **kwargs):
        t0 = time.time()
        result = f(*args, **kwargs)
        t1 = time.time()
        print(("Function {f_name} args:[{args}, {kwargs}] took: {time:.2f} secs."
                   .format(f_name=f.__name__, args=args, kwargs=kwargs, time=t1-t0)))
        return result
    return wrap


def send_request(parameters, timeout=10, endpoint=API_ENDPOINT):
    response = requests.get(endpoint, params=parameters)
    
    if response.status_code == 200:
        response = response.json()
        metadata = response["metadata"] if "metadata" in response.keys() else None
        data = response["data"]
        return (metadata, data)
    
    elif response.status_code == 429:
        print(f"Rate limit reached, sleeping for {timeout} secs.")
        time.sleep(timeout)
        return send_request(parameters)
    
    elif response.status_code >= 500:
        print(f"Server error (HTTP {response.status_code}), sleeping for {timeout} secs.")
        time.sleep(timeout)
        return send_request(parameters)
    
    else:
        raise NotImplementedError("HTTP status code {}".format(response.status_code))


def format_query_params(after, before=None, include_metadata=False,
                        query_str=QUERY_STRING, subreddit=SUBREDDIT, size=300):
    params = {
        "q": query_str, 
        "subreddit": subreddit,
        "metadata": include_metadata, 
        "size":size,
        "after": after
    }
    if before is not None:
        params["before"] = before
    return params


@timeit   
def get_daily_comments(after_datetime, before_datetime):
    after_timestamp = int(after_datetime.timestamp())
    before_timestamp = int(before_datetime.timestamp())
    
    get_next_query_params = partial(format_query_params, before=before_timestamp)
    params = get_next_query_params(after=after_timestamp, include_metadata=True)
    
    
    metadata, comments = send_request(params)
    iteration = 0
    print(f"#{iteration+1}: {len(comments)} ({len(comments)}/{metadata['total_results']})")

    while True:
        params = get_next_query_params(after=int(comments[-1]["created_utc"]))
        _, data = send_request(params)
        
        if bool(data):
            comments += list(data)
            iteration += 1
            print(f"#{iteration+1}: {len(data)} ({len(comments)}/{metadata['total_results']})")
        else:
            break
    
    return metadata, comments


def main(start_date, end_date=None, force_rewrite=False,
         dest_folder=pathlib.Path("data/comments/raw")):

    # Generating a list of days beween the start date and the end date
    if end_date is None:
        date_range = pd.date_range(start_date, periods=2, tz=timezone.utc)
    else:
        date_range = pd.date_range(start_date, end_date)


    for i, (after_datetime, before_datetime) in enumerate(pairwise(date_range)):

        # Skip if data for this date already exists and should not be overwritten
        target_folder = dest_folder.joinpath(after_datetime.strftime("%Y-%m-%d"))
        if target_folder.exists() and target_folder.is_dir() and not force_rewrite:
            continue

        metadata, comments = get_daily_comments(after_datetime, before_datetime)

        # Write data to disk
        target_folder.mkdir(parents=True, exist_ok=True)

        comments_file = target_folder.joinpath(COMMENTS_FILENAME)
        with open(comments_file, "w", encoding="utf8") as fp:
            for entry in comments:
                json.dump(entry, fp, ensure_ascii=False)
                fp.write("\n")
            
        metadata_file = target_folder.joinpath(METADATA_FILENAME)
        with open(metadata_file, "w", encoding="utf8") as fp:
            json.dump(metadata, fp, ensure_ascii=False, indent="\t")

        if i > 1:
            break


def date_str_to_ISO_8601(date_str):
    try:
        return datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
    except ValueError:
        msg = "Date must be specified in \"YYYY-MM-DD\" format."
        raise argparse.ArgumentTypeError(msg)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""Retrieve Reddit comments using pushshift.io API"""
    )
    parser.add_argument("--start", required=True, type=date_str_to_ISO_8601,
        metavar="YYYY-MM-DD", 
        help="""Start date (inclusive) after which comments should be requested. 
                ISO 8601 format (YYYY-MM-DD). Default timezone UTC.""")

    parser.add_argument("--end", required=False, type=date_str_to_ISO_8601,
        metavar="YYYY-MM-DD",  
        help="""End date (exclusive) before which comments should be requested. 
                ISO 8601 format (YYYY-MM-DD). Default timezone UTC.""")

    parser.add_argument("--dest_folder", required=False, type=pathlib.Path, 
        default=pathlib.Path("data/comments/raw"), 
        help="""Destination folder where comments should be saved to. 
                Defaults to `data/comments/raw` inside project directory.""")

    parser.add_argument("--force-rewrite", action="store_true", dest="force_rewrite",
        help="""Force rewrite data even if a version of it exists already.""")   
    
    
    # TODO: add query string parameter
    # TODO: add size arg
    # TODO: add timeout arg

    args = parser.parse_args()

    logging.basicConfig(format="%(asctime)s : %(levelname)s : %(message)s", 
                        level=logging.INFO, handlers=[TqdmLoggingHandler()])
        
    main(start_date=args.start, end_date=args.end, dest_folder=args.dest_folder,
         force_rewrite=args.force_rewrite)