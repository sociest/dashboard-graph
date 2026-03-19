import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Tuple

import pandas as pd
import requests
import json


DEFAULT_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "DNT": "1",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}


def _fetch_page(
    endpoint: str,
    query_template: str,
    offset: int,
    limit: int,
    headers: dict,
    max_retries: int,
    timeout: int,
    retry_delay: int,
) -> Tuple[Optional[dict], Optional[str]]:
    query_with_offset = query_template.replace("{{OFFSET}}", str(offset)).replace(
        "{{LIMIT}}", str(limit)
    )
    payload = {"query": query_with_offset}

    for attempt in range(max_retries):
        response = None
        try:
            print(
                f"Request {attempt + 1}/{max_retries} | offset={offset} | limit={limit}"
            )
            response = requests.post(
                endpoint,
                json=payload,
                headers=headers,
                timeout=timeout,
                allow_redirects=False,
            )

            if response.status_code in [200, 201]:
                try:
                    return response.json(), None
                except ValueError:
                    print("JSON inválido en respuesta exitosa.")
                    return None, "invalid_json"

            body_preview = (response.text or "")[:250]
            print(f"Error HTTP {response.status_code}: {body_preview}")

            if attempt < max_retries - 1:
                time.sleep(retry_delay)

        except Exception as exc:
            print(f"Exception: {exc}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)

    return None, "request_failed"


def _prepare_csv_output(output_path: str) -> None:
    parent = os.path.dirname(output_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    if os.path.exists(output_path):
        os.remove(output_path)


def _append_csv_chunk(output_path: str, rows: list, include_header: bool) -> bool:
    if not rows:
        return include_header
    pd.DataFrame(rows).to_csv(
        output_path,
        mode="a",
        index=False,
        header=include_header,
    )
    return False


def collect_query_to_csv(
    query_template: str,
    format: str = "csv",
    output_path: str = "",
    endpoint: Optional[str] = None,
    initial_limit: int = 100,
    max_retries: int = 3,
    request_timeout: int = 900,
    retry_delay: int = 5,
    sleep_between_pages: float = 1.0,
    max_records: Optional[int] = None,
    parallel_workers: int = 2,
) -> None:
    endpoint = endpoint or os.getenv("QUERY_ENDPOINT", "https://query.sociest.org/")

    all_data = []
    total_collected = 0
    csv_header = True
    page_limit = max(1, initial_limit)
    workers = max(1, min(2, parallel_workers))

    if format == "csv":
        _prepare_csv_output(output_path)

    print("Start Extraction")
    print(f"Endpoint: {endpoint}")
    print(f"Parallel workers: {workers}")
    if max_records is not None:
        print(f"Max records configurado: {max_records}")

    if workers == 1:
        offset = 0
        consecutive_skips = 0

        while True:
            response_data, error = _fetch_page(
                endpoint=endpoint,
                query_template=query_template,
                offset=offset,
                limit=page_limit,
                headers=DEFAULT_HEADERS,
                max_retries=max_retries,
                timeout=request_timeout,
                retry_delay=retry_delay,
            )

            if error is None and response_data is not None:
                results = response_data.get("results", {}).get("bindings", [])

                if not results:
                    print(f"No more records at offset {offset}")
                    break

                chunk = results
                reached_max = False
                if max_records is not None:
                    remaining = max_records - total_collected
                    if remaining <= 0:
                        print(f"Reached max_records={max_records}")
                        break
                    if len(chunk) > remaining:
                        chunk = chunk[:remaining]
                        reached_max = True

                print(f"Offset {offset}: Retrieved {len(chunk)} records")

                if format == "csv":
                    csv_header = _append_csv_chunk(output_path, chunk, csv_header)
                else:
                    all_data.extend(chunk)

                total_collected += len(chunk)
                offset += len(results)
                consecutive_skips = 0
                page_limit = max(1, initial_limit)

                if reached_max:
                    print(f"Reached max_records={max_records}")
                    break

                time.sleep(sleep_between_pages)
                continue

            print(f"Request failed at offset {offset} with limit={page_limit}")

            if page_limit > 1:
                page_limit = max(1, page_limit // 2)
                print(f"Reducing page size and retrying with limit={page_limit}...")
                continue

            print(f"Skipping problematic record at offset {offset}")
            offset += 1
            page_limit = max(1, initial_limit)
            consecutive_skips += 1

            if consecutive_skips >= 100:
                print(
                    "Too many consecutive skipped records. Stopping to avoid infinite loop."
                )
                break
    else:
        next_offset = 0
        stop = False

        with ThreadPoolExecutor(max_workers=workers) as executor:
            while not stop:
                futures = {}

                for _ in range(workers):
                    if max_records is not None and total_collected >= max_records:
                        stop = True
                        break

                    offset = next_offset
                    next_offset += page_limit

                    future = executor.submit(
                        _fetch_page,
                        endpoint,
                        query_template,
                        offset,
                        page_limit,
                        DEFAULT_HEADERS,
                        max_retries,
                        request_timeout,
                        retry_delay,
                    )
                    futures[future] = offset

                if not futures:
                    break

                batch_results = []
                for future in as_completed(futures):
                    offset = futures[future]
                    response_data, error = future.result()
                    batch_results.append((offset, response_data, error))

                batch_results.sort(key=lambda x: x[0])

                for offset, response_data, error in batch_results:
                    if error is not None or response_data is None:
                        print(f"Request failed at offset {offset} with limit={page_limit}")
                        continue

                    results = response_data.get("results", {}).get("bindings", [])
                    if not results:
                        print(f"No more records at offset {offset}")
                        stop = True
                        continue

                    chunk = results
                    if max_records is not None:
                        remaining = max_records - total_collected
                        if remaining <= 0:
                            print(f"Reached max_records={max_records}")
                            stop = True
                            break
                        if len(chunk) > remaining:
                            chunk = chunk[:remaining]

                    print(f"Offset {offset}: Retrieved {len(chunk)} records")

                    if format == "csv":
                        csv_header = _append_csv_chunk(output_path, chunk, csv_header)
                    else:
                        all_data.extend(chunk)

                    total_collected += len(chunk)

                    if len(results) < page_limit:
                        stop = True

                    if max_records is not None and total_collected >= max_records:
                        print(f"Reached max_records={max_records}")
                        stop = True
                        break

                if not stop:
                    time.sleep(sleep_between_pages)

    print(f"\nTotal records collected: {total_collected}")

    if total_collected > 0:
        if format == "json":
            with open(output_path, "w") as file:
                json.dump(all_data, file, ensure_ascii=False, indent=2)
        if format == "csv":
            pass
        print(f"Data saved to {output_path} in format {format}")
    else:
        print("No data collected. Check endpoint/query permissions.")
