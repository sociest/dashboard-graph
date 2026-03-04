import requests, json
import pandas as pd
import time

query = """SELECT ?item ?label ?ci ?cargo ?militancia ?trayectoria ?estudios ?foto ?youtube ?facebook ?instagram ?tiktok ?twitter ?partido ?territorio
WHERE { 
  ?item claim:69857da6142c6cf1636b ?stmt1 .
  OPTIONAL {
    ?item claim:69839e7ca5dfc05c1847 ?stmt2 .
    ?stmt2 value: ?ci .
    ?item claim:69909ff678ca509e132b ?stmt3 .
    ?stmt3 value: ?militancia .
    ?item claim:6991e1f71d9b946eed02 ?stmt4 .
    ?stmt4 value: ?trayectoria .
    ?item claim:698a960962e87e866083 ?stmt5 .
    ?stmt5 value: ?estudios .
    ?item claim:698d2b149e3a7aa9ca9d ?stmt6 .
    ?stmt6 value: ?foto .
    ?item claim:698ff396819084d3f34f ?stmt7 .
    ?stmt7 value: ?youtube .
    ?item claim:698a9704d5423dd2a594 ?stmt8 .
    ?stmt8 value: ?facebook .
    ?item claim:6990ac7d411c99d182eb ?stmt9 .
    ?stmt9 value: ?instagram .
    ?item claim:698d2ea93ec1314cd130 ?stmt10 .
    ?stmt10 value: ?tiktok .
    ?item claim:6990acbb7e77c6674b88 ?stmt11 .
    ?stmt11 value: ?twitter .
    ?item claim:69857da6142c6cf1636b ?stmt12 .
    ?stmt12 value: ?cargo .
    ?stmt12 qual:6985697dce1378ac55e9 ?partido .
    ?stmt12 qual:6982cd215f22d1c5d613 ?territorio .
  }
}
LIMIT {{LIMIT}}
OFFSET {{OFFSET}}"""

endpoint = "https://appwrite.sociest.org/v1/functions/69a736320015c4f40b23/executions"

# Fetch all candidates with pagination
offset = 0  # Start from next batch
limit = 100
max_iterations = 80  # Safety limit

all_data = []

headers = {
    "Content-Type": "application/json",
    "X-Appwrite-Project": "697ea96f003c3264105c"
}

print("Start Extraction")

for i in range(max_iterations):
    query_with_offset = query.replace("{{OFFSET}}", str(offset)).replace("{{LIMIT}}", str(limit))
    
    payload = {
        "body": json.dumps({
            "query": query_with_offset.replace('\n', '\n')
        }),
        "method": "POST",
        "headers": {"Content-Type": "application/json"}
    }
    
    max_retries = 3
    response = None
    
    for attempt in range(max_retries):
        try:
            print(f"get")
            response = requests.post(endpoint, json=payload, headers=headers, timeout=900)
            if response.status_code == 201:
                break
            elif attempt < max_retries - 1:
                print(f"Attempt {attempt + 1} failed with status {response.status_code}, retrying...")
                time.sleep(2)
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Attempt {attempt + 1} failed with error: {e}, retrying...")
                time.sleep(2)
    
    if response is None or response.status_code != 201:
        print(f"Request failed after {max_retries} attempts with status {response.status_code if response else 'No response'}")
        break
    
    response_data = response.json()
    response_body = json.loads(response_data["responseBody"])
    results = response_body.get("results", {}).get("bindings", [])
    
    if not results:
        print(f"No more records at offset {offset}")
        break
    
    print(f"Offset {offset}: Retrieved {len(results)} records")
    all_data.extend(results)
    offset += limit
    #time.sleep(1)

print(f"\nTotal candidates collected: {len(all_data)}")

all_data_df = pd.DataFrame(all_data)

all_data_df.to_csv("datos/candidatos.csv",index=False)

