import requests, json
import pandas as pd
import time
import os

query = """
SELECT ?item ?label ?cobertura ?coberturaLabel ?autor ?autorLabel ?fecha_fin ?fecha_inicio ?margen ?muestra ?resultado ?publicacion ?nivel_confianza ?archivo ?casa_registrada ?ciudadanos_registrados
WHERE {
  OPTIONAL { ?item claim:698febc2001abeb954c4 ?ret_stmt3 . ?ret_stmt3 value: ?cobertura . }
  OPTIONAL { ?item claim:698ff2fb95a70777d5e8 ?ret_stmt4 . ?ret_stmt4 value: ?autor . }
  OPTIONAL { ?item claim:69821fd781d266c39049 ?ret_stmt5 . ?ret_stmt5 value: ?fecha_fin . }
  OPTIONAL { ?item claim:698a967bd8e7eecbcef3 ?ret_stmt6 . ?ret_stmt6 value: ?fecha_inicio . }
  OPTIONAL { ?item claim:698caa881742f3dbb3b4 ?ret_stmt7 . ?ret_stmt7 value: ?margen . }
  OPTIONAL { ?item claim:698caa3f0d32d780c789 ?ret_stmt8 . ?ret_stmt8 value: ?muestra . }
  OPTIONAL { ?item claim:698cad71c5f2134e49f9 ?ret_stmt9 . ?ret_stmt9 value: ?resultado . }
  OPTIONAL { ?item claim:699ddf64a6cc230024bb ?ret_stmt10 . ?ret_stmt10 value: ?publicacion . }
  OPTIONAL { ?item claim:699893d5d05f1a329cb4 ?ret_stmt11 . ?ret_stmt11 value: ?nivel_confianza . }
  OPTIONAL { ?item claim:6985e9c7598e2f77b9d3 ?ret_stmt12 . ?ret_stmt12 value: ?archivo . }
  OPTIONAL { ?autor claim:69814ee90009513e4f69 ?ret_stmt13 . ?ret_stmt13 value: ?casa_registrada . }
  OPTIONAL { ?cobertura claim:69892ef13a40d8c7a3c7 ?ret_stmt14 . ?ret_stmt14 value: ?ciudadanos_registrados . }
  OPTIONAL { ?cobertura claim:69857deeecda0a266b57 ?ret_stmt15 . ?ret_stmt15 value: ?censo_personas16 . }

  ?item claim:69814ee90009513e4f69 ?stmt0 .
  ?stmt0 value: "698ff11ee732836cda65" .
}
LIMIT {{LIMIT}}
OFFSET {{OFFSET}}"""

endpoint = "https://query.sociest.org/"

# Cargar variables de entorno o usar valores por defecto
project_id = os.getenv("APPWRITE_PROJECT", "697ea96f003c3264105c")
api_key = os.getenv("APPWRITE_API_KEY", "")
appwrite_url = os.getenv("APPWRITE_URL", "https://appwrite.sociest.org")
cloudflare_token = os.getenv("CLOUDFLARE_TOKEN", "")

# Fetch all candidates with pagination
offset = 0
limit = 200
max_iterations = 10

all_data = []

# Headers que simulan un navegador real para evitar Cloudflare
headers = {
    "Content-Type": "application/json",
    #    "X-Appwrite-Project": project_id,
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    #    "Origin": appwrite_url,
    #    "Referer": f"{appwrite_url}/",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "DNT": "1",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}

# Agregar API key si está disponible
# if api_key:
#    headers["X-Appwrite-Key"] = api_key

# Agregar Cloudflare token si está disponible
# if cloudflare_token:
#    headers["CF-Access-Token"] = cloudflare_token


print("Start Extraction")
# print(f"Appwrite URL: {appwrite_url}")
# print(f"Project ID: {project_id[:20]}...")
# print(f"API Key: {'Configurada' if api_key else 'No configurada'}")
# print(f"Cloudflare Token: {'Configurado' if cloudflare_token else 'No configurado'}")

for i in range(max_iterations):
    query_with_offset = query.replace("{{OFFSET}}", str(offset)).replace(
        "{{LIMIT}}", str(limit)
    )

    payload = {"query": query_with_offset.replace("\n", "\n")}

    max_retries = 3
    response = None

    for attempt in range(max_retries):
        try:
            print(f"Attempting request {attempt + 1}/3 for offset {offset}...")

            # Crear sesión para mantener cookies
            session = requests.Session()
            session.headers.update(headers)

            response = session.post(
                endpoint, json=payload, timeout=900, allow_redirects=False
            )

            if response.status_code in [201, 200]:
                print(f"Success!")
                break
            elif response.status_code == 403:
                print(
                    f"403 Forbidden - {list(response.headers.keys())} {response.text}"
                )
                if attempt < max_retries - 1:
                    print(f"Esperando 5 segundos antes de reintentar...")
                    time.sleep(5)
            else:
                print(f"Error {response.status_code}: {response.text[:200]}")
                if attempt < max_retries - 1:
                    print(f"Reintentando en 5 segundos...")
                    time.sleep(5)
        except Exception as e:
            print(f"Exception: {str(e)}")
            if attempt < max_retries - 1:
                print(f"Reintentando en 5 segundos...")
                time.sleep(5)

    if response is None or response.status_code not in [201, 200]:
        print(f"Request failed after {max_retries} attempts")
        if response:
            print(f"Status: {response.status_code}")
            print(f"Response: {response.text[:500]}")
        raise Exception()
        # break

    response_data = response.json()
    results = response_data.get("results", {}).get("bindings", [])

    if not results:
        print(f"No more records at offset {offset}")
        break

    print(f"Offset {offset}: Retrieved {len(results)} records")
    all_data.extend(results)
    offset += limit
    time.sleep(1)

print(f"\nTotal candidates collected: {len(all_data)}")

if all_data:
    all_data_df = pd.DataFrame(all_data)
    all_data_df.to_csv("datos/encuestas.csv", index=False)
    print("Data saved to datos/encuestas.csv")
else:
    print("No data collected. Check authentication and permissions.")
