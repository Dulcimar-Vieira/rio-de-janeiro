import requests
import gzip
import xml.etree.ElementTree as ET
import io
import json
import os
import sys

# URL do feed
feed_url = "https://feeds.whatjobs.com/sinerj/sinerj_pt_BR.xml.gz"

# Pasta onde os arquivos JSON serão salvos
json_folder = "json_parts"
os.makedirs(json_folder, exist_ok=True)

# Contador de arquivos
file_count = 1
max_file_size_mb = 90  # Limite máximo por arquivo em MB

# Estados desejados
estados_desejados = {"rio de janeiro", "rj"}

# Baixar o feed XML comprimido
try:
    response = requests.get(feed_url, stream=True, timeout=60)
except requests.exceptions.RequestException as e:
    print(f"Erro ao baixar o feed: {e}")
    sys.exit(1)

if response.status_code == 200:
    with gzip.open(io.BytesIO(response.content), "rt", encoding="utf-8") as f:
        jobs = []

        for event, elem in ET.iterparse(f, events=("end",)):
            if elem.tag == "job":
                location_elem = elem.find("locations/location")
                city = location_elem.findtext("city", "").strip() if location_elem is not None else ""
                state = location_elem.findtext("state", "").strip() if location_elem is not None else ""

                if not city or not state:
                    elem.clear()
                    continue

                if state.lower() in estados_desejados:
                    company = elem.findtext("company/name", "").strip() or "Confidencial"

                    job_data = {
                        "title": elem.findtext("title", "").strip(),
                        "description": elem.findtext("description", "").strip(),
                        "company": company,
                        "city": city,
                        "state": state,
                        "url": elem.findtext("urlDeeplink", "").strip(),
                        "tipo": elem.findtext("jobType", "").strip(),
                    }

                    jobs.append(job_data)

                    # Estimar o tamanho do JSON em bytes
                    estimated_size_bytes = len(json.dumps(jobs, ensure_ascii=False).encode('utf-8'))
                    estimated_size_mb = estimated_size_bytes / (1024 * 1024)

                    if estimated_size_mb >= max_file_size_mb:
                        json_path = os.path.join(json_folder, f"part_{file_count}.json")
                        with open(json_path, "w", encoding="utf-8") as json_file:
                            json.dump(jobs, json_file, ensure_ascii=False, indent=2)
                        print(f"Arquivo salvo: {json_path} ({estimated_size_mb:.2f} MB)")
                        jobs = []
                        file_count += 1

                elem.clear()

        # Salvar o que restar
        if jobs:
            json_path = os.path.join(json_folder, f"part_{file_count}.json")
            with open(json_path, "w", encoding="utf-8") as json_file:
                json.dump(jobs, json_file, ensure_ascii=False, indent=2)
            size_mb = os.path.getsize(json_path) / (1024 * 1024)
            print(f"Arquivo final salvo: {json_path} ({size_mb:.2f} MB)")

    print(f"JSONs gerados: {os.listdir(json_folder)}")
else:
    print(f"Erro ao baixar o feed: código HTTP {response.status_code}")
    sys.exit(1)
