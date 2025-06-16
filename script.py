import requests
import gzip
import xml.etree.ElementTree as ET
import io
import json
import os

# URL do feed
feed_url = "https://feeds.whatjobs.com/sinerj/sinerj_pt_BR.xml.gz"

# Pasta onde os arquivos JSON serão salvos
json_folder = "json_parts"
os.makedirs(json_folder, exist_ok=True)

file_count = 1           # controle de partes geradas
jobs = []                # buffer de vagas

# Estados desejados → todos em minúsculo para comparação segura
estados_desejados = {"rio de janeiro", "rj"}

# Baixar o feed XML comprimido
print("📥 Baixando feed do WhatJobs…")
try:
    response = requests.get(feed_url, stream=True, timeout=60)
    response.raise_for_status()
except requests.exceptions.RequestException as e:
    print(f"❌ Erro ao baixar o feed: {e}")
    exit(1)

with gzip.open(io.BytesIO(response.content), "rt", encoding="utf-8") as f:
    for event, elem in ET.iterparse(f, events=("end",)):
        if elem.tag != "job":
            continue

        location_elem = elem.find("locations/location")
        city  = location_elem.findtext("city",  "").strip() if location_elem is not None else ""
        state = location_elem.findtext("state", "").strip() if location_elem is not None else ""

        # Ignora se city ou state estiverem vazios
        if not city or not state:
            elem.clear()
            continue

        # Filtra se o estado faz parte da lista desejada
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

        elem.clear()

        # Salva arquivos de 1 000 em 1 000
        if len(jobs) >= 1000:
            json_path = os.path.join(json_folder, f"part_{file_count}.json")
            with open(json_path, "w", encoding="utf-8") as fp:
                json.dump(jobs, fp, ensure_ascii=False, indent=2)
            print(f"✅ Arquivo salvo: {json_path}")
            jobs = []
            file_count += 1

# Salva o restante (menos de 1 000)
if jobs:
    json_path = os.path.join(json_folder, f"part_{file_count}.json")
    with open(json_path, "w", encoding="utf-8") as fp:
        json.dump(jobs, fp, ensure_ascii=False, indent=2)
    print(f"✅ Arquivo final salvo: {json_path}")

print(f"📦 Total de arquivos gerados: {file_count}")
