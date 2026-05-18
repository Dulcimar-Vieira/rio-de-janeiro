#!/usr/bin/env python3
# coding: utf-8

import requests
import gzip
import xml.etree.ElementTree as ET
import io
import json
import os
import hashlib
import re
from datetime import datetime

# =========================
# CONFIG
# =========================

FEED_URL = "https://feeds.whatjobs.com/sinerj/sinerj_pt_BR.xml.gz"

CIDADES_RJ = [
    "rio de janeiro",
    "niteroi",
    "duque de caxias",
    "nova iguacu",
    "sao goncalo"
]

KEYWORDS = [
    "jovem aprendiz",
    "aprendiz",
    "primeiro emprego",
    "estagio"
]

OUTPUT_FILE = "vagas_rj_jovem_aprendiz.json"

# =========================
# PASTA FEED
# =========================

def feed_dir():
    base = os.path.dirname(os.path.abspath(__file__))
    feed = os.path.join(base, "feed")
    os.makedirs(feed, exist_ok=True)
    return feed

# =========================
# HELPERS
# =========================

def normalize(text):
    if not text:
        return ""
    return text.strip().lower()


def clean_html(text):
    if not text:
        return ""

    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text)

    return text.strip()


def normalize_company(company):
    if not company or not company.strip():
        return "Confidencial"
    return company.strip()


def normalize_salary(text):
    if not text:
        return "A Combinar"

    text = re.sub(r"\s+", " ", text).strip()
    return text


def generate_hash(title, company, city, url):
    base = f"{title}-{company}-{city}-{url}"
    return hashlib.md5(base.encode("utf-8")).hexdigest()


def is_valid_keyword(text):
    text = normalize(text)

    for keyword in KEYWORDS:
        if keyword in text:
            return True

    return False

# =========================
# MAIN
# =========================

print("Baixando feed...")

response = requests.get(
    FEED_URL,
    headers={
        "User-Agent": "Mozilla/5.0"
    },
    timeout=60
)

if response.status_code != 200:
    print("Erro ao baixar feed")
    exit()

jobs = []
seen_urls = set()

with gzip.open(io.BytesIO(response.content), "rt", encoding="utf-8") as f:

    for event, elem in ET.iterparse(f, events=("end",)):

        if elem.tag != "job":
            continue

        title = elem.findtext("title", "").strip()
        description = elem.findtext("description", "").strip()
        company = normalize_company(
            elem.findtext("company/name", "")
        )

        job_type = elem.findtext("jobType", "").strip()
        url = elem.findtext("urlDeeplink", "").strip()

        salary = elem.findtext("salary", "").strip()

        location_elem = elem.find("locations/location")

        city = ""
        state = ""

        if location_elem is not None:
            city = location_elem.findtext("city", "").strip()
            state = location_elem.findtext("state", "").strip()

        if not city or not state or not title or not url:
            elem.clear()
            continue

        city_lower = normalize(city)

        # =========================
        # FILTRO RJ
        # =========================

        if city_lower not in CIDADES_RJ:
            elem.clear()
            continue

        # =========================
        # FILTRO KEYWORDS
        # =========================

        content_text = f"{title} {description}"

        if not is_valid_keyword(content_text):
            elem.clear()
            continue

        # =========================
        # DUPLICADOS
        # =========================

        if url in seen_urls:
            elem.clear()
            continue

        seen_urls.add(url)

        # =========================
        # LIMPEZA
        # =========================

        description = clean_html(description)
        salary = normalize_salary(salary)

        # =========================
        # HASH
        # =========================

        hash_unico = generate_hash(
            title,
            company,
            city,
            url
        )

        # =========================
        # JSON
        # =========================

        jobs.append({
            "title": title,
            "description": description,
            "company": company,
            "city": city,
            "state": state,
            "salary": salary if salary else "A Combinar",
            "tipo": job_type if job_type else "Nao informado",
            "origem": "WhatJobs",
            "url": url,
            "data_publicacao": datetime.utcnow().isoformat(),
            "hash_unico": hash_unico
        })

        elem.clear()

# =========================
# SAVE
# =========================

output_path = os.path.join(
    feed_dir(),
    OUTPUT_FILE
)

with open(output_path, "w", encoding="utf-8") as json_file:
    json.dump(
        jobs,
        json_file,
        ensure_ascii=False,
        indent=2
    )

print(f"TOTAL: {len(jobs)} vagas exportadas")
print(f"Arquivo: {output_path}")
