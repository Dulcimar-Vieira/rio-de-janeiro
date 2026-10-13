#!/usr/bin/env python3
# coding: utf-8

"""Processa o feed WhatJobs e gera até 1.000 vagas filtradas para o RJ."""

import gzip
import hashlib
import io
import json
import os
import re
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
import xml.etree.ElementTree as ET

import requests

FEED_URL = "https://feeds.whatjobs.com/sinerj/sinerj_pt_BR.xml.gz"
CIDADES_RJ = {
    "rio de janeiro",
    "niteroi",
    "duque de caxias",
    "nova iguacu",
    "sao goncalo",
}
# Este repositório é para oportunidades de Jovem Aprendiz / início de carreira.
KEYWORDS = ("jovem aprendiz", "aprendiz", "primeiro emprego", "estagio")
OUTPUT_FOLDER = Path("json_parts")
MAX_JOBS = 1000
REQUEST_TIMEOUT = 90


def normalize(text):
    """Lowercase, remove accents, and collapse whitespace."""
    text = unicodedata.normalize("NFKD", text or "")
    text = "".join(char for char in text if not unicodedata.combining(char))
    return re.sub(r"\s+", " ", text).strip().lower()


def clean_html(text):
    text = re.sub(r"<[^>]+>", " ", text or "")
    return re.sub(r"\s+", " ", text).strip()


def text_at(element, path):
    return (element.findtext(path) or "").strip()


def normalize_company(company):
    return company.strip() if company and company.strip() else "Confidencial"


def generate_hash(title, company, city, url):
    # URL is included so different published listings do not collapse together.
    base = f"{normalize(title)}|{normalize(company)}|{normalize(city)}|{url.strip()}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def is_valid_keyword(text):
    text = normalize(text)
    return any(keyword in text for keyword in KEYWORDS)


def clear_old_parts(folder):
    folder.mkdir(parents=True, exist_ok=True)
    for path in folder.glob("part_*.json"):
        if path.is_file():
            path.unlink()


def write_json(path, data):
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
        file.write("\n")
    tmp_path.replace(path)


def main():
    clear_old_parts(OUTPUT_FOLDER)

    print("Baixando feed WhatJobs...")
    try:
        response = requests.get(
            FEED_URL,
            headers={"User-Agent": "SineRJ-FeedProcessor/1.0"},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"ERRO ao baixar feed: {exc}", file=sys.stderr)
        return 1

    jobs = []
    seen_urls = set()
    counts = {
        "xml_lidas": 0,
        "sem_dados_obrigatorios": 0,
        "fora_cidades": 0,
        "estado_incompativel": 0,
        "fora_palavras_chave": 0,
        "duplicadas": 0,
    }

    try:
        with gzip.open(io.BytesIO(response.content), "rt", encoding="utf-8") as feed:
            for _, elem in ET.iterparse(feed, events=("end",)):
                if elem.tag.split("}")[-1] != "job":
                    continue

                counts["xml_lidas"] += 1

                title = text_at(elem, "title")
                description_raw = text_at(elem, "description")
                company = normalize_company(text_at(elem, "company/name"))
                job_type = text_at(elem, "jobType")
                url = text_at(elem, "urlDeeplink")

                location_elem = elem.find("locations/location")
                city = text_at(location_elem, "city") if location_elem is not None else ""
                state = text_at(location_elem, "state") if location_elem is not None else ""

                if not title or not url or not city or not state:
                    counts["sem_dados_obrigatorios"] += 1
                    elem.clear()
                    continue

                if normalize(city) not in CIDADES_RJ:
                    counts["fora_cidades"] += 1
                    elem.clear()
                    continue

                # O feed tem usado o nome do estado no campo state; aceite RJ e
                # variações do nome, sem aceitar cidade de outro estado.
                state_norm = normalize(state)
                accepted_states = {"rj", "rio de janeiro"}
                if state_norm not in accepted_states:
                    counts["estado_incompativel"] += 1
                    elem.clear()
                    continue

                if not is_valid_keyword(f"{title} {clean_html(description_raw)}"):
                    counts["fora_palavras_chave"] += 1
                    elem.clear()
                    continue

                url_key = url.strip().rstrip("/")
                if url_key in seen_urls:
                    counts["duplicadas"] += 1
                    elem.clear()
                    continue
                seen_urls.add(url_key)

                description = clean_html(description_raw)
                salary = text_at(elem, "salary")

                job = {
                    "title": title,
                    "description": description,
                    "company": company,
                    "city": city,
                    "state": state,
                    "salary": salary if salary else "A Combinar",
                    "tipo": job_type if job_type else "Nao informado",
                    "origem": "WhatJobs",
                    "url": url,
                    "data_publicacao": datetime.now(timezone.utc).isoformat(),
                    "hash_unico": generate_hash(title, company, city, url),
                }

                jobs.append(job)
                elem.clear()

                if len(jobs) >= MAX_JOBS:
                    break

    except (OSError, EOFError, ET.ParseError) as exc:
        print(f"ERRO ao interpretar o XML compactado: {exc}", file=sys.stderr)
        return 1

    output_path = OUTPUT_FOLDER / "part_1.json"
    write_json(output_path, jobs)

    print("Resumo do processamento:")
    for name, value in counts.items():
        print(f"  {name}: {value}")
    print(f"  exportadas: {len(jobs)}")
    print(f"Arquivo gerado: {output_path}")

    # Uma execução válida pode produzir menos de 1.000 vagas se o feed não
    # contiver registros suficientes para os filtros aplicados.
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
