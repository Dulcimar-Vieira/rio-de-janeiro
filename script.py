#!/usr/bin/env python3
# coding: utf-8

"""Processa o feed WhatJobs e gera até 1.000 vagas salariais do RJ."""

import gzip
import hashlib
import io
import json
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
OUTPUT_FOLDER = Path("json_parts")
MAX_JOBS = 1000
REQUEST_TIMEOUT = 90

SALARY_CONTEXT = re.compile(
    r"\b(?:sal[aá]rio|sal[aá]rios|remunera[cç][aã]o|faixa salarial|piso salarial|proventos)\b",
    re.IGNORECASE,
)
AMOUNT_PATTERN = re.compile(
    r"(?:R\$\s*)?(?:"
    r"\d{1,3}(?:\.\d{3})+(?:,\d{2})?"
    r"|\d{1,3}(?:,\d{3})+(?:\.\d{2})?"
    r"|\d+(?:[.,]\d{2})"
    r")",
    re.IGNORECASE,
)


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


def has_real_salary(salary):
    """Require a non-placeholder salary with a numeric amount."""
    value = normalize(salary)
    if not value:
        return False

    placeholders = (
        "a combinar",
        "nao informado",
        "nao informada",
        "nao divulgado",
        "nao divulgada",
        "confidencial",
        "negociavel",
        "sob consulta",
        "salario nao informado",
        "salario a combinar",
    )
    if any(marker in value for marker in placeholders):
        return False

    return bool(re.search(r"\d", value))


def extract_salary(structured_salary, description):
    """
    Prefer the XML salary tag. If it's empty, accept a numeric amount only
    when a salary/remuneration label appears close to that amount in text.
    This reduces confusion with transport, meal, or other benefit amounts.
    """
    structured_salary = re.sub(r"\s+", " ", structured_salary or "").strip()
    if has_real_salary(structured_salary):
        return structured_salary

    text = clean_html(description)
    for match in AMOUNT_PATTERN.finditer(text):
        start, end = match.span()
        context_start = max(0, start - 110)
        context_end = min(len(text), end + 50)
        context = text[context_start:context_end]
        if SALARY_CONTEXT.search(context):
            amount = re.sub(r"\s+", " ", match.group(0)).strip()
            if has_real_salary(amount):
                return amount

    return ""


def generate_hash(title, company, city, url):
    base = f"{normalize(title)}|{normalize(company)}|{normalize(city)}|{url.strip()}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


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
        "sem_salario_valido": 0,
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
                source_salary = text_at(elem, "salary")

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

                state_norm = normalize(state)
                if state_norm not in {"rj", "rio de janeiro"}:
                    counts["estado_incompativel"] += 1
                    elem.clear()
                    continue

                salary = extract_salary(source_salary, description_raw)
                if not salary:
                    counts["sem_salario_valido"] += 1
                    elem.clear()
                    continue

                url_key = url.strip().rstrip("/")
                if url_key in seen_urls:
                    counts["duplicadas"] += 1
                    elem.clear()
                    continue
                seen_urls.add(url_key)

                job = {
                    "title": title,
                    "description": clean_html(description_raw),
                    "company": company,
                    "city": city,
                    "state": state,
                    "salary": salary,
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

    # Não publicar feed vazio ou silenciosamente incompleto.
    if not jobs:
        print(
            "ERRO: nenhuma vaga com salário válido foi encontrada. "
            "O merged.json e a publicação não serão atualizados.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
