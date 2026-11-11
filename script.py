#!/usr/bin/env python3
# coding: utf-8

"""Processa até 1.000 vagas do RJ, priorizando salários informados."""

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

# O campo estruturado <salary> é preferido. No fallback, o número precisa
# aparecer logo após um rótulo salarial explícito, sem varrer a descrição inteira.
MONEY_NUMBER = (
    r"(?:"
    r"\d{1,3}(?:\.\d{3})+(?:,\d{2})?"
    r"|\d{1,3}(?:,\d{3})+(?:\.\d{2})?"
    r"|\d+(?:,\d{2}|\.\d{2})?"
    r")"
    r"(?:\s*(?:mil|k)\b)?"
)
SALARY_AMOUNT_RE = re.compile(
    rf"\b(?:faixa salarial|piso salarial|sal[aá]rios?|remunera[cç][aã]o|proventos)\b"
    rf"(?:\s+(?:fixo|base|mensal(?:\s+de)?|bruto|inicial|m[ií]nimo garantido|"
    rf"oferecido|oferecida|oferecidos|oferecidas|de at[eé]|a partir de|entre)){{0,3}}"
    rf"\s*(?:[:：=–—-]\s*)?"
    rf"(?:(?:a partir de|de|entre)\s+)?"
    rf"(?P<amount>(?:R\$\s*)?{MONEY_NUMBER})(?![\d.,])",
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

    # WhatJobs specifies salary as an amount plus currency. Reject bare
    # integer placeholders from the structured field; description-based
    # numbers are handled only through an explicit salary label below.
    if not re.search(r"\d", value):
        return False
    if not re.search(r"(?:R\$|[$€£])", value) and re.fullmatch(r"\d+", value):
        return False
    return True

MAX_SALARY_BRL = 10000.0
SALARY_VALUE_RE = re.compile(
    rf"(?<![\w])(?:R\$\s*|[$€£]\s*)?{MONEY_NUMBER}(?![\d.,])",
    re.IGNORECASE,
)


def parse_salary_amount(raw_amount):
    """Converte valores salariais brasileiros/internacionais, incluindo mil/k."""
    raw_value = str(raw_amount or "").strip()
    multiplier = 1000.0 if re.search(r"\s*(?:mil|k)\b", raw_value, re.IGNORECASE) else 1.0
    value = re.sub(r"\s*(?:mil|k)\b", "", raw_value, flags=re.IGNORECASE)
    value = re.sub(r"^(?:R\$|[$€£])\s*", "", value, flags=re.IGNORECASE)
    value = re.sub(r"[^\d.,]", "", value)
    if not value:
        return None

    if "." in value and "," in value:
        if value.rfind(",") > value.rfind("."):
            value = value.replace(".", "").replace(",", ".")
        else:
            value = value.replace(",", "")
    elif "," in value:
        left, _, right = value.rpartition(",")
        if len(right) == 3 and left:
            value = value.replace(",", "")
        else:
            value = left.replace(",", "") + "." + right
    elif "." in value:
        parts = value.split(".")
        if len(parts) > 1 and len(parts[0]) <= 3 and all(len(part) == 3 for part in parts[1:]):
            value = "".join(parts)
        else:
            value = "".join(parts[:-1]) + "." + parts[-1]

    try:
        return float(value) * multiplier
    except ValueError:
        return None


def salary_exceeds_limit(salary, description=""):
    """Descarta salários acima de R$ 10 mil, inclusive o teto de faixas."""
    for match in SALARY_VALUE_RE.finditer(salary or ""):
        amount = parse_salary_amount(match.group(0))
        if amount is not None and amount > MAX_SALARY_BRL:
            return True

    text = clean_html(description)
    range_tail_re = re.compile(
        rf"\s*(?:-|–|—|\ba\b|\bat[eé]\b)\s*(?:R\$\s*)?({MONEY_NUMBER})(?![\d.,])",
        re.IGNORECASE,
    )
    for match in SALARY_AMOUNT_RE.finditer(text):
        amount = parse_salary_amount(match.group("amount"))
        if amount is not None and amount > MAX_SALARY_BRL:
            return True
        range_match = range_tail_re.match(text[match.end():match.end() + 100])
        if range_match:
            upper_amount = parse_salary_amount(range_match.group(1))
            if upper_amount is not None and upper_amount > MAX_SALARY_BRL:
                return True
    return False


def extract_salary(structured_salary, description):
    """
    Use the salary field if it contains an explicit amount. Otherwise accept
    only an amount immediately associated with an explicit salary label.
    This intentionally rejects numbers that appear only in benefit sections.
    """
    structured_salary = re.sub(r"\s+", " ", structured_salary or "").strip()
    if has_real_salary(structured_salary):
        return structured_salary

    text = clean_html(description)
    for match in SALARY_AMOUNT_RE.finditer(text):
        amount = re.sub(r"\s+", " ", match.group("amount")).strip()
        raw_number = re.sub(r"^R\$\s*", "", amount, flags=re.IGNORECASE).strip()
        has_currency = amount.upper().startswith("R$")

        # Without an explicit currency, reject short bare integers that are
        # likely section numbers (e.g. "Salários oferecidos 2. DOS CARGOS...").
        if not has_currency and re.fullmatch(r"\d{1,2}", raw_number):
            continue

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

    jobs_with_salary = {}
    jobs_without_salary = {}
    counts = {
        "xml_lidas": 0,
        "sem_dados_obrigatorios": 0,
        "fora_cidades": 0,
        "estado_incompativel": 0,
        "com_salario": 0,
        "sem_salario": 0,
        "salario_acima_de_10000": 0,
        "duplicadas": 0,
    }

    try:
        with gzip.open(io.BytesIO(response.content), "rb") as feed:
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
                if salary_exceeds_limit(salary, description_raw):
                    counts["salario_acima_de_10000"] += 1
                    elem.clear()
                    continue
                has_salary = has_real_salary(salary)
                salary_value = salary if has_salary else "A combinar"
                counts["com_salario" if has_salary else "sem_salario"] += 1

                url_key = url.strip().rstrip("/")

                job = {
                    "title": title,
                    "description": clean_html(description_raw),
                    "company": company,
                    "city": city,
                    "state": state,
                    "salary": salary_value,
                    "tipo": job_type if job_type else "Nao informado",
                    "origem": "WhatJobs",
                    "url": url,
                    "data_publicacao": datetime.now(timezone.utc).isoformat(),
                    "hash_unico": generate_hash(title, company, city, url),
                }

                if has_salary:
                    target = jobs_with_salary
                    other = jobs_without_salary
                else:
                    target = jobs_without_salary
                    other = jobs_with_salary

                # Se a URL repetir, a versão que informa salário prevalece.
                if url_key in other:
                    if has_salary:
                        other.pop(url_key, None)
                    else:
                        counts["duplicadas"] += 1
                        elem.clear()
                        continue
                if url_key in target:
                    counts["duplicadas"] += 1
                else:
                    target[url_key] = job

                elem.clear()

    except (OSError, EOFError, ET.ParseError) as exc:
        print(f"ERRO ao interpretar o XML compactado: {exc}", file=sys.stderr)
        return 1

    salary_jobs = sorted(
        jobs_with_salary.values(),
        key=lambda job: job.get("data_publicacao", ""),
        reverse=True,
    )
    no_salary_jobs = sorted(
        jobs_without_salary.values(),
        key=lambda job: job.get("data_publicacao", ""),
        reverse=True,
    )
    jobs = (salary_jobs + no_salary_jobs)[:MAX_JOBS]

    print("Resumo do processamento:")
    for name, value in counts.items():
        print(f"  {name}: {value}")
    print(f"  exportadas: {len(jobs)}")

    # Proteção temporária para manter o último lote válido de 1.000 vagas
    # enquanto a WhatJobs não voltar a fornecer o feed completo do RJ.
    # Se vier um lote parcial, falha antes de gravar/mesclar/publicar JSON.
    if len(jobs) < MAX_JOBS:
        print(
            f"ERRO: foram encontradas apenas {len(jobs)} vagas válidas, "
            f"abaixo das {MAX_JOBS} esperadas. O merged.json e os JSON "
            "publicados serão preservados até o feed normalizar.",
            file=sys.stderr,
        )
        return 1

    clear_old_parts(OUTPUT_FOLDER)
    output_path = OUTPUT_FOLDER / "part_1.json"
    write_json(output_path, jobs)
    print(f"Arquivo gerado: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
