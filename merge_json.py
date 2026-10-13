#!/usr/bin/env python3
# coding: utf-8

"""Combina apenas os arquivos part_*.json gerados na execução atual."""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

folder = Path("json_parts")
output = Path("merged.json")


def main():
    parts = sorted(
        folder.glob("part_*.json"),
        key=lambda item: int(item.stem.split("_", 1)[1]),
    )
    all_jobs = []
    seen_urls = set()

    for part in parts:
        with part.open("r", encoding="utf-8") as file:
            data = json.load(file)
        if not isinstance(data, list):
            raise ValueError(f"{part} não contém uma lista JSON.")
        for job in data:
            url = (job.get("url") or "").strip().rstrip("/")
            if url and url in seen_urls:
                continue
            if url:
                seen_urls.add(url)
            all_jobs.append(job)

    result = {
        "gerado_em": datetime.now(timezone.utc).isoformat(),
        "total_vagas": len(all_jobs),
        "vagas": all_jobs,
    }

    tmp = output.with_suffix(output.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as file:
        json.dump(result, file, ensure_ascii=False, indent=2)
        file.write("\n")
    tmp.replace(output)

    size_mb = os.path.getsize(output) / (1024 * 1024)
    print(f"Arquivos de entrada: {len(parts)}")
    print(f"Total de vagas no merged.json: {len(all_jobs)}")
    print(f"Tamanho do arquivo: {size_mb:.2f} MB")

    # Saída vazia é informação útil, mas falha para impedir a publicação
    # acidental de um feed sem vagas.
    if not all_jobs:
        raise SystemExit("ERRO: nenhum registro foi gerado; confira os filtros/feed.")


if __name__ == "__main__":
    main()
