import os
import csv
import logging
from urllib.parse import urljoin
from typing import List, Dict

import boto3
from botocore.exceptions import ClientError
import requests
from bs4 import BeautifulSoup

# Página objetivo
BASE_URL = "https://ultimosismo.igp.gob.pe"
TARGET_URL = urljoin(BASE_URL, "/ultimo-sismo/sismos-reportados")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def fetch_latest_sismos(limit: int = 10) -> List[Dict]:
    """Usa requests + BeautifulSoup para obtener los últimos 'limit' sismos.

    Devuelve una lista de dicts con: referencia, reporte_url, fecha_hora, magnitud.
    """
    results: List[Dict] = []

    logger.info("Fetching %s", TARGET_URL)
    try:
        resp = requests.get(TARGET_URL, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        logger.exception("Error descargando la página: %s", e)
        return results

    soup = BeautifulSoup(resp.text, "html.parser")
    rows = soup.select("table.table tbody tr")
    logger.info("Filas encontradas: %d", len(rows))

    for i, row in enumerate(rows[:limit]):
        try:
            tds = row.find_all("td")
            referencia = tds[0].get_text(strip=True) if len(tds) > 0 else ""
            fecha_hora = tds[2].get_text(strip=True) if len(tds) > 2 else ""
            magnitud = tds[3].get_text(strip=True) if len(tds) > 3 else ""

            a = row.find("a", href=True)
            href = a["href"] if a else None
            reporte_url = urljoin(BASE_URL, href) if href else None

            referencia = " ".join([s.strip() for s in referencia.splitlines() if s.strip()])

            item = {
                "referencia": referencia,
                "reporte_url": reporte_url,
                "fecha_hora": fecha_hora,
                "magnitud": magnitud,
            }
            results.append(item)
        except Exception as e:
            logger.exception("Error parseando fila %d: %s", i, e)

    return results


def save_to_csv(items: List[Dict], path: str = "sismos.csv"):
    if not items:
        logger.info("No hay items para guardar en CSV")
        return

    keys = list(items[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for it in items:
            writer.writerow(it)

    logger.info("Guardado %d registros en %s", len(items), path)


def save_to_dynamodb(items: List[Dict], table_name: str) -> bool:
    if not items:
        logger.info("No hay items para guardar en DynamoDB")
        return True

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    try:
        with table.batch_writer() as batch:
            for it in items:
                # Añadir un id si no existe
                if "id" not in it:
                    from uuid import uuid4

                    it["id"] = str(uuid4())
                batch.put_item(Item=it)
        logger.info("Guardado %d registros en DynamoDB tabla %s", len(items), table_name)
        return True
    except ClientError as e:
        logger.exception("Error guardando en DynamoDB: %s", e)
        return False


def lambda_handler(event, context):
    """Handler pensado para uso en AWS Lambda (si Playwright está empaquetado apropiadamente).

    Si existe la variable de entorno DDB_TABLE, intenta guardar en DynamoDB; si falla o no existe,
    guarda en `sismos.csv`.
    """
    limit = int(os.environ.get("LIMIT", "10"))
    items = fetch_latest_sismos(limit=limit)

    table_name = os.environ.get("DDB_TABLE")
    saved = False
    if table_name:
        saved = save_to_dynamodb(items, table_name)

    if not table_name or not saved:
        save_to_csv(items, path=os.environ.get("CSV_PATH", "sismos.csv"))

    return {"statusCode": 200, "body": items}


if __name__ == "__main__":
    # Ejecución local de ejemplo
    items = fetch_latest_sismos(limit=10)
    # Intentar guardar en DynamoDB si la tabla está en la env var
    table_name = os.environ.get("DDB_TABLE")
    if table_name:
        ok = save_to_dynamodb(items, table_name)
        if not ok:
            save_to_csv(items)
    else:
        save_to_csv(items)
    print(f"Obtenidos {len(items)} registros.")
