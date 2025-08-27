#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
#from future annotation basicamente sirve para indicar que las anotaciones de tipo se evaluaran en tiempo de ejecucion
#osea que se pueden usar tipos que no han sido definidos aun

import concurrent.futures as cf
import os
import re
import time
import xml.etree.ElementTree as ET
#fromdataclass para describir archivos listados en el bucket
from dataclasses import dataclass
from typing import Iterable, List, Optional
from urllib.parse import quote, unquote
#libreria HTTP y progreso
import requests
from tqdm import tqdm

#constantes de configuracion basica
BUCKET_URL = "https://s3.amazonaws.com/tripdata"
DEFAULT_DEST = "./citibike_data"
WORKERS = 4
INCLUDE_REGEX: Optional[str] = None
#filtro opcional por nombre de archivo por si acaso
#namespace XML de S3 para realizar consultas con ElementTree
#en donde  elementtree permite trabajar con XML de manera sencilla
#sin esto find/findall no hayaria los nodos
S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

#estructura de datos para cada archivo encontrado en el bucket
@dataclass
class FileEntry:
    key: str
    url: str
    size: Optional[int] = None
    #tamano en bytes si se conoce

#crea una sesion HTTP simple
def session() -> requests.Session:
    return requests.Session()

#NOTA JD:
#Compartir una Session entre hilos suele funcionar para GET sencillos, 
#pero no está estrictamente garantizado como “thread-safe” en todos los escenarios
#bueno para GET simples (descargas simples) pero no muy bueno para
#pero pueden trabarse en la misma sesion si varias descargas se meten a la vez
#entonces usar una sesion por hilo estaria bien


#lista los objetos del bucket S3 usando la API ListObjectsV2 con paginacion
#la paginación es cuando el servidor envía la lista en partes 
#y el programa va pidiendo la siguiente hasta obtener todo.


#osea basicamente ir trayendo la lista de archivos del servidor por partes cuando hay demasiados.


#Esta función va al servidor, pide la lista de archivos en bloques (paginación) y guarda solo los que interesan:
#Solo .zip
#Que no empiecen con JC-
#Luego construye su URL y tamaño, los ordena por nombre y devuelve la lista lista para descargar.


def list_bucket_objects(base_url: str) -> List[FileEntry]:
    entries: List[FileEntry] = []
    token: Optional[str] = None
    s = session()
    while True:
        #parametros de consulta para obtener hasta 1000 objetos por pagina y claves url-encoded
        params = {"list-type": "2", "max-keys": "1000", "encoding-type": "url"}
        #si hay token, lo enviamos para obtener la siguiente pagina
        if token:
            params["continuation-token"] = token
        r = s.get(base_url, params=params, timeout=60)
        r.raise_for_status()
        #parseamos la respuesta XML
        root = ET.fromstring(r.content)
        #iteramos cada nodo Contents (cada objeto del bucket)
        for contents in root.findall("s3:Contents", S3_NS):
            key_enc = contents.findtext("s3:Key", default="", namespaces=S3_NS)
            size_txt = contents.findtext("s3:Size", default="", namespaces=S3_NS)
            if not key_enc:
                continue
            #decodificamos la clave porque viene url-encoded
            key = unquote(key_enc)
            #nos interesan solo archivos .zip
            if not key.lower().endswith(".zip"):
                continue
            name = os.path.basename(key)
            #omitimos archivos de Jersey City que empiezan con JC-
            if name.upper().startswith("JC-"):
                continue
            #construimos la URL directa del objeto en el bucket
            url = f"{base_url}/{quote(key)}"
            size = int(size_txt) if size_txt.isdigit() else None
            entries.append(FileEntry(key=key, url=url, size=size))
        #manejo de paginacion
        next_token = root.findtext("s3:NextContinuationToken", default=None, namespaces=S3_NS)
        is_truncated = root.findtext("s3:IsTruncated", default="false", namespaces=S3_NS)
        if not (is_truncated and is_truncated.lower() == "true" and next_token):
            break
        token = next_token
    #orden estable por nombre de clave
    entries.sort(key=lambda e: e.key.lower())
    return entries

#aplica un filtro regex opcional a la lista de entradas
def filter_includes(entries: Iterable[FileEntry], include_regex: Optional[str]) -> List[FileEntry]:
    if not include_regex:
        return list(entries)
    rx = re.compile(include_regex)
    return [e for e in entries if rx.search(os.path.basename(e.key))]

#decide si podemos saltar la descarga cuando el archivo ya existe y el tamano coincide
def should_skip(path: str, remote_size: Optional[int]) -> bool:
    if not os.path.exists(path):
        return False
    if remote_size is None:
        return True
    try:
        return os.path.getsize(path) == remote_size
    except OSError:
        return False

#descarga un archivo con reintentos y barra de progreso; retorna (nombre, estado)
def download_one(s: requests.Session, entry: FileEntry, dest_dir: str, max_retries: int = 4) -> tuple[str, str]:
    os.makedirs(dest_dir, exist_ok=True)
    name = os.path.basename(entry.key)
    target = os.path.join(dest_dir, name)
    #si ya esta y coincide con el tamano remoto (si se conoce), lo omitimos
    if should_skip(target, entry.size):
        return (name, "skipped")
    #factor de backoff exponencial para reintentos
    backoff = 1.6
    for attempt in range(1, max_retries + 1):
        try:
            #solicitamos el archivo en modo stream para escribir por chunks
            with s.get(entry.url, stream=True, timeout=120) as r:
                r.raise_for_status()
                total = entry.size
                #escribimos a un archivo temporal .part mientras mostramos el progreso
                with tqdm(total=total, unit="B", unit_scale=True, unit_divisor=1024,
                          desc=name, leave=False) as pbar, open(target + ".part", "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))
            #validamos el tamano descargado si lo conocemos
            if entry.size is not None and os.path.getsize(target + ".part") != entry.size:
                try:
                    os.remove(target + ".part")
                except OSError:
                    pass
                raise IOError("size mismatch after download")
            #movemos el .part al nombre final
            os.replace(target + ".part", target)
            return (name, "ok")
        except Exception:
            #en fallo, intentamos limpiar el parcial y aplicamos backoff si hay reintentos
            try:
                if os.path.exists(target + ".part"):
                    os.remove(target + ".part")
            except OSError:
                pass
            if attempt == max_retries:
                return (name, "failed")
            time.sleep((backoff ** attempt) + 0.25 * attempt)

#funcion principal que coordina listado, filtro y descargas concurrentes
def main() -> None:
    #listamos claves desde S3 y aplicamos filtro opcional
    all_entries = list_bucket_objects(BUCKET_URL)
    entries = filter_includes(all_entries, INCLUDE_REGEX)
    #si no hay nada que descargar, avisamos y salimos
    if not entries:
        print("No matching files found to download (after excluding JC-* and applying any include regex).")
        return
    #descargamos en paralelo usando ThreadPoolExecutor
    statuses = {"ok": 0, "skipped": 0, "failed": 0}
    s = session()
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = [ex.submit(download_one, s, e, DEFAULT_DEST) for e in entries]
        #usamos tqdm para ver el avance total de archivos
        for fut in tqdm(cf.as_completed(futures), total=len(futures), desc="Overall", unit="file"):
            name, status = fut.result()
            statuses[status] += 1
    #resumen final
    print(f"Done. ok={statuses['ok']}, skipped={statuses['skipped']}, failed={statuses['failed']}")

#punto de entrada
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAborted by user.")
        raise SystemExit(130)