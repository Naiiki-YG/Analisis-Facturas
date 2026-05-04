import pdfplumber
import pandas as pd
import re
import sqlite3
from pathlib import Path

# ---------------- CONFIG ----------------
DATA_PATH = Path("../Facturas/dataset_facturas_2025")
OUTPUT_PATH = Path("data")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

DB_PATH = OUTPUT_PATH / "ventas_2025.db"

# ---------------- EXTRACT ----------------
def extract_text_from_pdf(pdf_path):
    text = ""

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for i, page in enumerate(pdf.pages):
                try:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
                except Exception as e:
                    print(f"[WARN] Página {i} falló en {pdf_path.name}: {e}")
                    continue

    except Exception as e:
        print(f"[ERROR] No se pudo leer {pdf_path.name}: {e}")
        return None

    return text


# ---------------- TRANSFORM ----------------
def parse_invoice(text):
    text = re.sub(r"\s+", " ", text)

    # -------- HEADER --------
    cliente = re.search(r"Cliente:\s*(.*?)\s+Factura", text)
    factura = re.search(r"Factura N°:\s*(F\d+-\d+)", text)
    fecha = re.search(r"Fecha de Emisi\w+:\s*(\d{2}/\d{2}/\d{4})", text)

    cliente = cliente.group(1).strip() if cliente else None
    factura = factura.group(1).strip() if factura else None
    fecha = pd.to_datetime(fecha.group(1), dayfirst=True, errors="coerce") if fecha else None

    # -------- ITEMS --------
    items = []

    pattern = r"([A-Za-zÁÉÍÓÚñÑ0-9\s]+?)\s+(\d+)\s+([\d,]+\.\d{2})"

    for match in re.finditer(pattern, text):
        descripcion = match.group(1).strip()
        cantidad = int(match.group(2))
        total = float(match.group(3).replace(",", ""))

        # limpieza
        if not descripcion or cantidad <= 0:
            continue

        if any(x in descripcion for x in ["Subtotal", "IVA", "TOTAL"]):
            continue

        if len(descripcion) < 3:
            continue

        items.append({
            "factura_id": factura,
            "producto": descripcion,
            "cantidad": cantidad,
            "total": total
        })

    # -------- TOTALES --------
    subtotal = re.search(r"Subtotal:\s*([\d,]+\.\d{2})", text)
    iva = re.search(r"IVA\s*\(.*?\):\s*([\d,]+\.\d{2})", text)
    total_final = re.search(r"TOTAL:\s*([\d,]+\.\d{2})", text)
    
    subtotal = float(subtotal.group(1).replace(",", "")) if subtotal else None
    iva = float(iva.group(1).replace(",", "")) if iva else None
    total_final = float(total_final.group(1).replace(",", "")) if total_final else None

    header = {
        "factura_id": factura,
        "cliente": cliente,
        "fecha": fecha,
        "subtotal": subtotal,
        "iva": iva,
        "total": total_final
    }

    return header, items


# ---------------- VALIDATION ----------------
def validate_invoice(header, items):

    if not header["factura_id"]:
        return False

    if header["subtotal"] is None or header["iva"] is None or header["total"] is None:
        return False

    iva_calc = round(header["subtotal"] * 0.19, 2)
    total_items = sum(i["total"] for i in items)

    # tolerancias amplias (PDF ≠ base de datos)
    iva_ok = abs(iva_calc - header["iva"]) <= 20
    subtotal_ok = abs(total_items - header["subtotal"]) <= 50
    total_ok = abs(header["subtotal"] + header["iva"] - header["total"]) <= 50

    return iva_ok and subtotal_ok and total_ok

# ---------------- LOAD ----------------
def load_to_sqlite(df_facturas, df_items):
    conn = sqlite3.connect(DB_PATH)

    df_facturas.to_sql("facturas", conn, if_exists="replace", index=False)
    df_items.to_sql("items", conn, if_exists="replace", index=False)

    conn.close()


# ---------------- PIPELINE ----------------
def main():
    all_facturas = []
    all_items = []

    for pdf_file in sorted(DATA_PATH.glob("*.pdf")):
        print(f"Procesando: {pdf_file.name}")

        text = extract_text_from_pdf(pdf_file)

        if not text:
            continue

        header, items = parse_invoice(text)

        if not items:
            print(f"[WARN] Sin items: {pdf_file.name}")
            continue

        is_valid = validate_invoice(header, items)
        header["valida"] = is_valid
        
        if not is_valid:
            print(f"[WARN] Factura con inconsistencias: {header['factura_id']}")

        all_facturas.append(header)
        all_items.extend(items)

    # -------- DATAFRAMES --------
    df_facturas = pd.DataFrame(all_facturas)
    df_items = pd.DataFrame(all_items)

    # -------- LIMPIEZA --------
    df_items = df_items.drop_duplicates(
        subset=["factura_id", "producto", "cantidad", "total"]
    )

    # IDs
    df_items["item_id"] = range(1, len(df_items) + 1)

    # -------- VALIDACIÓN FINAL --------
    if df_facturas.empty:
        print("❌ No hay facturas válidas")
        return

    if df_items.empty:
        print("❌ No hay items válidos")
        return

    # -------- EXPORT --------
    df_facturas.to_csv(OUTPUT_PATH / "facturas.csv", index=False)
    df_items.to_csv(OUTPUT_PATH / "items.csv", index=False)

    load_to_sqlite(df_facturas, df_items)

    print("✅ ETL completado correctamente")


if __name__ == "__main__":
    main()