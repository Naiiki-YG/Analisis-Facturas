import pdfplumber
import pandas as pd
import re
import sqlite3
from pathlib import Path

# configuración
DATA_PATH = Path("../Facturas/dataset_facturas_2025")
OUTPUT_PATH = Path("data")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
DB_PATH = OUTPUT_PATH / "ventas_2025.db"
#proceso de extracción

def extract_text_from_pdf(pdf_path):
    text = ""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                page_text =page.extract_text()
                if page_text : #evita errores
                    text+= page_text + "\n"
    except Exception as e:
        print(f"error leyendo{pdf_path}:{e}")
        return None
    return text


# proceso de transformación
def parse_invoice(text):
    cliente = re.search(r"Cliente:\s*(.+)",text)
    fecha = re.search(r"Fecha de Emisi\w+:\s*(\d{2}/\d{2}/\d{4})",text)
    factura = re.search(r"Factura N°:\s*(\S+)", text)

    cliente = cliente.group(1).strip() if cliente else None
    fecha =pd.to_datetime(fecha.group(1),format="mixed", dayfirst=True, errors="coerce") if fecha else None
    factura = factura.group(1).strip() if factura else None


    items = []
    pattern = r"(.+?)\s+(\d+)\s+([\d]+\.[\d]+)"
    print(pattern)

    for match in re.finditer(pattern, text):
        descripcion =match.group(1).strip()
        cantidad = int(match.group(2))
        total =float(match.group(3))

        if "Subtotal" in descripcion or "IVA" in descripcion :
            continue

        items.append({
            "factura": factura,
            "descripcion": descripcion,
            "cantidad": cantidad,
            "total":total
        })
    subtotal = re.search(r"Subtotal:\s*([\d\.]+)", text)
    iva = re.search(r"IVA.*:\s*([\d\.]+)", text)
    total_final = re.search(r"TOTAL:\s*([\d\.]+)", text)

    subtotal =float(subtotal.group(1)) if subtotal else None
    iva =float(iva.group(1)) if iva else None
    total_final =float(total_final.group(1)) if total_final else None

    return{
        "factura":factura,
        "cliente":cliente,
        "fecha":fecha,
        "subtotal": subtotal,
        "iva":iva,
        "total": total_final
    }, items

#Proceso de validación
def validate_invoice(header):
    if header["subtotal"] is None or header["iva"] is None:
        return False
    
    iva_calc=round(header["subtotal"]*0.19, 2)
    return abs(iva_calc - header["iva"])<1


#load
def load_to_sqlite(df_facturas, df_items):
    conn = sqlite3.connect(DB_PATH)

    df_facturas.to_sql("facturas", conn, if_exists="replace", index=False)
    df_items.to_sql("items", conn, if_exists="replace", index=False)

    conn.close()

#pipeline
def main():
    all_facturas =[]
    all_items=[]
    
    for pdf_file in DATA_PATH.glob("*.pdf"):
        print(f"Procesando: {pdf_file.name}")

        text =extract_text_from_pdf(pdf_file)
        
        if not text: 
            print(f"saltando archivos:{pdf_file.name}")
            continue

        header, items =parse_invoice(text)

        if not validate_invoice(header):
            print(f"Error en validación IVA: {header['factura']}")
            continue

        all_facturas.append(header)
        all_items.extend(items)

    
    df_facturas = pd.DataFrame(all_facturas)
    df_items =pd.DataFrame(all_items)

    df_facturas.to_csv(OUTPUT_PATH / "facturas.csv", index=False)
    df_items.to_csv(OUTPUT_PATH / "items.csv", index=False)
    # Guardar DB
    load_to_sqlite(df_facturas, df_items)

    print("ETL completado")


if __name__ == "__main__":
    main()