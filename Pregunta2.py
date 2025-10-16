import os
import pandas as pd
import sqlite3
import smtplib
import glob
from email.message import EmailMessage
from dotenv import load_dotenv
from pymongo import MongoClient

def main():
    # ✅ Leer el archivo local desde la carpeta /data
    df = pd.read_csv("./data/winemag-data-130k-v2.csv", index_col=0)

    # --- RENOMBRAR COLUMNAS ---
    df = df.rename(columns={
        "country": "pais",
        "points": "puntuacion",
        "price": "precio",
        "variety": "variedad",
        "province": "provincia"
    })

    # --- NUEVAS COLUMNAS ---
    df["calidad"] = pd.cut(df["puntuacion"], bins=[0, 85, 90, 95, 100],
                           labels=["Regular", "Buena", "Excelente", "Premium"], include_lowest=True)
    df["precio_categoria"] = pd.cut(df["precio"], bins=[0, 20, 50, 100, 500],
                                    labels=["Económico", "Accesible", "Costoso", "Premium"], include_lowest=True)
    df["relacion_puntos_precio"] = df["puntuacion"] / df["precio"]
    df["continente"] = "Desconocido"

       # --- MAPEAR PAÍS A CONTINENTE ---
    print("Cargando mapeo de países a continentes...")

    # Descargar el CSV de países y continentes desde el enlace original
    url = "https://raw.githubusercontent.com/plotly/datasets/master/2014_world_gdp_with_codes.csv"

    try:
        paises_csv = pd.read_csv(url)
        paises_csv.columns = paises_csv.columns.str.strip().str.lower()

        print("\nColumnas del archivo de países:")
        print(paises_csv.columns)
    except Exception as e:
        print(f"No se pudo descargar el archivo desde la URL: {e}")
        paises_csv = None

    # Solo hacer el merge si se cargó correctamente
    if paises_csv is not None and "country" in paises_csv.columns and "continent" in paises_csv.columns:
        df = df.merge(
            paises_csv[["country", "continent"]],
            how="left",
            left_on="pais",
            right_on="country"
        )
        df["continente"] = df["continent"].fillna("Desconocido")
        df = df.drop(columns=["country", "continent"])
    else:
        df["continente"] = "Desconocido"

    print("\nAsignación de continentes completada.")

    # --- REPORTES ---

    # 1️ Promedio de puntuación y precio por continente
    rep1 = df.groupby("continente", as_index=False).agg({
        "puntuacion": "mean",
        "precio": "mean"
    }).rename(columns={"puntuacion": "prom_puntuacion", "precio": "prom_precio"})

    # 2️ Mejores vinos por país
    rep2 = df.loc[df.groupby("pais")["puntuacion"].idxmax(), ["pais", "variedad", "puntuacion", "precio"]].reset_index(drop=True)

    # 3️ Cantidad de vinos por categoría de precio y calidad
    rep3 = df.groupby(["precio_categoria", "calidad"], as_index=False).size().rename(columns={"size": "cantidad"})

    # 4️ Top 10 países con mejor relación puntos/precio
    rep4 = df.groupby("pais", as_index=False)["relacion_puntos_precio"].mean().sort_values(by="relacion_puntos_precio", ascending=False).head(10)

    reports = {
        "reporte1_continente": rep1,
        "reporte2_mejores_vinos": rep2,
        "reporte3_categoria_calidad": rep3,
        "reporte4_top10_relacion": rep4
    }

    # --- EXPORTAR ---

    print("Exportando reportes...")

    # Crear carpeta /reportes si no existe
    os.makedirs("reportes", exist_ok=True)

    # Exportar cada reporte en formato distinto
    reports["reporte1_continente"].to_csv("reportes/reporte1_continente.csv", index=False)
    reports["reporte2_mejores_vinos"].to_excel("reportes/reporte2_mejores_vinos.xlsx", index=False)

    # Exportar reporte 3 a base de datos SQLite
    conn = sqlite3.connect("reportes/reporte3_categoria_calidad.sqlite")
    reports["reporte3_categoria_calidad"].to_sql("categoria_calidad", conn, if_exists="replace", index=False)
    conn.close()

    # --- EXPORTAR REPORTE 4 A MONGODB ---
    print("Exportando reporte 4 a MongoDB...")


    # Cargar credenciales desde .env
    load_dotenv()

    MONGO_USER = os.getenv("MONGO_USER")
    MONGO_PASS = os.getenv("MONGO_PASS")
    MONGO_CLUSTER = os.getenv("MONGO_CLUSTER")
    MONGO_DB = os.getenv("MONGO_DB")

    # Construir URI de conexión
    mongo_uri = f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@{MONGO_CLUSTER}/?retryWrites=true&w=majority"

    try:
        client = MongoClient(mongo_uri)
        db = client[MONGO_DB]

        # Solo subimos el reporte 4
        rep4_name = "reporte4_top10_relacion"
        rep4_data = reports[rep4_name]

        collection = db[rep4_name]
        collection.delete_many({})  # limpia colección anterior
        collection.insert_many(rep4_data.to_dict("records"))

        client.close()
        print(f"✅ {rep4_name} exportado correctamente a MongoDB Atlas.")
    except Exception as e:
        print(f"❌ Error al exportar a MongoDB: {e}")


def enviar_reporte_por_correo():
    """
    Envía por correo los reportes generados (por ejemplo, en CSV).
    """
    load_dotenv()

    EMAIL_USER = os.getenv("EMAIL_USER")
    EMAIL_PASS = os.getenv("EMAIL_PASS")

    if not EMAIL_USER or not EMAIL_PASS:
        print("⚠️ No se encontraron credenciales de correo en el archivo .env")
        return

    # Configura el mensaje
    msg = EmailMessage()
    msg["Subject"] = "Reportes generados automáticamente"
    msg["From"] = EMAIL_USER
    msg["To"] = "nunezortizrenato@gmail.com"  # <- cámbialo por tu correo o el destinatario real
    msg.set_content("Adjunto los reportes generados automáticamente desde el script.")

    # Adjuntar los CSV generados
    for filepath in glob.glob("reportes/*.csv"): # <- Se puede modificar .csv por el archivo que se prefiera
        with open(filepath, "rb") as f:
            msg.add_attachment(f.read(), maintype="text", subtype="csv", filename=os.path.basename(filepath))

    # Enviar correo mediante SMTP (Gmail)
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(EMAIL_USER, EMAIL_PASS)
            smtp.send_message(msg)
        print("✅ Correo enviado correctamente.")
    except Exception as e:
        print(f"❌ Error al enviar el correo: {e}")


    print("\n✅ Reportes exportados correctamente en carpeta 'reportes':")
    print(" - reportes/reporte1_continente.csv")
    print(" - reportes/reporte2_mejores_vinos.xlsx")
    print(" - reportes/reporte3_categoria_calidad.sqlite")

    # --- ENVÍO POR CORREO ---
enviar_reporte_por_correo()


if __name__ == "__main__":
    main()
