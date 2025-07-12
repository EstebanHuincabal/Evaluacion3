import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
from mysql.connector import errorcode


def crear_datamart_mermas():
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',
            database='datamart_mermas'
        )
        cursor = conn.cursor()

        print("Creando tablas en la base de datos 'datamart_mermas'...")

        # DROP TABLES opcional para limpieza
        cursor.execute("DROP TABLE IF EXISTS fact_merma")
        cursor.execute("DROP TABLE IF EXISTS dim_motivo")
        cursor.execute("DROP TABLE IF EXISTS dim_tienda")
        cursor.execute("DROP TABLE IF EXISTS dim_producto")
        cursor.execute("DROP TABLE IF EXISTS dim_tiempo")

        cursor.execute("""
        CREATE TABLE dim_tiempo (
            id_tiempo INT AUTO_INCREMENT PRIMARY KEY,
            fecha DATE NOT NULL,
            dia INT,
            mes INT,
            anio INT,
            semestre VARCHAR(10),
            nombre_mes VARCHAR(20),
            trimestre VARCHAR(20),
            dia_semana VARCHAR(20),
            feriado VARCHAR(20),
            dia_año INT,
            es_finde VARCHAR(20)
        )
        """)

        cursor.execute("""
        CREATE TABLE dim_producto (
            id_producto INT AUTO_INCREMENT PRIMARY KEY,
            codigo VARCHAR(50),
            nombre VARCHAR(100),
            categoria VARCHAR(50),
            abastecimiento VARCHAR(50),
            linea VARCHAR(50),
            seccion VARCHAR(50),
            negocio VARCHAR(50)
        )
        """)

        cursor.execute("""
        CREATE TABLE dim_tienda (
            id_tienda INT AUTO_INCREMENT PRIMARY KEY,
            nombre VARCHAR(100),
            comuna VARCHAR(50),
            region VARCHAR(50),
            zonal VARCHAR(50)
        )
        """)

        cursor.execute("""
        CREATE TABLE dim_motivo (
            id_motivo INT AUTO_INCREMENT PRIMARY KEY,
            tipo_motivo VARCHAR(100),
            ubicacion_motivo VARCHAR(100),
            fecha_motivo DATE
        )
        """)

        cursor.execute("""
        CREATE TABLE fact_merma (
            id_merma INT AUTO_INCREMENT PRIMARY KEY,
            id_tiempo INT,
            id_producto INT,
            id_tienda INT,
            id_motivo INT,
            cantidad_merma DECIMAL(10, 2),
            monto_merma DECIMAL(12, 2),
            precio_producto DECIMAL(10, 2),
            FOREIGN KEY (id_tiempo) REFERENCES dim_tiempo(id_tiempo),
            FOREIGN KEY (id_producto) REFERENCES dim_producto(id_producto),
            FOREIGN KEY (id_tienda) REFERENCES dim_tienda(id_tienda),
            FOREIGN KEY (id_motivo) REFERENCES dim_motivo(id_motivo)
        )
        """)

        conn.commit()
        conn.close()
        print("🎉 Tablas creadas exitosamente en MySQL")

    except mysql.connector.Error as err:
        print(f"Error: {err}")


def etl_datamart_mermas():
    # conexión sqlalchemy para pandas
    engine = create_engine("mysql+mysqlconnector://root:@localhost/datamart_mermas")

    df = pd.read_csv("mermas_utf8_clean.csv", parse_dates=["fecha"])
    df = df[df['fecha'].notnull()]
    df['fecha_motivo'] = df['fecha']

    df.rename(columns={
        'codigo_producto': 'codigo',
        'descripcion': 'nombre',
        'tienda': 'nombre_tienda',
        'motivo': 'tipo_motivo',
    }, inplace=True)

    # Dimensión tiempo
    df_tiempo = df[['fecha']].drop_duplicates().copy()
    df_tiempo['dia'] = df_tiempo['fecha'].dt.day
    df_tiempo['mes'] = df_tiempo['fecha'].dt.month
    df_tiempo['anio'] = df_tiempo['fecha'].dt.year
    df_tiempo['semestre'] = df_tiempo['mes'].apply(lambda x: '1° Semestre' if x <= 6 else '2° Semestre')
    df_tiempo['nombre_mes'] = df_tiempo['fecha'].dt.strftime('%B')
    df_tiempo['trimestre'] = df_tiempo['mes'].apply(lambda x: f'T{((x-1)//3)+1}')
    df_tiempo['dia_semana'] = df_tiempo['fecha'].dt.day_name()
    df_tiempo['feriado'] = 'No'
    df_tiempo['dia_año'] = df_tiempo['fecha'].dt.dayofyear
    df_tiempo['es_finde'] = df_tiempo['dia_semana'].isin(['Saturday', 'Sunday']).map({True: 'Sí', False: 'No'})

    df_tiempo.to_sql('dim_tiempo', con=engine, if_exists='append', index=False)

    dim_tiempo_sql = pd.read_sql("SELECT * FROM dim_tiempo", engine)
    dim_tiempo_sql['fecha'] = pd.to_datetime(dim_tiempo_sql['fecha'])
    df = df.merge(dim_tiempo_sql, on="fecha", how="left")

    # Dimensión producto
    cols_producto = ['codigo', 'nombre', 'categoria', 'abastecimiento', 'linea', 'seccion', 'negocio']
    df_producto = df[cols_producto].drop_duplicates()
    df_producto.to_sql('dim_producto', con=engine, if_exists='append', index=False)

    dim_producto_sql = pd.read_sql("SELECT * FROM dim_producto", engine)
    df = df.merge(dim_producto_sql, on=cols_producto, how='left')

    # Dimensión tienda
    cols_tienda = ['nombre_tienda', 'comuna', 'region', 'zonal']
    df_tienda = df[cols_tienda].drop_duplicates().copy()
    df_tienda.columns = ['nombre', 'comuna', 'region', 'zonal']
    df_tienda.to_sql('dim_tienda', con=engine, if_exists='append', index=False)

    dim_tienda_sql = pd.read_sql("SELECT * FROM dim_tienda", engine)
    df = df.merge(dim_tienda_sql, left_on=cols_tienda,
                  right_on=['nombre', 'comuna', 'region', 'zonal'], how='left')

    # Dimensión motivo
    cols_motivo = ['tipo_motivo', 'ubicacion_motivo', 'fecha_motivo']
    df_motivo = df[cols_motivo].drop_duplicates()
    df_motivo.to_sql('dim_motivo', con=engine, if_exists='append', index=False)

    dim_motivo_sql = pd.read_sql("SELECT * FROM dim_motivo", engine)
    dim_motivo_sql['fecha_motivo'] = pd.to_datetime(dim_motivo_sql['fecha_motivo'])
    df = df.merge(dim_motivo_sql, on=cols_motivo, how='left')

    # Hechos
    df.rename(columns={
        'merma_unidad_p': 'cantidad_merma',
        'merma_monto_p': 'monto_merma'
    }, inplace=True)
    df['precio_producto'] = round(df['monto_merma'] / df['cantidad_merma'])

    df_fact = df[['id_tiempo', 'id_producto', 'id_tienda', 'id_motivo',
                  'cantidad_merma', 'monto_merma', 'precio_producto']]
    df_fact.to_sql('fact_merma', con=engine, if_exists='append', index=False)

    print("✅ ETL ejecutado exitosamente en MySQL")


if __name__ == "__main__":
    crear_datamart_mermas()
    etl_datamart_mermas()
