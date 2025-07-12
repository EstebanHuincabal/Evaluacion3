import mysql.connector
from datetime import datetime
import anthropic

# Configuración de conexión a MySQL (XAMPP)
DB_CONFIG = {
    'host': '',
    'user': '',
    'password': '',
    'database': 'datamart_mermas',
    'port': 3306,
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_general_ci'
}

# Configurar el cliente de Anthropic
client = anthropic.Anthropic(api_key="")

# Estructura de la tabla 
ESTRUCTURA_TABLA = """
TABLE dim_tiempo (
        id_tiempo INTEGER PRIMARY KEY AUTO_INCREMENT,
        fecha DATE NOT NULL,
        dia INTEGER,
        mes INTEGER,
        anio INTEGER,
        semestre VARCHAR(10),
        nombre_mes VARCHAR(20),
        trimestre VARCHAR(20),
        dia_semana VARCHAR(20),
        feriado VARCHAR(20),
        dia_año INTEGER,
        es_finde VARCHAR(20))

TABLE dim_producto (
        id_producto INTEGER PRIMARY KEY AUTO_INCREMENT,
        codigo VARCHAR(50),
        nombre VARCHAR(100),
        categoria VARCHAR(50),
        abastecimiento VARCHAR(50),
        linea VARCHAR(50),
        seccion VARCHAR(50),
        negocio VARCHAR(50))

TABLE dim_tienda (
        id_tienda INTEGER PRIMARY KEY AUTO_INCREMENT,
        nombre VARCHAR(100),
        comuna VARCHAR(50),
        region VARCHAR(50),
        zonal VARCHAR(50)
    )

TABLE dim_motivo (
        id_motivo INTEGER PRIMARY KEY AUTO_INCREMENT,
        tipo_motivo VARCHAR(100),
        ubicacion_motivo VARCHAR(100),
        fecha_motivo DATE
    )

TABLE fact_merma (
        id_merma INTEGER PRIMARY KEY AUTO_INCREMENT,
        id_tiempo INTEGER,
        id_producto INTEGER,
        id_tienda INTEGER,
        id_motivo INTEGER,
        cantidad_merma DECIMAL(10, 2),
        monto_merma DECIMAL(12, 2),
        precio_producto DECIMAL(10, 2),
        FOREIGN KEY (id_tiempo) REFERENCES dim_tiempo(id_tiempo),
        FOREIGN KEY (id_producto) REFERENCES dim_producto(id_producto),
        FOREIGN KEY (id_tienda) REFERENCES dim_tienda(id_tienda),
        FOREIGN KEY (id_motivo) REFERENCES dim_motivo(id_motivo)
    )
"""

def obtener_consulta_sql(pregunta):
    prompt = f""" Dada la siguientes estructuras de tablas :
    {ESTRUCTURA_TABLA}
    Y la siguiente consulta en lenguaje natural:
    "{pregunta}"

    Genera una consulta SQL para MySQL que responda la pregunta del usuario. Sigue estas pautas:
    0. La tablas se llaman dim_tiempo, dim_producto, dim_tienda, dim_motivo y fact_merma.
    1. Utiliza LIKE para búsquedas de texto, permitiendo coincidencias parciales.
    2. Para hacer búsqueda insensible a mayúsculas y minúsculas utiliza LOWER() para tratar todos los datos en minúsculas.
    4. Si la consulta puede devolver múltiples resultados, usa GROUP BY para agrupar resultados similares.
    5. Incluye COUNT(*) o COUNT(DISTINCT...) cuando sea apropiado para contar resultados.
    6. Usa IFNULL cuando sea necesario para manejar valores null.
    7. Limita los resultados a 100 filas como máximo. Usa LIMIT 100.
    8. Incluye ORDER BY para ordenar los resultados de manera lógica.
    9. Si la consulta utiliza cálculos numéricos, usa funciones como MIN, MAX, AVG, SUM, u otras que se requieran y que sean válidas en MySQL.
    10. Si la consulta es sobre fechas, usa funciones apropiadas en MySQL para esto.
    11. Usa únicamente los nombres de columnas que están explícitamente definidos en la estructura de tabla provista. No inventes nombres como 'nombre_tienda' si solo existe 'nombre'.
    12. En la respuesta puedes incorporar datos de la tabla que sean útiles para que el usuario final tenga una respuesta clara.
    13. No generes bajo ningún caso instrucciones de tipo DDL (Create, drop) o DML diferentes de Select.
    14. las columnas 'cantidad_merma' es de unidades, mientras que 'monto_merma' es de cifra monetaria
    Responde solo con la consulta SQL, sin agregar nada más."""

    message = client.messages.create(
        model="claude-3-haiku-20240307",
        max_tokens=1000,
        temperature=0,
        messages=[
            {
                "role": "user",
                "content": prompt
            }
        ]
    )

    return message.content[0].text.strip()


def ejecutar_sql(sql):
    conn = None
    cursor = None
    try:
        # Establecer conexión
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        
        # Ejecutar la consulta
        cursor.execute(sql)
        results = cursor.fetchall()
        
        return results
        
    except mysql.connector.Error as err:
        print(f"Error MySQL: {err}")
        return []
    except Exception as e:
        print(f"Error general: {e}")
        return []
    finally:
        # Cerrar conexiones
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def generar_respuesta_final(resultado_sql, pregunta):
    prompt = f"""Dada la siguiente pregunta:
    "{pregunta}"
    Y los siguientes resultados de la consulta SQL:
    {resultado_sql}
    Genera una respuesta en lenguage natural, entendible para un usuario de negocio en el ámbito empresarial de supermercado y de RRHH con las siguientes reglas:
    1. Responde directamente sin hacer mención a SQL u otros términos técnicos.
    2. Usa un lenguaje claro, profesional, como si estuvieses conversando con el usuario que efectúa la pregunta.
    3. Presenta la información de manera organizada y fácil de entender. Trata de estructurar los datos y ordenarlos al momento de responder.
    4. Si los datos son limitados o incompletos, proporciona una respuesta con la información disponible y no pidas disculpas.
    6. Si los datos incluyen cifras monetarias, utiliza el símbolo $ e incorpora separadores de miles. Los datos monetarios son siempre en pesos chilenos.
    7. No agregues información que no esté explicitamente en los datos obtenidos.
    8. Si la respuesta no puede ser respondida, indica amablemente que no hay datos disponibles e invita a una nueva pregunta.
    9. No agregues a menos que se solicite un análisis de resultados. Sólo entrégalos de manera entendible sin emitir opinión a menos que se solicite.
    10. No hagas supuestos ni hagas sugerencias con los datos. Esto es muy importante.
    11. Envía el resultado de manera precisa y estructurada sin un análisis salvo que se solicite.
    12. Los resultados son utilizados en una conversión tipo chat, por tanto no saludes ni te despidas. Limita a entregar los resultados de manera clara.
    13. IMPORTANTE: Nunca menciones datos técnicos ni pidas disculpas.
    14. las columnas 'cantidad_merma' es de unidades, mientras que 'monto_merma' es de cifra monetaria.
    15. cuando se pregunte por cantidad de monto no responda con cifras monetarias 
    """
    message = client.messages.create(
        model="claude-3-haiku-20240307",
        max_tokens=1000,
        temperature=0,
        messages=[
            {
                "role": "user",
                "content": prompt
            }
        ]
    )

    return message.content[0].text.strip()

def main():
    
    n = 1
    while True:
        pregunta = input(f"{n}.- Ingrese una pregunta (Ingrese 'salir' para finalizar): ")
        n += 1
        
        # Verificamos si desea finalizar
        if pregunta.lower() == 'salir':
            print("Chat finalizado")
            break
        
        # Intentamos ejecutar las operaciones de consulta
        try:
            # Obtenemos consulta SQL
            sql_query = obtener_consulta_sql(pregunta)
            # print(f"SQL GENERADO: {sql_query}")  # Descomenta para debug

            # Ejecutamos la consulta SQL
            sql_resultados = ejecutar_sql(sql_query)
            # print(f"RESULTADOS CONSULTA: {sql_resultados}")  # Descomenta para debug

            # Generamos la respuesta final al usuario
            respuesta_final = generar_respuesta_final(sql_resultados, pregunta)
            print(f"RESPUESTA: {respuesta_final}\n")
            
        except Exception as e:
            print(f"Error: {e}\n")


if __name__ == "__main__":
    main()