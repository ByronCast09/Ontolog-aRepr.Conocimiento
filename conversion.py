import pandas as pd
import json
from urllib.parse import quote
from datetime import datetime
import re


def clean_uri_string(text):
    """Limpia una cadena para usar en URIs"""
    if pd.isna(text) or text == "":
        return None
    # Reemplaza caracteres problemáticos y espacios
    cleaned = re.sub(r'[^\w\s-]', '', str(text))
    cleaned = re.sub(r'\s+', '_', cleaned.strip())
    return quote(cleaned)


def clean_literal_string(text):
    """Limpia una cadena para usar como literal en TTL"""
    if pd.isna(text) or text == "":
        return None
    # Escapa comillas y caracteres especiales
    return str(text).replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')


def parse_delimited_field(field_value, debug=False):
    """Parsea campos delimitados por || del dataset RAWG"""
    if pd.isna(field_value) or field_value == "" or str(field_value).strip() == "":
        return []

    if debug:
        print(f"Valor original: {repr(field_value)}")

    try:
        field_str = str(field_value).strip()

        # El formato del dataset RAWG usa || como separador
        if '||' in field_str:
            # Múltiples valores separados por ||
            values = [v.strip() for v in field_str.split('||') if v.strip()]
        else:
            # Valor único
            values = [field_str] if field_str else []

        # Convierte a formato dict con 'name' para consistencia con el código existente
        result = [{'name': value} for value in values if value]

        if debug:
            print(f"Valores parseados: {result}")

        return result

    except Exception as e:
        if debug:
            print(f"Error parseando: {e}")
        return []


def format_date(date_str):
    """Formatea fecha para TTL"""
    if pd.isna(date_str) or date_str == "":
        return None
    try:
        # Asume formato YYYY-MM-DD
        datetime.strptime(str(date_str), '%Y-%m-%d')
        return str(date_str)
    except ValueError:
        return None


def generate_ttl_from_rawg_dataset(csv_file_path, output_file_path, limit=None):
    """
    Genera un archivo TTL a partir del dataset RAWG

    Args:
        csv_file_path: Ruta al archivo CSV
        output_file_path: Ruta donde guardar el archivo TTL
        limit: Número máximo de juegos a procesar (None para todos)
    """

    # Lee el CSV
    df = pd.read_csv(csv_file_path)

    if limit:
        df = df.head(limit)

    # Comienza el archivo TTL
    ttl_content = """@prefix : <http://www.semanticweb.org/kevin/ontologies/2025/7/VideoGames#> .
@prefix dcterms: <http://purl.org/dc/terms/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix schema: <http://schema.org/> .
@prefix rawg: <http://rawg.io/ontology#> .
@prefix vgo: <http://purl.org/net/VideoGameOntology#> .

"""

    # Sets para almacenar entidades únicas
    platforms_set = set()
    developers_set = set()
    publishers_set = set()
    genres_set = set()
    esrb_ratings_set = set()

    # Primer pase: recopilar todas las entidades únicas
    print("Recopilando entidades únicas...")
    for index, row in df.iterrows():
        # Plataformas
        platforms = parse_delimited_field(row.get('platforms', ''))
        for platform in platforms:
            if isinstance(platform, dict) and 'name' in platform:
                platforms_set.add(platform['name'])

        # Desarrolladores
        developers = parse_delimited_field(row.get('developers', ''))
        for dev in developers:
            if isinstance(dev, dict) and 'name' in dev:
                developers_set.add(dev['name'])

        # Editores
        publishers = parse_delimited_field(row.get('publishers', ''))
        for pub in publishers:
            if isinstance(pub, dict) and 'name' in pub:
                publishers_set.add(pub['name'])

        # Géneros
        genres = parse_delimited_field(row.get('genres', ''))
        for genre in genres:
            if isinstance(genre, dict) and 'name' in genre:
                genres_set.add(genre['name'])

        # Rating ESRB - Este podría ser un valor simple sin ||
        esrb = row.get('esrb_rating', '')
        if not pd.isna(esrb) and esrb != "":
            esrb_ratings_set.add(str(esrb).strip())

    # Genera las entidades auxiliares
    print("Generando entidades auxiliares...")

    # Plataformas
    ttl_content += "# Plataformas\n"
    for platform in platforms_set:
        clean_platform = clean_uri_string(platform)
        if clean_platform:
            ttl_content += f":platform_{clean_platform} rdf:type schema:VideoGamePlatform ;\n"
            ttl_content += f'    schema:name "{clean_literal_string(platform)}" .\n\n'

    # Desarrolladores
    ttl_content += "# Desarrolladores\n"
    for developer in developers_set:
        clean_dev = clean_uri_string(developer)
        if clean_dev:
            ttl_content += f":developer_{clean_dev} rdf:type schema:Organization ;\n"
            ttl_content += f'    schema:name "{clean_literal_string(developer)}" .\n\n'

    # Editores
    ttl_content += "# Editores\n"
    for publisher in publishers_set:
        clean_pub = clean_uri_string(publisher)
        if clean_pub:
            ttl_content += f":publisher_{clean_pub} rdf:type schema:Organization ;\n"
            ttl_content += f'    schema:name "{clean_literal_string(publisher)}" .\n\n'

    # Géneros
    ttl_content += "# Géneros\n"
    for genre in genres_set:
        clean_genre = clean_uri_string(genre)
        if clean_genre:
            ttl_content += f":genre_{clean_genre} rdf:type schema:Genre ;\n"
            ttl_content += f'    schema:name "{clean_literal_string(genre)}" .\n\n'

    # Ratings ESRB
    ttl_content += "# Ratings ESRB\n"
    for rating in esrb_ratings_set:
        clean_rating = clean_uri_string(rating)
        if clean_rating:
            ttl_content += f":esrb_{clean_rating} rdf:type schema:GameRating ;\n"
            ttl_content += f'    schema:name "{clean_literal_string(rating)}" .\n\n'

    # Genera los videojuegos
    print("Generando videojuegos...")
    ttl_content += "# Videojuegos\n"

    for index, row in df.iterrows():
        if index % 100 == 0:
            print(f"Procesando juego {index + 1}/{len(df)}")

        game_id = row.get('id', '')
        if pd.isna(game_id):
            continue

        game_uri = f":game_{game_id}"
        ttl_content += f"{game_uri} rdf:type schema:VideoGame"

        # Propiedades básicas
        if not pd.isna(row.get('id')):
            ttl_content += f' ;\n    dcterms:identifier "{row["id"]}"'

        if not pd.isna(row.get('name')) and row['name'] != "":
            ttl_content += f' ;\n    schema:name "{clean_literal_string(row["name"])}"'

        if not pd.isna(row.get('slug')) and row['slug'] != "":
            ttl_content += f' ;\n    schema:alternateName "{clean_literal_string(row["slug"])}"'

        # Fecha de lanzamiento
        release_date = format_date(row.get('released'))
        if release_date:
            ttl_content += f' ;\n    schema:datePublished "{release_date}"^^xsd:date'

        # URL del sitio web
        if not pd.isna(row.get('website')) and row['website'] != "":
            ttl_content += f' ;\n    schema:url "{row["website"]}"^^xsd:anyURI'

        # Propiedades numéricas
        numeric_props = [
            ('metacritic', 'schema:ratingValue', 'decimal'),
            ('rating', 'schema:bestRating', 'decimal'),
            ('playtime', 'vgo:averagePlayTime', 'integer'),
            ('achievements_count', 'rawg:achievementCount', 'integer'),
            ('ratings_count', 'schema:ratingCount', 'integer'),
            ('suggestions_count', 'rawg:suggestionCount', 'integer'),
            ('game_series_count', 'rawg:gameSeriesCount', 'integer'),
            ('reviews_count', 'schema:reviewCount', 'integer'),
            ('added_status_yet', 'rawg:addedStatusYet', 'integer'),
            ('added_status_owned', 'rawg:addedStatusOwned', 'integer'),
            ('added_status_beaten', 'rawg:addedStatusBeaten', 'integer'),
            ('added_status_toplay', 'rawg:addedStatusToPlay', 'integer'),
            ('added_status_dropped', 'rawg:addedStatusDropped', 'integer'),
            ('added_status_playing', 'rawg:addedStatusPlaying', 'integer')
        ]

        for col, prop, data_type in numeric_props:
            value = row.get(col)
            if not pd.isna(value) and str(value) != "" and str(value) != "0.0":
                if data_type == 'integer':
                    ttl_content += f' ;\n    {prop} "{int(float(value))}"^^xsd:integer'
                else:
                    ttl_content += f' ;\n    {prop} "{value}"^^xsd:decimal'

        # Propiedades booleanas
        if not pd.isna(row.get('tba')):
            tba_value = "true" if str(row['tba']).lower() in ['true', '1'] else "false"
            ttl_content += f' ;\n    rawg:toBeAnnounced "{tba_value}"^^xsd:boolean'

        # Fecha de actualización
        if not pd.isna(row.get('updated')) and row['updated'] != "":
            ttl_content += f' ;\n    dcterms:modified "{row["updated"]}"^^xsd:dateTime'

        # Relaciones con otras entidades

        # Plataformas
        platforms = parse_delimited_field(row.get('platforms', ''))
        for platform in platforms:
            if isinstance(platform, dict) and 'name' in platform:
                clean_platform = clean_uri_string(platform['name'])
                if clean_platform:
                    ttl_content += f' ;\n    schema:gamePlatform :platform_{clean_platform}'

        # Desarrolladores
        developers = parse_delimited_field(row.get('developers', ''))
        for dev in developers:
            if isinstance(dev, dict) and 'name' in dev:
                clean_dev = clean_uri_string(dev['name'])
                if clean_dev:
                    ttl_content += f' ;\n    schema:developer :developer_{clean_dev}'

        # Editores
        publishers = parse_delimited_field(row.get('publishers', ''))
        for pub in publishers:
            if isinstance(pub, dict) and 'name' in pub:
                clean_pub = clean_uri_string(pub['name'])
                if clean_pub:
                    ttl_content += f' ;\n    schema:publisher :publisher_{clean_pub}'

        # Géneros
        genres = parse_delimited_field(row.get('genres', ''))
        for genre in genres:
            if isinstance(genre, dict) and 'name' in genre:
                clean_genre = clean_uri_string(genre['name'])
                if clean_genre:
                    ttl_content += f' ;\n    schema:genre :genre_{clean_genre}'

        # Rating ESRB - Valor simple
        esrb = row.get('esrb_rating', '')
        if not pd.isna(esrb) and esrb != "":
            clean_rating = clean_uri_string(str(esrb).strip())
            if clean_rating:
                ttl_content += f' ;\n    schema:contentRating :esrb_{clean_rating}'

        ttl_content += " .\n\n"

    # Guarda el archivo
    with open(output_file_path, 'w', encoding='utf-8') as f:
        f.write(ttl_content)

    print(f"Archivo TTL generado exitosamente: {output_file_path}")
    print(f"Procesados {len(df)} juegos")
    print(f"- {len(platforms_set)} plataformas únicas")
    print(f"- {len(developers_set)} desarrolladores únicos")
    print(f"- {len(publishers_set)} editores únicos")
    print(f"- {len(genres_set)} géneros únicos")
    print(f"- {len(esrb_ratings_set)} ratings ESRB únicos")


def diagnose_csv_format(csv_file_path, num_samples=5):
    """Diagnostica el formato de los campos JSON en el CSV"""
    print("=== DIAGNÓSTICO DEL FORMATO CSV ===")

    try:
        # Lee solo las primeras filas para diagnóstico
        df = pd.read_csv(csv_file_path, nrows=num_samples)

        print(f"Columnas encontradas: {list(df.columns)}")
        print(f"Forma del dataset: {df.shape}")
        print()

        # Campos que deberían contener datos delimitados
        delimited_fields = ['platforms', 'developers', 'genres', 'publishers', 'esrb_rating']

        for field in delimited_fields:
            if field in df.columns:
                print(f"=== CAMPO: {field} ===")
                for i, value in enumerate(df[field].head(3)):
                    print(f"  Muestra {i + 1}: {repr(value)}")
                    if not pd.isna(value) and value != "":
                        print(f"    Tipo: {type(value)}")
                        print(f"    Longitud: {len(str(value))}")
                        # Intenta parsear con el nuevo método
                        result = parse_delimited_field(value, debug=True)
                        print(f"    Resultado parseado: {result}")
                    print()
            else:
                print(f"ADVERTENCIA: Campo '{field}' no encontrado en el CSV")

        # Muestra una fila completa como ejemplo
        print("=== PRIMERA FILA COMPLETA ===")
        if len(df) > 0:
            for col in df.columns:
                print(f"{col}: {repr(df.iloc[0][col])}")

    except Exception as e:
        print(f"Error en diagnóstico: {e}")


# Ejemplo de uso
if _name_ == "_main_":
    # Cambia estas rutas según tu configuración
    csv_file = "rawg_games.csv"  # Ruta a tu archivo CSV
    output_file = "videojuegos_dataset.ttl"  # Archivo TTL de salida

    # Procesa solo los primeros 1000 juegos para prueba (None para todos)
    limit = 1000

    try:
        # Primero ejecuta el diagnóstico
        print("Ejecutando diagnóstico...")
        diagnose_csv_format(csv_file, num_samples=10)

        print("\n" + "=" * 50)
        response = input("¿Continuar con la generación del TTL? (s/n): ")
        if response.lower() in ['s', 'si', 'y', 'yes']:
            generate_ttl_from_rawg_dataset(csv_file, output_file, limit)

    except FileNotFoundError:
        print(f"Error: No se encontró el archivo {csv_file}")
        print("Asegúrate de que el archivo CSV esté en la misma carpeta que este script")
    except Exception as e:
        print(f"Error durante el procesamiento: {str(e)}")