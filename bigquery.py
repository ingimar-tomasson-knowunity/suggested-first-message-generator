from google.cloud import bigquery

client = bigquery.Client(project="knowunity-data-prod")


def fetch_countries() -> list[tuple[int, str]]:
    """Fetch country id–name pairs from BigQuery."""
    query = """
SELECT
    id,
    english_name
FROM
    `knowunity-data-prod.knowunity_backend_public.country`
"""
    query_job = client.query(query)
    results = list(query_job.result())

    return [(r.id, r.english_name) for r in results]


def fetch_subjects() -> list[tuple[int, int, str]]:
    """Fetch subject id, country_id and long_name from BigQuery."""
    query = """
SELECT
    id,
    country_id,
    long_name
FROM
    `knowunity-data-prod.knowunity_backend_public.subject`
"""
    query_job = client.query(query)
    results = list(query_job.result())

    return [(r.id, r.country_id, r.long_name) for r in results]


def fetch_languages() -> list[tuple[int, str]]:
    """Fetch language id–name pairs from BigQuery."""
    query = """
SELECT
    id,
    english_name
FROM
    `knowunity-data-prod.knowunity_backend_public.language`
"""
    query_job = client.query(query)
    results = list(query_job.result())

    return [(r.id, r.english_name) for r in results]


def fetch_grades() -> list[tuple[int, int, str]]:
    """Fetch grade id, country_id and long_name from BigQuery."""
    query = """
SELECT
    id,
    country_id,
    long_name
FROM
    `knowunity-data-prod.knowunity_backend_public.grade`
"""
    query_job = client.query(query)
    results = list(query_job.result())

    return [(r.id, r.country_id, r.long_name) for r in results]
