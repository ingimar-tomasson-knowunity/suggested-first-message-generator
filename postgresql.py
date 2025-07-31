import os
import logging
from typing import List, Tuple
import psycopg2
from psycopg2.extras import execute_values
from tqdm import tqdm

from model import SuggestedFirstMessage

logger = logging.getLogger(__name__)


def upload_suggested_messages_to_db(
    records: List[SuggestedFirstMessage], batch_size: int = 1000
) -> Tuple[int, List[str]]:
    """Upload suggested first messages to database using batch processing.

    Args:
        records: List of SuggestedFirstMessage objects to import.
        batch_size: Number of records to process in each batch. Defaults to 1000.

    Returns:
        tuple[int, list[str]]: A tuple containing:
            - Number of successfully imported records
            - List of error messages as strings
    """
    db_params = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
    }

    # Using a CTE for bulk insert
    insert_query = """
        INSERT INTO ai_chat_suggested_first_message (
            uuid, created_on, message, language_id, country_id, grade_id, subject_id
        )
        SELECT v.uuid, v.created_on, v.message, v.language_id, v.country_id, v.grade_id, v.subject_id
        FROM (VALUES %s) AS v(uuid, created_on, message, language_id, country_id, grade_id, subject_id)
        ON CONFLICT (uuid) DO NOTHING
        RETURNING uuid
    """

    conn = None
    cursor = None
    total_imported = 0
    errors = []

    try:
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        total_batches = (len(records) + batch_size - 1) // batch_size
        with tqdm(total=total_batches, desc="Importing suggested messages to Database") as pbar:
            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]
                try:
                    values = [
                        (
                            record.uuid,
                            record.created_on,
                            record.message,
                            record.language_id,
                            record.country_id,
                            record.grade_id,
                            record.subject_id,
                        )
                        for record in batch
                    ]

                    # Execute bulk insert using psycopg2.extras.execute_values
                    inserted_rows = execute_values(
                        cursor, insert_query, values, template=None, fetch=True
                    )

                    # Check which UUIDs were actually inserted
                    inserted_uuids = {row[0] for row in inserted_rows}
                    batch_uuids = {record.uuid for record in batch}
                    skipped_uuids = batch_uuids - inserted_uuids

                    if skipped_uuids:
                        for uuid in skipped_uuids:
                            logger.warning(f"Skipping duplicate UUID: {uuid}")

                    total_imported += len(inserted_rows)
                    conn.commit()
                    logger.debug(
                        f"Batch processed - batch_size: {len(batch)}, imported_count: {len(inserted_rows)}"
                    )
                    pbar.update(1)

                except psycopg2.Error as e:
                    errors.append(str(e))
                    conn.rollback()
                    logger.error(f"Error processing batch starting at index {i}: {str(e)}")

    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            logger.info("Database connection closed")

    return total_imported, errors


def upload_suggested_messages_to_db_from_dicts(
    records: List[dict], batch_size: int = 1000
) -> Tuple[int, List[str]]:
    """Upload suggested first messages to database from dictionary records.
    
    This is a convenience function that converts dictionaries to SuggestedFirstMessage
    objects and then uploads them.

    Args:
        records: List of record dictionaries to import.
        batch_size: Number of records to process in each batch. Defaults to 1000.

    Returns:
        tuple[int, list[str]]: A tuple containing:
            - Number of successfully imported records
            - List of error messages as strings
    """
    # Convert dictionaries to SuggestedFirstMessage objects
    message_objects = [SuggestedFirstMessage(**record) for record in records]
    return upload_suggested_messages_to_db(message_objects, batch_size)


def test_db_connection() -> bool:
    """Test database connection with current environment variables.
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    db_params = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": os.getenv("DB_PORT"),
    }
    
    try:
        conn = psycopg2.connect(**db_params)
        conn.close()
        logger.info("Database connection test successful")
        return True
    except Exception as e:
        logger.error(f"Database connection test failed: {str(e)}")
        return False
