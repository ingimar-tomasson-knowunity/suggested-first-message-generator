import logging
from typing import List, Tuple
from tqdm import tqdm
from dotenv import load_dotenv
import remote_resource_access
from model import SuggestedFirstMessage

load_dotenv()

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

    total_imported = 0
    errors = []
    
    if not records:
        return total_imported, errors

    # Process records in batches
    for i in tqdm(range(0, len(records), batch_size), desc="Uploading batches"):
        batch = records[i:i + batch_size]
        
        # Convert SuggestedFirstMessage objects to tuples for VALUES clause
        batch_data = []
        for record in batch:
            batch_data.append((
                record.uuid,
                record.created_on,
                record.message,
                record.language_id,
                record.country_id,
                record.grade_id,
                record.subject_id
            ))
        
        try:
            try:
                db = remote_resource_access.get_db()
            except Exception as e:
                logger.error(f"Error getting database connection: {e}")
                errors.append(f"Error getting database connection: {e}")
                continue
            
            with db.cursor() as cur:
                # Use executemany for efficient bulk insert
                try:
                    cur.executemany(
                        "INSERT INTO ai_chat_suggested_first_message (uuid, created_on, message, language_id, country_id, grade_id, subject_id) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (uuid) DO NOTHING",
                        batch_data
                    )
                except Exception as e:
                    logger.error(f"Error inserting {batch_data[0][0]}: {e}")
                    errors.append(f"Error inserting {batch_data[0][0]}: {e}")
                    continue
                
                total_imported += len(batch)
                
        except Exception as e:
            error_msg = f"Batch {i//batch_size + 1} failed: {str(e)}"
            logger.error(error_msg)
            errors.append(error_msg)

    return total_imported, errors


def upload_suggested_messages_to_db_from_dicts(
    records: List[dict], batch_size: int = 100
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


