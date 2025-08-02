
import click
import csv
from threading import Lock
from bigquery import fetch_countries, fetch_subjects, fetch_grades, fetch_languages
from utils import (
    save_csv_data,
    append_csv_data,
    create_combinations,
    get_names_for_combination,
    load_combinations_for_country,
    get_languages_for_country,
    get_country_id_by_name,
    get_language_id_by_name,
)
from gemini import generate_topics_batch, generate_suggested_prompts_batch
from db import upload_suggested_messages_to_db_from_dicts
from uuid import uuid4, UUID
from datetime import datetime, timezone
import logging



@click.group()
def cli():
    """A CLI to manage the first prompt macher workflow."""


@cli.command()
def download_data():
    """Downloads data from BigQuery and saves it to CSV files."""
    click.echo("Downloading data from BigQuery...")

    countries = fetch_countries()
    save_csv_data(
        "./data/country_table.csv",
        [{"id": r[0], "english_name": r[1]} for r in countries],
        ["id", "english_name"],
    )
    click.echo("Downloaded country data.")

    subjects = fetch_subjects()
    save_csv_data(
        "./data/subject_table.csv",
        [{"id": r[0], "country_id": r[1], "long_name": r[2]} for r in subjects],
        ["id", "country_id", "long_name"],
    )
    click.echo("Downloaded subject data.")

    grades = fetch_grades()
    save_csv_data(
        "./data/grade_table.csv",
        [{"id": r[0], "country_id": r[1], "long_name": r[2]} for r in grades],
        ["id", "country_id", "long_name"],
    )
    click.echo("Downloaded grade data.")

    languages = fetch_languages()
    save_csv_data(
        "./data/language_table.csv",
        [{"id": r[0], "english_name": r[1]} for r in languages],
        ["id", "english_name"],
    )
    click.echo("Downloaded language data.")

    click.echo("All data downloaded successfully.")


@cli.command()
def make_combinations():
    """Creates combination files for each country."""
    click.echo("Creating combination files...")
    create_combinations()
    click.echo("Combination files created successfully.")


@cli.command()
@click.option("--country", required=True, help="The country to generate topics for.")
@click.option("--batch-size", default=20, help="Number of parallel requests to process.")
def generate_all_topics(country: str, batch_size: int):
    """Generates topics for a given country using parallel processing."""
    click.echo(f"Generating topics for {country} with batch size {batch_size}...")
    country_id = get_country_id_by_name(country)
    combinations = load_combinations_for_country(country_id)

    # Prepare batch inputs
    batch_inputs = []
    combination_metadata = []
    
    for combo in combinations:
        country_name, grade_name, subject_name = get_names_for_combination(
            combo.country_id, combo.grade_id, combo.subject_id
        )
        batch_inputs.append({
            'subject': subject_name,
            'grade': grade_name, 
            'country': country_name
        })
        combination_metadata.append({
            'country_id': combo.country_id,
            'grade_id': combo.grade_id,
            'subject_id': combo.subject_id
        })

    click.echo(f"Processing {len(batch_inputs)} combinations in parallel...")
    
    # Initialize CSV file with header
    csv_filename = f"./data/topics_{country_id}.csv"
    fieldnames = ["country_id", "grade_id", "subject_id", "topic"]
    with open(csv_filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

    # Create a lock to manage file access
    file_lock = Lock()

    # Define callback function to save results as they complete
    def save_topics_callback(index: int, topics: list[str]):
        """Callback function to save topics to CSV as they are generated."""
        if not topics:  # Skip empty results
            return
            
        combo_meta = combination_metadata[index]
        batch_topics = []
        for topic in topics:
            batch_topics.append(
                {
                    "country_id": combo_meta["country_id"],
                    "grade_id": combo_meta["grade_id"], 
                    "subject_id": combo_meta["subject_id"],
                    "topic": topic,
                }
            )
        
        # Use a lock to prevent race conditions when writing to the file
        with file_lock:
            append_csv_data(csv_filename, batch_topics, fieldnames)
    
    # Generate topics in batch with iterative saving
    generate_topics_batch(
        batch_inputs, 
        batch_size=batch_size, 
        num_topics=10, 
        callback=save_topics_callback
    )

    click.echo(f"Topics for {country} generated and saved.")


@cli.command()
@click.option("--country", required=True, help="The country to generate prompts for.")
@click.option("--batch-size", default=20, help="Number of parallel requests to process.")
def generate_all_prompts(country: str, batch_size: int):
    """Generates prompts for a given country using parallel processing and saves them to CSV."""
    click.echo(f"Generating prompts for {country} with batch size {batch_size}...")
    country_id = get_country_id_by_name(country)
    languages = get_languages_for_country(country_id)

    with open(f"./data/topics_{country_id}.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        topics_data = list(reader)

    # Prepare batch inputs
    batch_inputs = []
    input_metadata = []
    
    for row in topics_data:
        country_name, grade_name, _ = get_names_for_combination(
            int(row["country_id"]), int(row["grade_id"]), int(row["subject_id"])
        )
        for lang in languages:
            batch_inputs.append({
                'topic': row["topic"],
                'country': country_name,
                'grade': grade_name,
                'language': lang
            })
            input_metadata.append({
                'country_id': int(row["country_id"]),
                'grade_id': int(row["grade_id"]),
                'subject_id': int(row["subject_id"]),
                'language': lang,
                'language_id': get_language_id_by_name(lang)
            })

    click.echo(f"Processing {len(batch_inputs)} topic-language combinations in parallel...")
    
    # Initialize CSV file with header
    csv_filename = f"./data/messages_{country_id}.csv"
    fieldnames = ["uuid", "created_on", "message", "language_id", "country_id", "grade_id", "subject_id"]
    with open(csv_filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

    # Create a lock to manage file access
    file_lock = Lock()

    # Define callback function to save prompts as they complete
    def save_prompts_callback(index: int, prompts: list[str]):
        """Callback function to save prompts to CSV as they are generated."""
        if not prompts:  # Skip empty results
            return
            
        metadata = input_metadata[index]
        batch_prompts = []
        for prompt in prompts:
            batch_prompts.append({
                "uuid": str(uuid4()),
                "created_on": datetime.now(timezone.utc).isoformat(),
                "message": prompt,
                "language_id": metadata['language_id'],
                "country_id": metadata['country_id'],
                "grade_id": metadata['grade_id'],
                "subject_id": metadata['subject_id'],
            })
        
        # Use a lock to prevent race conditions when writing to the file
        with file_lock:
            append_csv_data(csv_filename, batch_prompts, fieldnames)
    
    # Generate prompts in batch with iterative saving
    generate_suggested_prompts_batch(
        batch_inputs, 
        batch_size=batch_size, 
        num_prompts=10,
        callback=save_prompts_callback
    )

    click.echo(f"Prompts for {country} generated and saved to {csv_filename}.")


logger = logging.getLogger(__name__)


@cli.command()
@click.option("--file-path", required=True, help="Path to the CSV file to upload to the database.")
@click.option("--batch-size", default=100, help="Number of records to process in each batch.")
def upload_to_db(file_path: str, batch_size: int):
    """Upload suggested first messages from CSV file to the database."""
    click.echo(f"Uploading data from {file_path} to database...")
    
    try:
        records = []
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row_num, row in enumerate(reader, 1): # Added row_num for better error reporting
                record = {}
                try:
                    record["uuid"] = UUID(row["uuid"])
                    
                    # Handle datetime conversion
                    created_on_str = row["created_on"]
                    if created_on_str.endswith('Z'):
                        created_on_str = created_on_str.replace('Z', '+00:00')
                    record["created_on"] = datetime.fromisoformat(created_on_str)
                    
                    record["message"] = row["message"]

                    # Helper function to safely convert to int
                    def safe_int_conversion(value_str):
                        if value_str is None or not value_str.strip():
                            return None
                        try:
                            return int(value_str.strip())
                        except ValueError:
                            logger.warning(f"Row {row_num}: Could not convert '{value_str}' to int for an ID field. Setting to None.")
                            return None

                    record["language_id"] = safe_int_conversion(row.get("language_id"))
                    record["country_id"] = safe_int_conversion(row.get("country_id"))
                    record["grade_id"] = safe_int_conversion(row.get("grade_id"))
                    record["subject_id"] = safe_int_conversion(row.get("subject_id"))
                    
                    records.append(record)

                except KeyError as e:
                    logger.error(f"Row {row_num}: Missing expected column '{e}'. Skipping record.")
                except ValueError as e:
                    logger.error(f"Row {row_num}: Data conversion error for record: {e}. Row data: {row}. Skipping record.")
                except Exception as e:
                    logger.error(f"Row {row_num}: Unexpected error processing record: {e}. Row data: {row}. Skipping record.")
        
        click.echo(f"Read {len(records)} valid records from CSV file.")
        
        # Upload to database
        total_imported, errors = upload_suggested_messages_to_db_from_dicts(records, batch_size)
        
        click.echo(f"Successfully imported {total_imported} records to database.")
        
        if errors:
            click.echo(f"Encountered {len(errors)} errors during database upload:")
            for error in errors:
                click.echo(f"  - {error}")
        
    except FileNotFoundError:
        click.echo(f"Error: File {file_path} not found.", err=True)
    except Exception as e:
        click.echo(f"An unexpected error occurred during CSV processing or initial setup: {str(e)}", err=True)



if __name__ == "__main__":
    cli()
