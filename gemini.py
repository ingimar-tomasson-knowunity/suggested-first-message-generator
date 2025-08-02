import json
from google import genai
from google.genai import types
import os
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any
from tqdm import tqdm

load_dotenv()

gemini_client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

topics_instruction = """
Please generate a comma separated list of {num_topics} most likely topics a student in '{grade}' in country '{country}' would study in subject '{subject}'.
You may use the grounding tool to search the web to find information about what these topics would be (e.g. national curricula etc.)
Ensure the topics are specific and few words long (1-4).
Output only the comma separated list of topics, nothing else.
"""

topic_schema = types.Schema(
    type=types.Type.OBJECT,
    properties={
        "topics": types.Schema(type=types.Type.ARRAY, items=types.Schema(type=types.Type.STRING)),
    },
    required=["topics"],
)

grounding_tool = types.Tool(
    google_search=types.GoogleSearch()
)

def generate_topics(subject: str, grade: str, country: str, num_topics: int = 10) -> list[str]:
    prompt = topics_instruction.format(grade=grade, country=country, subject=subject, num_topics=str(num_topics))
    response = gemini_client.models.generate_content(
        model="gemini-2.5-flash",
        contents=[prompt],
        config=types.GenerateContentConfig(
            temperature=0.5,
            thinking_config=types.ThinkingConfig(
                include_thoughts=False,
                thinking_budget=0,
            ),
            response_schema=topic_schema,
            response_mime_type="application/json",
        ),
    )

    response_dict = json.loads(response.text)
    topics_list = response_dict["topics"]

    return topics_list

suggested_prompt_instruction = '''Generate {num_prompts} most likely prompts a student from {country} in grade {grade} would ask an LLM about the topic {topic}.
The prompts should each be 4-7 words long.
Each prompt must start with a relevant emoji.
The prompts should not contain the word please or full stops.
The prompts MUST be in {language}
Output only the comma separated list of prompts, nothing else.
'''

suggested_prompt_schema = types.Schema(
    type=types.Type.OBJECT,
    properties={
        "suggested_prompts": types.Schema(type=types.Type.ARRAY, items=types.Schema(type=types.Type.STRING)),
    },
    required=["suggested_prompts"],
)

def generate_suggested_prompts(topic: str, country: str, grade: str, language: str, num_prompts: int = 10) -> list[str]:
    prompt = suggested_prompt_instruction.format(topic=topic, country=country, grade=grade, language=language, num_prompts=str(num_prompts))
    response = gemini_client.models.generate_content(
        model="gemini-2.5-flash",
        contents=[prompt],
        config=types.GenerateContentConfig(
            response_schema=suggested_prompt_schema,
            response_mime_type="application/json",
            thinking_config=types.ThinkingConfig(
                include_thoughts=False,
                thinking_budget=0,
            )
        ),
    )
    response_dict = json.loads(response.text)
    return response_dict["suggested_prompts"]


def generate_topics_batch(
    inputs: List[Dict[str, Any]], 
    batch_size: int = 20, 
    num_topics: int = 15,
    callback = None
) -> List[List[str]]:
    """
    Generate topics for multiple subject/grade/country combinations in parallel batches.
    
    Args:
        inputs: List of dicts with keys 'subject', 'grade', 'country'
        batch_size: Number of concurrent requests to process (default: 20)
        num_topics: Number of topics to generate per combination (default: 15)
        callback: Optional callback function called with (index, result) as each result completes
    
    Returns:
        List of topic lists corresponding to each input
    """
    def process_single_input(input_data: Dict[str, Any]) -> List[str]:
        return generate_topics(
            input_data['subject'], 
            input_data['grade'], 
            input_data['country'], 
            num_topics
        )
    
    results = []
    with ThreadPoolExecutor(max_workers=batch_size) as executor:
        # Process all inputs in parallel batches
        future_to_input = {
            executor.submit(process_single_input, input_data): i 
            for i, input_data in enumerate(inputs)
        }
        
        # Collect results in original order with progress tracking
        results = [None] * len(inputs)
        for future in tqdm(as_completed(future_to_input), total=len(inputs), desc="Generating topics"):
            index = future_to_input[future]
            try:
                result = future.result()
                results[index] = result
                # Call callback immediately when result is ready
                if callback:
                    callback(index, result)
            except (ConnectionError, TimeoutError, ValueError, RuntimeError) as e:
                print(f"Error processing input {index}: {e}")
                results[index] = []
                if callback:
                    callback(index, [])
            except Exception as e:  # noqa: BLE001
                print(f"Unexpected error processing input {index}: {e}")
                results[index] = []
                if callback:
                    callback(index, [])
    
    return results


def generate_suggested_prompts_batch(
    inputs: List[Dict[str, Any]], 
    batch_size: int = 20, 
    num_prompts: int = 10,
    callback = None
) -> List[List[str]]:
    """
    Generate suggested prompts for multiple topic/country/grade/language combinations in parallel batches.
    
    Args:
        inputs: List of dicts with keys 'topic', 'country', 'grade', 'language'
        batch_size: Number of concurrent requests to process (default: 20)
        num_prompts: Number of prompts to generate per combination (default: 10)
        callback: Optional callback function called with (index, result) as each result completes
    
    Returns:
        List of prompt lists corresponding to each input
    """
    def process_single_input(input_data: Dict[str, Any]) -> List[str]:
        return generate_suggested_prompts(
            input_data['topic'],
            input_data['country'], 
            input_data['grade'],
            input_data['language'],
            num_prompts
        )
    
    results = []
    with ThreadPoolExecutor(max_workers=batch_size) as executor:
        # Process all inputs in parallel batches
        future_to_input = {
            executor.submit(process_single_input, input_data): i 
            for i, input_data in enumerate(inputs)
        }
        
        # Collect results in original order with progress tracking
        results = [None] * len(inputs)
        for future in tqdm(as_completed(future_to_input), total=len(inputs), desc="Generating prompts"):
            index = future_to_input[future]
            try:
                result = future.result()
                results[index] = result
                # Call callback immediately when result is ready
                if callback:
                    callback(index, result)
            except (ConnectionError, TimeoutError, ValueError, RuntimeError) as e:
                print(f"Error processing input {index}: {e}")
                results[index] = []
                if callback:
                    callback(index, [])
            except Exception as e:  # noqa: BLE001
                print(f"Unexpected error processing input {index}: {e}")
                results[index] = []
                if callback:
                    callback(index, [])
    
    return results