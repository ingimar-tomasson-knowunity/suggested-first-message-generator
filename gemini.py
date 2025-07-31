import json
from google import genai
from google.genai import types
import os
from dotenv import load_dotenv

load_dotenv()

gemini_client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

topics_instruction = """
Please generate a comma separated list of 15 most likely topics a student in grade '{grade}' in country '{country}' would study in subject '{subject}'.
You may search the web to find information about what these topics would be.
Ensure the topics are a few words long (1-4).
Output only the comma separated list of topics, nothing else.
"""

grounding_tool = types.Tool(
    google_search=types.GoogleSearch()
)

def generate_topics(subject: str, grade: str, country: str) -> list[str]:
    prompt = topics_instruction.format(grade=grade, country=country, subject=subject)
    response = gemini_client.models.generate_content(
        model="gemini-2.5-pro",
        contents=[prompt],
        config=types.GenerateContentConfig(
            tools=[grounding_tool]
        ),
    )

    topics_list = [topic.strip() for topic in response.text.split(',')]
    return topics_list

suggested_prompt_instruction = '''Generate 10 most likely prompts a student from {country} in grade {grade} would ask an LLM about the topic {topic}.
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

def generate_suggested_prompts(topic: str, country: str, grade: str, language: str) -> list[str]:
    prompt = suggested_prompt_instruction.format(topic=topic, country=country, grade=grade, language=language)
    response = gemini_client.models.generate_content(
        model="gemini-2.5-pro",
        contents=[prompt],
        config=types.GenerateContentConfig(
            response_schema=suggested_prompt_schema,
            response_mime_type="application/json"
        ),
    )
    response_dict = json.loads(response.text)
    return response_dict["suggested_prompts"]