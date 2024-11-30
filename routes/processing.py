from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ValidationError
import google.generativeai as genai
import json
import re

router = APIRouter()

# Configure Gemini API
GEMINI_API_KEY = "AIzaSyDYTAnPjkk3o-1s9kL1P4Dv1qkgSs25Fsg"
genai.configure(api_key=GEMINI_API_KEY)

# Define Pydantic models
class ProcessingRequest(BaseModel):
    raw_text: str

class ProcessingResponse(BaseModel):
    id: str
    title: str
    summary: str
    keywords: list[str]

# POST /process - Process raw text using the Gemini API
@router.post("/process", response_model=ProcessingResponse)
def process_text(request: ProcessingRequest):
    prompt = (
        "Process the following raw text into structured JSON with the fields: "
        "id (unique identifier) consider id as string always, title (summary title), summary (a brief overview), "
        "and keywords (a list of relevant keywords). "
        "Text: " + request.raw_text
    )

    try:
        # Use the `google.generativeai` package to generate content
        model = genai.GenerativeModel("gemini-1.5-flash")
        response = model.generate_content(prompt)
        print(response.text)  # This will output the generated JSON as a string

        # Remove the Markdown formatting from the response text
        cleaned_response = re.sub(r"^```json|```$", "", response.text).strip()

        # Parse the cleaned response as JSON
        structured_response = json.loads(cleaned_response)

        # Validate the parsed response with Pydantic
        processed_data = ProcessingResponse(**structured_response)
        return processed_data

    except json.JSONDecodeError as e:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid JSON response: {str(e)}. Response content: {cleaned_response}",
        )
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=f"Invalid response format: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Processing failed: {str(e)}")
