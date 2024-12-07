@@ -6,11 +6,9 @@

router = APIRouter()

# Configure Gemini API
GEMINI_API_KEY = "AIzaSyDYTAnPjkk3o-1s9kL1P4Dv1qkgSs25Fsg"
genai.configure(api_key=GEMINI_API_KEY)

# Define Pydantic models
class ProcessingRequest(BaseModel):
    raw_text: str

@@ -20,7 +18,6 @@ class ProcessingResponse(BaseModel):
    summary: str
    keywords: list[str]

# POST /process - Process raw text using the Gemini API
@router.post("/process", response_model=ProcessingResponse)
def process_text(request: ProcessingRequest):
    prompt = (
@@ -31,18 +28,14 @@ def process_text(request: ProcessingRequest):
    )

    try:
        # Use the `google.generativeai` package to generate content
        model = genai.GenerativeModel("gemini-1.5-flash")
        response = model.generate_content(prompt)
        print(response.text)  # This will output the generated JSON as a string
        print(response.text)  

        # Remove the Markdown formatting from the response text
        cleaned_response = re.sub(r"^```json|```$", "", response.text).strip()

        # Parse the cleaned response as JSON
        structured_response = json.loads(cleaned_response)

        # Validate the parsed response with Pydantic
        processed_data = ProcessingResponse(**structured_response)
        return processed_data
