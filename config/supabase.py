

from supabase import create_client, Client

# Supabase configuration
SUPABASE_URL = "https://prkbiqmifzhsgazeibyf.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBya2JpcW1pZnpoc2dhemVpYnlmIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzI4MDc1MTUsImV4cCI6MjA0ODM4MzUxNX0.Cz0VRiWMCzySn1wVH1qjTEKv9C1yAr19P5HkTNE_7Ao"

# Create Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
