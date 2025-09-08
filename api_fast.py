from fastapi import FastAPI, File, UploadFile, Depends, Security, HTTPException
from fastapi.security import APIKeyHeader
from starlette.status import HTTP_401_UNAUTHORIZED
import tempfile, shutil

# Import your logic
# Replace this import with your actual code file if different
from org_1_2907 import SQLGenerator

# -------------------------------
# 🔑 API Key Security
# -------------------------------
API_KEY = "my-secret-key"   # 👉 replace with strong secret
API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

def get_api_key(api_key: str = Security(api_key_header)):
    if api_key == API_KEY:
        return api_key
    raise HTTPException(
        status_code=HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing API Key",
        headers={"WWW-Authenticate": "API Key"},
    )

# -------------------------------
# 🚀 FastAPI App
# -------------------------------
app = FastAPI(
    title="Data Quality API",
    description="Upload config files, run checks, return JSON results",
    version="1.0.0"
)

# Initialize DB + checker
sql_gen = SQLGenerator()
sql_gen.connect_database("test.db")   # change to your real DB
checker = sql_gen.data_quality_checker
results_manager = sql_gen.results_manager

@app.get("/")
def root():
    return {"message": "✅ Data Quality API is running. Visit /docs for Swagger UI."}

# -------------------------------
# 📂 Main Endpoint
# -------------------------------
@app.post("/run_quality_checks")
async def run_quality_checks(
    checks_config: UploadFile = File(..., description="CSV file with checks configuration"),
    system_codes_config: UploadFile = File(..., description="CSV file with system codes configuration"),
    api_key: str = Depends(get_api_key)
):
    """Upload two config files, run checks, export results, and return JSON."""

    # Save uploaded files temporarily
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp1:
        shutil.copyfileobj(checks_config.file, temp1)
        checks_path = temp1.name

    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp2:
        shutil.copyfileobj(system_codes_config.file, temp2)
        system_codes_path = temp2.name

    # Load configs
    checker.load_checks_config(checks_path)
    checker.load_system_codes_config(system_codes_path)

    # Step 6: Run checks
    results = checker.run_all_checks()

    # Step 7: Export results
    checker.export_passed_checks_to_results_db(results, results_manager)
    checker.export_failed_checks_to_results_db(results, results_manager)

    # Step 8: Fetch stored results metadata
    cursor = results_manager.results_connection.cursor()
    cursor.execute("""
        SELECT table_name, execution_date, version, row_count, column_count, description, created_timestamp
        FROM query_metadata ORDER BY created_timestamp DESC
    """)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    metadata = [dict(zip(columns, row)) for row in rows]

    return {
        "status": "success",
        "checks_run": len(results),
        "results_summary": results,
        "stored_metadata": metadata
    }
