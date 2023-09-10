from cx_Freeze import setup, Executable

# List of Python script(s) to convert to executables
target_files = ["metadata_generator.py"]

# Configure the setup
setup(
    name="Airflow DAG Metadata Generator - Versent/karenina.comia",
    version="1.0",
    description="Use this to generate DAG Metadata",
    executables=[Executable(script=file) for file in target_files]
)
