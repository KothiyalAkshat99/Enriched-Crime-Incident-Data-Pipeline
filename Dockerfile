FROM python:3.12-slim

WORKDIR /app

# Copy requirements.txt and install dependencies (in the working directory)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code as a package
COPY src/ src/

# Default command to run the application
CMD ["python", "-m", "src.pipeline.main"]