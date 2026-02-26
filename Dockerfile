FROM python:3.12-slim

WORKDIR /app

# Copy requirements.txt and install dependencies (in the working directory)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code (in the src directory)
COPY src/ .

# Default command to run the application
CMD ["python", "src/main.py"]