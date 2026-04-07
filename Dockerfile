FROM python:3.9-slim-bookworm

# Set the working directory in the container
WORKDIR /app

# Copy the dependencies file to the working directory
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application's code to the working directory
COPY . .

# Expose the port the app runs on
EXPOSE 8000

# Define the command to run the application
# Increase timeout for long-running operations like database imports (30 minutes)
# timeout-keep-alive: How long to wait for requests to complete
# timeout-graceful-shutdown: How long to wait for graceful shutdown
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--timeout-keep-alive", "1800", "--timeout-graceful-shutdown", "30"]
