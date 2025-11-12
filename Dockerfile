FROM mcr.microsoft.com/playwright/python:latest

# Working directory for Lambda
WORKDIR /var/task

# Copy requirements and install (including AWS Lambda RIC)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt awslambdaric

# Copy source
COPY . .

# Ensure Python output is not buffered
ENV PYTHONUNBUFFERED=1

# Start the AWS Lambda Runtime Interface Client and point to the handler
ENTRYPOINT ["/usr/bin/python3", "-m", "awslambdaric"]
CMD ["scrap_table.lambda_handler"]
