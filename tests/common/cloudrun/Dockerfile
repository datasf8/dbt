FROM python:3.8-slim

# Install SDK.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
# Verify that the image does not have conflicting dependencies.
RUN pip check

# Copy files from official SDK image, including script/dependencies.
COPY . .

# Set the entrypoint to Apache Beam SDK launcher.
# ENTRYPOINT ["/opt/apache/beam/boot"]
CMD ["python", "./process.py"]