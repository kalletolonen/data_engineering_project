FROM python:3.11-bullseye

# Install Java (OpenJDK 17) and other dependencies
RUN apt-get update && \
    mkdir -p /usr/share/man/man1 && \
    apt-get install -y openjdk-17-jre-headless && \
    apt-get clean;

# Determine JAVA_HOME (usually /usr/lib/jvm/java-17-openjdk-arm64 on arm64 debian)
# We can set it dynamically or just rely on the path being standard
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

# Install PySpark
RUN pip install --no-cache-dir pyspark==3.5.0

# Create working directory
WORKDIR /app

# Copy files
COPY . /app

# Run the script
CMD ["python", "pyspark_sqlite_demo.py"]
