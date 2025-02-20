# Use an official Python runtime as a parent image
FROM gboeing/osmnx

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY dist/ .

# Install the Python dependencies
RUN uv pip install *.whl --system

# Expose the port that the application will run on
EXPOSE 8000

# Command to run the application
CMD cicloapi
