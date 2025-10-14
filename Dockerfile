# ── Dockerfile ────────────────────────────────────────────────────────────────
FROM public.ecr.aws/docker/library/python:3.12-slim

WORKDIR /app

# real-time logs to CloudWatch
ENV PYTHONUNBUFFERED=1

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy code and sample ATP image
COPY truck_activity_simulator.py ./
COPY image.jpg ./image.jpg

CMD ["python", "-u", "truck_activity_simulator.py"]

