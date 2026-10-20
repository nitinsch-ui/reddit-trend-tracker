FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    HF_HOME=/opt/hf TOKENIZERS_PARALLELISM=false OMP_NUM_THREADS=2
WORKDIR /app

# CPU-only torch keeps the image ~1.5 GB smaller than the default CUDA build.
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu torch==2.3.1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY download_model.py .
RUN python download_model.py && chmod -R a+rX /opt/hf
ENV HF_HUB_OFFLINE=1

COPY app.py extractor.py normalize.py ./
RUN useradd --create-home --uid 10001 app
USER app

EXPOSE 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
