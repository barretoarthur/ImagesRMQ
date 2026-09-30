FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# Comando para arrancar o script quando o contentor iniciar
CMD ["python", "client.py"]