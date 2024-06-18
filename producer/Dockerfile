FROM python:3.9-slim

# Copy ứng dụng của bạn vào container
COPY . /app

WORKDIR /app

# Cài đặt các thư viện Python cần thiết
RUN pip install -r requirements.txt

CMD ["python", "producer.py"]
