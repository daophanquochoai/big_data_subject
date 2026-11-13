FROM pytorch/pytorch:2.6.0-cuda12.4-cudnn9-runtime
WORKDIR /app
COPY train.py /app/train.py
RUN mkdir -p /data
ENV PYTHONUNBUFFERED=1 OMP_NUM_THREADS=1
ENTRYPOINT ["torchrun"]
