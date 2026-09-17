FROM python:3.11-slim
WORKDIR /app
COPY hub ./hub
COPY scripts ./scripts
ENV HUB_HOST=0.0.0.0 HUB_PORT=8080 HUB_DB=/data/hub.db
VOLUME /data
EXPOSE 8080 9100
# hub serves the control plane; the demo partner receiver runs alongside
CMD ["python3", "-m", "hub", "--demo-receiver-port", "9100"]
