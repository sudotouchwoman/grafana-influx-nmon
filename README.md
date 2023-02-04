# **Monitoring pipeline example**

This project uses InfluxDB and Grafana to monitor performance metrics
collected during DB benchmarking.

To get the project running, the following commands might be helpful:

```bash
# virtual environment setup
python -m venv --prompt monitor .venv
source .venv/bin/activate
# download dependencies
pip install -r requirements.txt
```

```bash
# start grafana and influx in containers
cd docker
docker-compose -f docker-compose.yaml up
# to stop (from the same location)
docker-compose -f docker-compose.yaml down
```

```bash
# run python parser for nmon
python main.py
# run unit-tests
python -m pytest testing
# or just
pytest
```
