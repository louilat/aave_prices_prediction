FROM ubuntu:22.04
WORKDIR /aave_prices_prediction
# Install Python
RUN apt-get -y update && \
    apt-get install -y python3-pip
# Install project dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY api ./api
RUN chmod +x ./api/run.sh
EXPOSE 8000
CMD ["fastapi", "run", "./api/aave_data_api.py"]