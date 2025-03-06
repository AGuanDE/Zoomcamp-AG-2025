
set -e

TAXI_TYPE=$1 #"yellow"
YEAR=$2 #2020


# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
# https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2021-01.parquet
# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-01.parquet
# https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2020-01.parquet

URL_BASE="https://d37ci6vzurychx.cloudfront.net/trip-data"


for MONTH in {1..12}; do
  FMONTH=`printf "%02d" $MONTH`

  URL="${URL_BASE}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet"
  
  LOCAL_PREFIX="/mnt/c/Users/aguan/project_folder/zoomcamp-ag-de/Module_5_Batch/data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
  LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.parquet"
  LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

  echo "downloading ${URL} to ${LOCAL_PATH}"
  mkdir -p ${LOCAL_PREFIX}
  wget ${URL} -O ${LOCAL_PATH}

  
  echo wget ${URL}
done