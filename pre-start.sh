# kowiki-20250922-cirrussearch-content.json.gz 가 있는지 확인하고 없으면 다운로드
if [ ! -f "kowiki-20250922-cirrussearch-content.json.gz" ]; then
    echo "kowiki-20250922-cirrussearch-content.json.gz 가 없습니다. 다운로드 중..."
    wget https://dumps.wikimedia.org/other/cirrussearch/20250922/kowiki-20250922-cirrussearch-content.json.gz
    echo "kowiki-20250922-cirrussearch-content.json.gz 다운로드 완료"
fi

docker-compose down
echo "docker-compose down"
docker-compose up -d
echo "elastic-search, kibana 실행 완료"