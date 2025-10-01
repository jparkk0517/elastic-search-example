# 한국 위키피디아 검색 시스템

Elasticsearch와 Nori 형태소 분석기를 활용한 한국어 위키피디아 검색 시스템입니다.

## 📊 사용 데이터

- **데이터셋**: 한국 위키피디아 CirrusSearch 덤프
- **파일**: `kowiki-20250922-cirrussearch-content.json.gz` (약 2.71GB)
- **출처**: [Wikimedia Dumps](https://dumps.wikimedia.org/other/cirrussearch/)
- **색인 문서 수**: 10,000개
- **자동 다운로드**: `pre-start.sh` 스크립트 실행 시 자동으로 다운로드됨

## 🚀 시작하기

### 1. 필수 요구사항

- Docker & Docker Compose
- Python 3.8+
- Poetry (Python 패키지 관리자)

### 2. Elasticsearch & Kibana 실행

```bash
# Elasticsearch와 Kibana 시작
bash pre-start.sh
```

이 스크립트는 다음을 수행합니다:

- 위키피디아 덤프 파일이 없으면 자동 다운로드
- 기존 Docker 컨테이너 중지
- Elasticsearch(포트 9200)와 Kibana(포트 5601) 실행

### 3. Python 패키지 설치

```bash
# Poetry로 의존성 설치
poetry install
```

### 4. 데이터 색인

```bash
# 기본 실행 (대화형)
poetry run python main.py
```

프롬프트에서:

- 기존 데이터 사용 여부 선택 (y/n)
- 색인할 문서 수 입력 (예: 10000, 또는 'all')

## 🔍 검색하기

### 방법 1: main.py 사용 (대화형)

```bash
poetry run python main.py
```

기존 인덱스가 있으면 바로 검색 모드로 진입합니다.

### 방법 2: 직접 검색 (추천)

Elasticsearch API를 직접 사용하거나 Kibana Dev Tools를 이용할 수 있습니다.

**Kibana Dev Tools** (http://localhost:5601/app/dev_tools#/console):

```json
GET /kowiki_cirrus/_search
{
  "query": {
    "multi_match": {
      "query": "수학",
      "fields": ["title^3", "opening_text^2", "text"]
    }
  },
  "size": 5
}
```

**Python으로 검색**:

```python
from elasticsearch import Elasticsearch

es = Elasticsearch(["http://localhost:9200"], basic_auth=("elastic", "hosun"))

result = es.search(
    index="kowiki_cirrus",
    body={
        "query": {
            "multi_match": {
                "query": "지미 카터",
                "fields": ["title^3", "opening_text^2", "text"]
            }
        },
        "size": 5
    }
)

for hit in result["hits"]["hits"]:
    print(f"{hit['_source']['title']}: {hit['_score']}")
```

## 📁 주요 파일 구조

```
.
├── pre-start.sh                    # Elasticsearch/Kibana 실행 스크립트
├── docker-compose.yml              # Docker 설정
├── pyproject.toml                  # Python 의존성 설정
├── index_setting.json              # Elasticsearch 인덱스 설정
├── main.py                         # 메인 실행 파일
├── wiki_loader.py                  # 위키피디아 데이터 로더
└── kowiki-20250922-cirrussearch-content.json.gz  # 위키피디아 덤프 (자동 다운로드)
```

## 🛠️ 주요 설정

### Elasticsearch 인덱스 설정

- **인덱스명**: `kowiki_cirrus`
- **Analyzer**: Nori Tokenizer 기반 `korean_analyzer`
- **유사도 알고리즘**: BM25 (k1=1.2, b=0.75)
- **검색 필드**:
  - `title` (가중치 3배)
  - `opening_text` (가중치 2배)
  - `text` (기본 가중치)

### Docker 컨테이너

- **Elasticsearch**: `localhost:9200`
  - Username: `elastic`
  - Password: `hosun`
- **Kibana**: `localhost:5601`

## 🔧 문제 해결

### Elasticsearch 연결 실패

```bash
# Docker 컨테이너 상태 확인
docker ps

# 재시작
bash pre-start.sh
```

### 색인 실패

기존 인덱스를 삭제하고 다시 생성:

```bash
# Elasticsearch에서 인덱스 삭제
curl -X DELETE "localhost:9200/kowiki_cirrus" -u elastic:hosun

# 다시 색인
poetry run python main.py
```

### Poetry 설치

```bash
# macOS/Linux
curl -sSL https://install.python-poetry.org | python3 -

# 환경변수 추가 (필요시)
export PATH="$HOME/.local/bin:$PATH"
```

## 📝 예시 검색어

- `수학` - 1,015개 문서 검색
- `지미 카터` - 82개 문서 검색
- `대한민국` - 수천 개 문서 검색
- `컴퓨터 과학` - 관련 문서 검색

## 📚 참고 자료

- [Elasticsearch 공식 문서](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html)
- [Nori 형태소 분석기](https://www.elastic.co/guide/en/elasticsearch/plugins/current/analysis-nori.html)
- [Elastic 가이드북 (한글)](https://esbook.kimjmin.net/)
