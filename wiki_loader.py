# -*- coding: utf-8 -*-
"""
한국 위키피디아 CirrusSearch 덤프 데이터 로더
- Wikipedia CirrusSearch JSON 덤프 다운로드
- Elasticsearch 색인
"""

import os
import gzip
import json
from typing import Generator, Dict
from elasticsearch import Elasticsearch, helpers


class WikipediaCirrusLoader:
    """한국 위키피디아 CirrusSearch 덤프 로더"""

    def __init__(
        self,
        es_url: str = "http://localhost:9200",
        index_name: str = "kowiki_cirrus",
        username: str = "elastic",
        password: str = "hosun",
    ):
        """
        초기화

        Args:
            es_url: Elasticsearch URL
            index_name: 인덱스 이름
            username: Elasticsearch 사용자명
            password: Elasticsearch 비밀번호
        """
        self.es_url = es_url
        self.index_name = index_name
        self.client = Elasticsearch([es_url], basic_auth=(username, password))

    def check_connection(self):
        """Elasticsearch 연결 확인"""
        if self.client.ping():
            print("✅ Elasticsearch 연결 성공!")
            info = self.client.info()
            print(f"   버전: {info['version']['number']}")
            print(f"   클러스터: {info['cluster_name']}")
            return True
        else:
            print("❌ Elasticsearch 연결 실패!")
            return False

    def create_index(self):
        """위키피디아용 인덱스 생성"""
        # 기존 인덱스가 있으면 삭제
        if self.client.indices.exists(index=self.index_name):
            self.client.indices.delete(index=self.index_name)
            print(f"📝 기존 인덱스 '{self.index_name}' 삭제됨")

        # 인덱스 설정
        index_settings = {
            "settings": {
                "analysis": {
                    "tokenizer": {
                        "nori_user_dict": {
                            "type": "nori_tokenizer",
                            "decompound_mode": "mixed",
                            "discard_punctuation": "true",
                        }
                    },
                    "analyzer": {
                        "korean_analyzer": {
                            "type": "custom",
                            "tokenizer": "nori_user_dict",
                            "filter": [
                                "lowercase",
                                "nori_part_of_speech",
                                "nori_readingform",
                            ],
                        }
                    },
                },
                "similarity": {"default": {"type": "BM25", "k1": 1.2, "b": 0.75}},
            },
            "mappings": {
                "properties": {
                    "title": {
                        "type": "text",
                        "analyzer": "korean_analyzer",
                        "fields": {"keyword": {"type": "keyword"}},
                    },
                    "text": {"type": "text", "analyzer": "korean_analyzer"},
                    "category": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "template": {"type": "keyword"},
                    "namespace": {"type": "integer"},
                    "redirect": {"type": "keyword"},
                    "incoming_links": {"type": "integer"},
                    "opening_text": {"type": "text", "analyzer": "korean_analyzer"},
                }
            },
        }

        # 인덱스 생성
        self.client.indices.create(index=self.index_name, body=index_settings)
        print(f"✅ 인덱스 '{self.index_name}' 생성 완료!")
        print("   - Nori Tokenizer 기반 korean_analyzer 적용")
        print("   - BM25 유사도 알고리즘 설정")

    def parse_local_cirrus_file(
        self, file_path: str, max_docs: int = None
    ) -> Generator[Dict, None, None]:
        """
        로컬에 저장된 CirrusSearch 덤프 파일을 파싱

        Args:
            file_path: 로컬 .json.gz 파일 경로
            max_docs: 최대 처리할 문서 수 (None이면 전체)

        Yields:
            문서 딕셔너리
        """

        print(f"\n📂 로컬 파일 읽기 시작: {file_path}")

        # 파일 존재 확인
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"파일을 찾을 수 없습니다: {file_path}")

        # 파일 크기 확인
        file_size = os.path.getsize(file_path)
        print(f"📦 파일 크기: {file_size / (1024**3):.2f} GB")

        doc_count = 0
        processed_lines = 0

        # gzip으로 압축된 파일을 한 줄씩 읽기
        with gzip.open(file_path, "rt", encoding="utf-8") as gz_file:
            for line in gz_file:
                processed_lines += 1

                # CirrusSearch 형식: 두 줄씩 쌍을 이룸
                # 첫 번째 줄: index action (건너뛰기)
                # 두 번째 줄: 실제 문서 데이터
                if processed_lines % 2 == 0:
                    try:
                        doc = json.loads(line)
                        yield doc
                        doc_count += 1

                        if doc_count % 1000 == 0:
                            print(f"   처리 중... {doc_count:,}개 문서")

                        if max_docs and doc_count >= max_docs:
                            print(f"⚠️  최대 문서 수({max_docs})에 도달하여 중단")
                            break

                    except json.JSONDecodeError as e:
                        print(f"⚠️  JSON 파싱 오류: {e}")
                        continue

        print(f"✅ 총 {doc_count:,}개 문서 파싱 완료")

    def bulk_index_documents(
        self,
        local_file: str = None,
        max_docs: int = None,
        batch_size: int = 500,
    ):
        """
        위키피디아 문서를 Elasticsearch에 bulk 색인

        Args:
            cirrus_url: CirrusSearch 덤프 URL (온라인 다운로드 시)
            local_file: 로컬 .json.gz 파일 경로 (로컬 파일 사용 시)
            max_docs: 최대 색인할 문서 수 (None이면 전체)
            batch_size: bulk 색인 배치 크기
        """
        if not local_file:
            raise ValueError(
                "cirrus_url 또는 local_file 중 하나는 반드시 지정해야 합니다."
            )

        print("\n📊 Elasticsearch에 문서 색인 시작...")

        def generate_actions():
            """Elasticsearch bulk API용 action 생성"""
            # 로컬 파일 또는 URL에서 문서 가져오기

            doc_generator = self.parse_local_cirrus_file(local_file, max_docs)

            for idx, doc in enumerate(doc_generator):
                # 🔍 디버깅: 첫 3개 문서의 원본 데이터 구조 출력
                if idx < 3:
                    print(f"\n{'='*80}")
                    print(f"📝 문서 #{idx + 1} 원본 데이터 구조:")
                    print(f"{'='*80}")
                    print(f"📌 사용 가능한 필드: {list(doc.keys())}")
                    print(
                        f"📄 title: {doc.get('title', 'N/A')[:100] if doc.get('title') else 'N/A'}"
                    )
                    print(f"📄 text 길이: {len(doc.get('text', ''))} 자")
                    if doc.get("text"):
                        print(f"📄 text 미리보기: {doc.get('text', '')[:200]}...")
                    print(f"📄 namespace: {doc.get('namespace', 'N/A')}")
                    print(f"📄 timestamp: {doc.get('timestamp', 'N/A')}")
                    print(
                        f"📄 opening_text: {doc.get('opening_text', 'N/A')[:150] if doc.get('opening_text') else 'N/A'}..."
                    )
                    print(f"{'='*80}\n")

                # 인덱싱할 문서 구조 정리
                source = {
                    "title": doc.get("title", ""),
                    "text": doc.get("text", ""),
                    "timestamp": doc.get("timestamp"),
                    "namespace": doc.get("namespace"),
                }

                # 선택적 필드 추가
                if "category" in doc:
                    source["category"] = doc["category"]
                if "template" in doc:
                    source["template"] = doc["template"]
                if "redirect" in doc:
                    # redirect가 객체인 경우 title만 추출, 문자열이면 그대로 사용
                    redirect_value = doc["redirect"]
                    if isinstance(redirect_value, dict):
                        source["redirect"] = redirect_value.get("title", "")
                    elif isinstance(redirect_value, str):
                        source["redirect"] = redirect_value
                if "incoming_links" in doc:
                    source["incoming_links"] = doc["incoming_links"]
                if "opening_text" in doc:
                    source["opening_text"] = doc["opening_text"]

                # 🔍 디버깅: 첫 3개 문서의 색인될 데이터 출력
                if idx < 3:
                    print("✅ 색인될 데이터:")
                    for key, value in source.items():
                        if isinstance(value, str) and len(value) > 100:
                            print(f"   - {key}: {value[:100]}... (길이: {len(value)})")
                        else:
                            print(f"   - {key}: {value}")
                    print("\n")

                yield {
                    "_index": self.index_name,
                    "_id": idx + 1,
                    "_source": source,
                }

        # Bulk 색인 실행
        success_count = 0
        error_count = 0

        for ok, result in helpers.streaming_bulk(
            self.client,
            generate_actions(),
            chunk_size=batch_size,
            raise_on_error=False,
        ):
            if ok:
                success_count += 1
            else:
                error_count += 1
                print(f"⚠️  색인 오류: {result}")

            if (success_count + error_count) % 1000 == 0:
                print(f"   색인 중... 성공: {success_count:,}, 실패: {error_count:,}")

        print("\n✅ 문서 색인 완료!")
        print(f"   - 성공: {success_count:,}개")
        print(f"   - 실패: {error_count:,}개")

        # 인덱스 refresh
        self.client.indices.refresh(index=self.index_name)

    def get_index_stats(self):
        """인덱스 통계 정보 조회"""
        stats = self.client.indices.stats(index=self.index_name)
        doc_count = stats["indices"][self.index_name]["total"]["docs"]["count"]
        size_bytes = stats["indices"][self.index_name]["total"]["store"][
            "size_in_bytes"
        ]

        print("\n📊 인덱스 통계")
        print(f"   - 인덱스: {self.index_name}")
        print(f"   - 문서 개수: {doc_count:,}")
        print(f"   - 저장 크기: {size_bytes / (1024**2):.2f} MB")

    def show_sample_documents(self, count: int = 3):
        """색인된 문서 샘플 조회"""
        print(f"\n{'='*80}")
        print(f"📋 색인된 문서 샘플 (최대 {count}개)")
        print(f"{'='*80}\n")

        try:
            result = self.client.search(
                index=self.index_name, body={"query": {"match_all": {}}, "size": count}
            )

            hits = result["hits"]["hits"]

            if not hits:
                print("❌ 색인된 문서가 없습니다.")
                return

            for i, hit in enumerate(hits, 1):
                doc = hit["_source"]
                print(f"[문서 {i}]")
                print(f"   📄 title: {doc.get('title', 'N/A')}")
                print(f"   📁 namespace: {doc.get('namespace', 'N/A')}")
                print(f"   📅 timestamp: {doc.get('timestamp', 'N/A')}")
                if doc.get("text"):
                    print(f"   📝 text 길이: {len(doc.get('text', ''))} 자")
                    print(f"   📝 text 미리보기: {doc.get('text', '')[:200]}...")
                if doc.get("opening_text"):
                    print(f"   📝 opening_text: {doc.get('opening_text', '')[:150]}...")
                print(f"{'-'*80}\n")

        except (KeyError, ValueError) as e:
            print(f"❌ 문서 조회 실패: {e}")

    def search(self, query: str, size: int = 5):
        """
        위키피디아 검색

        Args:
            query: 검색어
            size: 결과 개수
        """
        search_body = {
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": ["title^3", "opening_text^2", "text"],
                    "type": "best_fields",
                }
            },
            "size": size,
            "_source": ["title", "opening_text", "timestamp", "namespace"],
        }

        result = self.client.search(index=self.index_name, body=search_body)
        return result

    def display_search_results(self, query: str, size: int = 5):
        """검색 결과를 보기 좋게 출력"""
        print(f"\n{'=' * 80}")
        print(f"🔍 검색어: '{query}'")
        print(f"{'=' * 80}")

        result = self.search(query, size)
        hits = result["hits"]["hits"]
        total = result["hits"]["total"]["value"]

        # 🔍 디버깅: 검색 쿼리와 결과 상세 정보
        print("\n🔍 [디버깅] 검색 쿼리 정보:")
        print("   - 검색 필드: title^3, opening_text^2, text")
        print("   - 매칭 방식: best_fields")
        print(f"   - 총 매칭 문서 수: {total:,}")
        print(f"   - 요청한 결과 수: {size}")
        print(f"   - 실제 반환된 결과 수: {len(hits)}")

        print(
            f"\n📊 총 {total:,}개 문서 중 상위 {min(size, len(hits))}개 결과 (BM25 Score 기준)"
        )
        print(f"{'-' * 80}\n")

        if not hits:
            print("❌ 검색 결과가 없습니다.")
            print("\n💡 가능한 원인:")
            print("   1. 색인된 문서에 검색어가 포함되어 있지 않음")
            print("   2. Nori Tokenizer가 검색어를 다르게 분석함")
            print("   3. namespace 필터 등으로 제외됨")
            print("\n🔧 디버깅 방법:")
            print("   1. 색인된 문서 샘플 확인")
            print("   2. Analyzer 테스트 (Kibana Dev Tools):")
            print("      GET /kowiki_cirrus/_analyze")
            print("      {")
            print('        "analyzer": "korean_analyzer",')
            print(f'        "text": "{query}"')
            print("      }")
            return

        for rank, hit in enumerate(hits, 1):
            doc = hit["_source"]
            score = hit["_score"]

            print(f"[{rank}위] 📄 {doc.get('title', 'N/A')}")
            print(f"   🏆 BM25 Score: {score:.4f}")
            if "namespace" in doc:
                print(f"   📁 네임스페이스: {doc['namespace']}")
            if "timestamp" in doc:
                print(f"   📅 최종 수정: {doc['timestamp']}")
            if "opening_text" in doc:
                opening = doc["opening_text"]
                print(f"   📝 요약: {opening[:150]}...")
            print(f"{'-' * 80}\n")
