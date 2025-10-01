# -*- coding: utf-8 -*-
"""
ElasticSearch 한국어 검색엔진 구축 프로젝트
- Nori Tokenizer를 사용한 custom analyzer
- BM25 검색 모델
- 한국 위키피디아 데이터 색인 및 검색
"""

import os
from elasticsearch import Elasticsearch
from wiki_loader import WikipediaCirrusLoader


class KoreanSearchEngine:
    def __init__(
        self,
        host="localhost",
        port=9200,
        username="elastic",
        password="hosun",
    ):
        """ElasticSearch 클라이언트 초기화"""
        self.client = Elasticsearch(
            [f"http://{host}:{port}"], basic_auth=(username, password)
        )
        self.index_name = "kowiki_cirrus"
        self.wiki_loader = WikipediaCirrusLoader(
            es_url=f"http://{host}:{port}",
            index_name=self.index_name,
            username=username,
            password=password,
        )

    def check_connection(self):
        """ElasticSearch 연결 확인"""
        if self.client.ping():
            print("✅ ElasticSearch 연결 성공!")
            info = self.client.info()
            print(f"   버전: {info['version']['number']}")
            print(f"   클러스터: {info['cluster_name']}")
            return True
        else:
            print("❌ ElasticSearch 연결 실패!")
            return False

    def index_exists_with_data(self):
        """인덱스가 존재하고 데이터가 있는지 확인"""
        if not self.client.indices.exists(index=self.index_name):
            return False

        stats = self.client.indices.stats(index=self.index_name)
        doc_count = stats["indices"][self.index_name]["total"]["docs"]["count"]
        return doc_count > 0

    def create_index(self):
        """
        Nori Tokenizer를 사용한 custom analyzer로 인덱스 생성
        참고: https://esbook.kimjmin.net/06-text-analysis
        """
        self.wiki_loader.create_index()

    def bulk_index_documents(self, max_docs=None, local_file=None):
        """
        위키피디아 문서를 Bulk API로 색인

        Args:
            max_docs: 최대 색인할 문서 수 (None이면 전체)
            cirrus_url: 위키피디아 CirrusSearch 덤프 URL (온라인 다운로드 시)
            local_file: 로컬 .json.gz 파일 경로 (로컬 파일 사용 시)
        """
        print(f"\n📂 로컬 파일 사용: {local_file}")
        self.wiki_loader.bulk_index_documents(
            local_file=local_file, max_docs=max_docs, batch_size=500
        )

    def search(self, query: str, size: int = 5):
        """
        BM25 모델을 사용한 검색
        참고: https://esbook.kimjmin.net/05-search
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

        print(
            f"\n📊 총 {total:,}개 문서 중 상위 {min(size, len(hits))}개 결과 (BM25 Score 기준)"
        )
        print(f"{'-' * 80}\n")

        if not hits:
            print("검색 결과가 없습니다.")
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


def main():
    """메인 실행 함수"""
    # ElasticSearch 연결
    print("\n[STEP 1] ElasticSearch 연결 중...")
    engine = KoreanSearchEngine()
    if not engine.check_connection():
        return

    # 인덱스 확인 및 생성
    print("\n[STEP 2] 인덱스 확인 중...")

    if engine.index_exists_with_data():
        print(f"✅ 인덱스 '{engine.index_name}'에 데이터가 이미 존재합니다.")
        use_existing = (
            input("기존 데이터를 사용하시겠습니까? (y/n, 기본값 y): ").strip().lower()
            or "y"
        )

        if use_existing == "y":
            print("📊 기존 데이터를 사용합니다.")
        else:
            print("🔄 인덱스를 재생성합니다...")
            # 기존 인덱스 삭제
            engine.client.indices.delete(index=engine.index_name)
            engine.create_index()

            # 문서 색인
            print("\n[STEP 3] 문서 색인 중...")

            local_file = os.path.join(
                os.path.dirname(__file__),
                "kowiki-20250922-cirrussearch-content.json.gz",
            )
            max_docs_input = (
                input("최대 문서 수 (기본값 10000, 전체는 'all'): ").strip() or "10000"
            )
            max_docs = None if max_docs_input.lower() == "all" else int(max_docs_input)
            engine.bulk_index_documents(max_docs=max_docs, local_file=local_file)

    else:
        print(f"ℹ️  인덱스 '{engine.index_name}'가 존재하지 않거나 데이터가 없습니다.")
        print("📝 인덱스를 생성하고 데이터를 색인합니다...")

        engine.create_index()

        # 문서 색인
        print("\n[STEP 3] 문서 색인 중...")

        # 로컬 파일 경로 입력
        local_file = os.path.join(
            os.path.dirname(__file__),
            "kowiki-20250922-cirrussearch-content.json.gz",
        )
        max_docs_input = (
            input("최대 문서 수 (기본값 10000, 전체는 'all'): ").strip() or "10000"
        )
        max_docs = None if max_docs_input.lower() == "all" else int(max_docs_input)
        engine.bulk_index_documents(max_docs=max_docs, local_file=local_file)

    # 인덱스 통계
    engine.get_index_stats()

    # 색인된 문서 샘플 확인
    engine.wiki_loader.show_sample_documents(count=3)

    while True:
        query = input("\n검색어 입력: ").strip()
        if query.lower() == "q":
            break
        if query:
            engine.display_search_results(query, size=5)

    print("\n✅ 프로그램 종료")


if __name__ == "__main__":
    main()
