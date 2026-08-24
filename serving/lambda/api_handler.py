"""대시보드 조회 요청을 처리하는 Lambda.

API Gateway(GET /lol)에 연결되며, OpenSearch의 세 인덱스에서 최신 상태를 조회해
하나의 응답으로 합쳐 반환한다. 프론트엔드가 0.5초 주기로 폴링하는 엔드포인트다.

응답 구조
    latest_score   최신 하이라이트 점수 1건
    latest_chats   최근 채팅 50건
    latest_viewer  최신 시청자 수 1건

각 인덱스는 파이프라인 기동 시점에 따라 아직 비어 있을 수 있으므로, 조회 실패가
전체 응답 실패로 이어지지 않도록 인덱스별로 기본값을 반환한다.

환경 변수
    OS_HOST  OpenSearch 도메인 엔드포인트
    OS_USER  마스터 사용자명
    OS_PASS  마스터 비밀번호
"""

import json
import logging
import os

from opensearchpy import OpenSearch

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 자격 증명은 반드시 환경 변수로 주입한다. 기본값을 두지 않아 설정 누락 시 즉시 실패한다.
OS_HOST = os.environ['OS_HOST']
OS_USER = os.environ['OS_USER']
OS_PASS = os.environ['OS_PASS']

CHAT_PAGE_SIZE = 50

client = OpenSearch(
    hosts=[{'host': OS_HOST, 'port': 443}],
    http_auth=(OS_USER, OS_PASS),
    use_ssl=True,
    verify_certs=True,
)


def search_latest(index, sort_field, size=1):
    """지정한 인덱스에서 최신 문서를 조회한다.

    인덱스가 아직 생성되지 않았거나 비어 있으면 빈 리스트를 반환한다.
    """
    try:
        response = client.search(index=index, body={
            "size": size,
            "sort": [{sort_field: "desc"}],
        })
        return [hit['_source'] for hit in response['hits']['hits']]
    except Exception as e:
        logger.warning(f"{index} 조회에 실패했습니다: {e}")
        return []


def lambda_handler(event, context):
    try:
        scores = search_latest("gold-highlight-scores", "window_start")
        chats = search_latest("gold-selected-chats", "ts", size=CHAT_PAGE_SIZE)
        viewers = search_latest("gold-viewer-counts", "timestamp")

        combined_data = {
            "latest_score": scores[0] if scores else {},
            "latest_chats": chats,
            "latest_viewer": viewers[0] if viewers else {"viewer_count": 0},
        }

        return {
            'statusCode': 200,
            'headers': {
                # 대시보드가 별도 오리진에서 호스팅되므로 CORS를 허용한다.
                'Access-Control-Allow-Origin': '*',
                'Content-Type': 'application/json',
            },
            'body': json.dumps(combined_data, ensure_ascii=False),
        }

    except Exception as e:
        logger.error(f"조회 처리 중 오류가 발생했습니다: {e}")
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': '데이터를 조회하지 못했습니다.'}, ensure_ascii=False),
        }
