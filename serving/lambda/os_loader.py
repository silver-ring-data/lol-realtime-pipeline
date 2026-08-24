"""Gold 스트림을 OpenSearch로 적재하는 Lambda.

Kinesis Data Streams 트리거로 실행되며, 이벤트 소스 ARN을 보고 레코드를 적절한
OpenSearch 인덱스로 라우팅한 뒤 벌크 API로 한 번에 적재한다.

라우팅 규칙
    ARN에 'chat'   포함 -> gold-selected-chats
    ARN에 'score'  포함 -> gold-highlight-scores
    ARN에 'viewer' 포함 -> gold-viewer-counts

문서 ID를 레코드 값에서 결정론적으로 생성하므로, Lambda가 재시도되어 같은
레코드가 다시 들어와도 중복 문서가 쌓이지 않는다(멱등성).

환경 변수
    OS_HOST  OpenSearch 도메인 엔드포인트
    OS_USER  마스터 사용자명
    OS_PASS  마스터 비밀번호

인덱스 매핑은 serving/opensearch/index_templates.md를 참고한다.
"""

import base64
import json
import logging
import os

from opensearchpy import OpenSearch, helpers

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 자격 증명은 반드시 환경 변수로 주입한다. 기본값을 두지 않아 설정 누락 시 즉시 실패한다.
OS_HOST = os.environ['OS_HOST']
OS_USER = os.environ['OS_USER']
OS_PASS = os.environ['OS_PASS']

# 소스 스트림 종류별 대상 인덱스와 문서 ID 구성 필드
ROUTING_RULES = (
    ('chat', 'gold-selected-chats', ('nickname', 'ts')),
    ('score', 'gold-highlight-scores', ('match_id', 'window_start')),
    ('viewer', 'gold-viewer-counts', ('match_id', 'timestamp')),
)

client = OpenSearch(
    hosts=[{'host': OS_HOST, 'port': 443}],
    http_compress=True,
    http_auth=(OS_USER, OS_PASS),
    use_ssl=True,
    verify_certs=True,
    ssl_assert_hostname=False,
    ssl_show_warn=False,
)


def resolve_target(source_arn, data):
    """이벤트 소스 ARN을 기준으로 대상 인덱스와 문서 ID를 결정한다."""
    for keyword, index, id_fields in ROUTING_RULES:
        if keyword in source_arn:
            doc_id = "_".join(str(data.get(field, 'unknown')) for field in id_fields)
            return index, doc_id
    return 'default-index', None


def lambda_handler(event, context):
    actions = []

    for record in event['Records']:
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        data = json.loads(payload)

        target_index, doc_id = resolve_target(record['eventSourceARN'], data)

        action = {"_index": target_index, "_source": data}
        if doc_id:
            action["_id"] = doc_id
        actions.append(action)

    if not actions:
        return {'statusCode': 200, 'body': json.dumps('처리할 레코드가 없습니다.')}

    try:
        success, errors = helpers.bulk(client, actions)
        if errors:
            logger.error(f"일부 문서 적재에 실패했습니다: {errors}")
        logger.info(f"{success}건의 문서를 적재했습니다.")
    except Exception as e:
        logger.error(f"OpenSearch 적재 중 오류가 발생했습니다: {e}")
        # 예외를 다시 발생시켜 Kinesis 트리거가 해당 배치를 재시도하도록 한다.
        raise

    return {
        'statusCode': 200,
        'body': json.dumps(f'{success}건 적재 완료')
    }
