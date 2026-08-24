# OpenSearch 인덱스 템플릿

Gold 스트림을 적재하기 전에 등록해야 하는 인덱스 템플릿입니다.
OpenSearch Dashboards의 **Dev Tools** 콘솔에서 실행합니다.

## 등록이 필요한 이유

동적 매핑에 맡기면 `window_start`와 `ts`가 문자열(`text`)로 추론되어
시간 기준 정렬과 범위 질의가 동작하지 않습니다. 대시보드는 최신 문서를
`sort: desc`로 조회하므로, 적재를 시작하기 **전에** 날짜 타입을 고정해야 합니다.

이미 문자열로 매핑된 인덱스가 있다면 매핑을 변경할 수 없으므로 삭제 후
템플릿을 등록하고 다시 적재합니다.

```json
DELETE /gold-highlight-scores
```

## 1. 하이라이트 점수

```json
PUT _index_template/gold_scores_template
{
  "index_patterns": ["gold-highlight-scores*"],
  "template": {
    "mappings": {
      "properties": {
        "window_start": {
          "type": "date",
          "format": "yyyy-MM-dd HH:mm:ss"
        },
        "match_id":    { "type": "keyword" },
        "final_score": { "type": "double" },
        "event_score": { "type": "double" },
        "chat_score":  { "type": "integer" }
      }
    }
  }
}
```

## 2. 서빙용 채팅

`nickname`은 집계 및 정확 일치 조회 대상이므로 `keyword`,
`content`는 전문 검색 대상이므로 `text`로 지정합니다.

```json
PUT _index_template/gold_chats_template
{
  "index_patterns": ["gold-selected-chats*"],
  "template": {
    "mappings": {
      "properties": {
        "ts": {
          "type": "date",
          "format": "yyyy-MM-dd HH:mm:ss"
        },
        "nickname": { "type": "keyword" },
        "content":  { "type": "text" },
        "platform": { "type": "keyword" },
        "priority": { "type": "integer" }
      }
    }
  }
}
```

## 3. 시청자 수

시청자 봇은 밀리초 단위 epoch를 보내므로 `epoch_millis` 포맷을 사용합니다.

```json
PUT _index_template/gold_viewer_template
{
  "index_patterns": ["gold-viewer-counts*"],
  "template": {
    "mappings": {
      "properties": {
        "timestamp": {
          "type": "date",
          "format": "epoch_millis"
        },
        "match_id":         { "type": "keyword" },
        "viewer_count":     { "type": "integer" },
        "elapsed_seconds":  { "type": "integer" }
      }
    }
  }
}
```
