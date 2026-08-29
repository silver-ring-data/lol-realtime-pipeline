# 0048. Airflow DAG 와 비즈니스 SQL 로직을 분리한다 (Separation of Concerns)

> 원본: https://app.notion.com/p/3561198df8d980da8631d0f3decad621

| | |
|---|---|
| 상태 | **채택** |
| 날짜 | 2026-05-04 |
| 결정자 | silver-ring-data |
| 관련 | 0032, 0046 |

## 배경

기존 배치 파이프라인(`lol_series_analysis_master_dag.py`)은 `AthenaOperator` 내부에 긴 DDL·DML 쿼리가 파이썬 `f-string` 으로 하드코딩되어 있었다. 파이썬 코드가 과도하게 길어지고, IDE 의 SQL 문법 하이라이팅·자동 완성을 지원받지 못해 오류를 사전에 잡기 어려웠다. 무엇보다 오케스트레이션과 데이터 변환 로직이 한 파일에 섞여 관심사의 분리 원칙에 위배되었다.

## 선택지

| 선택지 | 장점 | 단점 |
|---|---|---|
| A. `dags/sql/` 에 `.sql` 파일 분리 + Jinja 템플릿 + `params` 주입 | 유지보수성, 가독성, IDE 지원, 도커 볼륨 마운트 호환 | 파일 수 증가 |
| B. f-string 하드코딩 유지 | 변경 없음 | 가독성 저하, 관심사 혼재 |

## 결정

**A 를 채택한다.**

- Airflow `dags/` 하위에 `sql/` 폴더를 만들어 모든 쿼리를 `.sql` 파일로 독립시킨다.
- 파이썬 `f-string` 동적 변수 할당을 제거하고 Airflow 네이티브 Jinja Template(`{{ params.variable_name }}`)으로 교체한다.
- DAG 코드에서는 `AthenaOperator` 의 `query` 에 SQL 파일 경로만 지정하고, 동적 변수는 `params` 딕셔너리로 주입한다.
- SQL 파일을 상위 폴더가 아닌 `dags/sql/` 내부에 두어 Airflow 컨테이너 볼륨 마운트 구조에서도 `FileNotFoundError` 없이 동작하게 한다.

## 결과

- 좋아지는 것: 실행 순서 변경은 `.py` 만, 스키마·분석 로직 변경은 `.sql` 만 수정하면 되고, DAG 코드가 짧아지며 SQL 디버깅이 쉬워진다.
- 감수하는 것: 파일 수 증가.
- 다시 볼 조건: SQL 파일이 많아져 별도 템플릿 관리가 필요해질 때.
