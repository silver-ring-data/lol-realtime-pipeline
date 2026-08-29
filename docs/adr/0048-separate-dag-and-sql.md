# 0048. Airflow DAG와 비즈니스 SQL 로직의 완벽한 분리 (Separation of Concerns)

> 원본: https://app.notion.com/p/3561198df8d980da8631d0f3decad621

| 상태 | 날짜 |
|---|---|
| 채택 | 2026-05-04 |

## 현상

- 기존의 배치 파이프라인(`lol_series_analysis_master_dag.py`)은 `AthenaOperator` 내부에 긴 DDL(테이블 생성) 및 DML(데이터 가공) 쿼리가 파이썬 `f-string` 형태로 하드코딩되어 있었음.
- 이로 인해 파이썬 코드의 길이가 과도하게 길어지고 가독성이 저하됨.
- 또한, 파이썬 파일 내에 쿼리가 텍스트로 존재하여 IDE의 SQL 문법 하이라이팅(Syntax Highlighting) 및 자동 완성 기능을 지원받지 못해 오타나 쿼리 오류를 사전에 잡기 어려웠음.
- 가장 큰 문제는 '작업의 흐름을 제어하는 오케스트레이션(Orchestration)'과 '데이터를 가공하는 비즈니스 로직(Transformation)'이 한 파일에 섞여 있어 **관심사의 분리(Separation of Concerns)** 원칙에 위배됨.

## 결정

비즈니스 로직(SQL)을 오케스트레이션 로직(Python)에서 완전히 분리하는 구조적 리팩토링을 단행함.

- **SQL 전용 디렉토리 생성:** Airflow `dags/` 폴더 하위에 `sql/` 폴더를 신설하여 모든 쿼리를 `.sql` 파일로 독립시킴.
- **Jinja 템플릿 도입:** 기존 파이썬 `f-string`을 통한 동적 변수 할당을 제거하고, Airflow의 네이티브 템플릿 엔진인 **Jinja Template**(`{{ params.variable_name }}`)으로 전면 교체함.
- **파라미터화 (Parameterization):** 파이썬 DAG 코드에서는 `AthenaOperator`의 `query` 속성으로 SQL 파일 경로만 지정하고, 실행에 필요한 동적 변수들은 `params` 딕셔너리를 통해 안전하게 주입하는 방식으로 개선함.

## 이유

- **유지보수성 극대화:** 데이터 파이프라인의 실행 순서를 변경할 때는 `.py` 파일만, 테이블 스키마나 분석 로직을 변경할 때는 `.sql` 파일만 수정하면 되므로 협업 및 운영이 매우 편리해짐.
- **가독성 및 개발자 경험(DX) 향상:** DAG 코드가 획기적으로 짧아졌으며, 분리된 SQL 파일은 IDE에서 완벽한 문법 하이라이팅을 지원받아 디버깅이 쉬워짐.
- **도커 환경 호환성 보장:** SQL 파일을 상위 폴더가 아닌 `dags/sql/` 내부에 위치시켜, Airflow 컨테이너의 볼륨 마운트(Volume Mount) 구조에서도 `FileNotFoundError` 없이 안전하게 작동하도록 구성함.

## 트레이드오프

(원문에 없음)
