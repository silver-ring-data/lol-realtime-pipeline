# 아키텍처 다이어그램

draw.io([app.diagrams.net](https://app.diagrams.net))로 작성한 원본 파일입니다.

| 파일 | 내용 |
|---|---|
| `architecture_overview.drawio` | 전체 파이프라인 구조 (Speed / Batch Layer) |
| `speed_layer.drawio` | 실시간 처리 경로 상세 |

## 보는 방법

`.drawio`는 XML 형식이라 GitHub에서 바로 그림으로 표시되지 않습니다.
[app.diagrams.net](https://app.diagrams.net)에서 **File > Open From > Device**로
열면 됩니다.

## 이미지로 내보내기

README에 그림을 삽입하려면 draw.io에서
**File > Export as > PNG**(Zoom 200%, Transparent background 해제)로 내보내
같은 폴더에 저장합니다.

```
docs/diagrams/architecture_overview.png
docs/diagrams/speed_layer.png
```

내보낸 뒤 최상위 README의 아키텍처 섹션에 아래와 같이 삽입합니다.

```markdown
![전체 아키텍처](docs/diagrams/architecture_overview.png)
```
