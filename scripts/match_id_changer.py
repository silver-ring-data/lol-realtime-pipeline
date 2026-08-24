"""경기 원본 JSON의 match_id를 일괄 치환하는 유틸리티.

수집 시점과 파이프라인에서 사용하는 match_id 규칙이 다를 때, Bronze 적재 전에
원본 파일의 match_id를 파티션 키 규칙에 맞게 맞추기 위해 사용한다.

사용 예:
    python scripts/match_id_changer.py \
        data_source/producer/game/data/t1_blg_g4_exact.json \
        worlds_2024_20241102_t1_blg_g4
"""

import argparse
import json
import os
import sys


def update_match_id(file_path, new_id):
    """지정한 JSON 파일의 모든 항목에서 match_id를 new_id로 치환한다."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        updated = 0
        for entry in data:
            if 'match_id' in entry:
                entry['match_id'] = new_id
                updated += 1

        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        print(f"수정 완료: {os.path.basename(file_path)} ({updated}건)")
        return True

    except FileNotFoundError:
        print(f"파일을 찾을 수 없습니다: {file_path}", file=sys.stderr)
    except json.JSONDecodeError as e:
        print(f"JSON 형식이 올바르지 않습니다: {file_path} ({e})", file=sys.stderr)
    except OSError as e:
        print(f"파일 처리 중 오류가 발생했습니다: {e}", file=sys.stderr)
    return False


def main():
    parser = argparse.ArgumentParser(description="경기 원본 JSON의 match_id를 일괄 치환한다.")
    parser.add_argument('file_path', help="대상 JSON 파일 경로")
    parser.add_argument('new_match_id', help="새로 지정할 match_id")
    args = parser.parse_args()

    if not update_match_id(args.file_path, args.new_match_id):
        sys.exit(1)


if __name__ == "__main__":
    main()
