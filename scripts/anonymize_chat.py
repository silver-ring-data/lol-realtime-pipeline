"""채팅 데이터 익명화 스크립트.

수집한 라이브 채팅에는 시청자 닉네임이 그대로 포함되어 있어 저장소에 원본을
커밋할 수 없다. 이 스크립트는 닉네임을 복원 불가능한 형태로 치환하고, 저장소에
포함할 샘플 크기로 축소한다.

익명화 방식
    - 닉네임을 solt를 포함한 SHA-256으로 해싱하여 ``viewer_xxxxxxxx`` 형태로 치환한다.
    - 같은 닉네임은 항상 같은 값으로 매핑되므로 사용자 단위 집계 로직은 그대로 검증할 수 있다.
    - salt를 모르면 원래 닉네임을 역산할 수 없다.
    - 봇 계정(@nightbot 등)은 분석에서 제외 대상이므로 별도 라벨을 부여한다.

메시지 본문은 하이라이트 탐지 로직의 입력이므로 유지한다. 다만 본문에 다른
사용자를 지칭하는 @멘션이 포함된 경우 해당 부분도 함께 익명화한다.

사용 예:
    python scripts/anonymize_chat.py \
        --input  data_source/producer/chat/data/t1_blg_chat_raw.jsonl \
        --output data_source/producer/chat/data/t1_blg_chat.jsonl \
        --sample 500
"""

import argparse
import hashlib
import json
import os
import re
import sys

# 분석 대상에서 제외되는 봇 계정
BOT_ACCOUNTS = {"@nightbot", "@streamlabs", "@moobot"}

# 메시지 본문에 포함된 @멘션
MENTION_PATTERN = re.compile(r'@[^\s:,]+')


def anonymize_nickname(nickname, salt):
    """닉네임을 복원 불가능한 익명 ID로 치환한다."""
    if not nickname:
        return "viewer_unknown"
    if nickname.lower() in BOT_ACCOUNTS:
        return "bot_account"
    digest = hashlib.sha256((salt + nickname).encode('utf-8')).hexdigest()
    return f"viewer_{digest[:8]}"


def anonymize_content(content, salt):
    """메시지 본문 안의 @멘션을 익명 ID로 치환한다."""
    if not content:
        return content
    return MENTION_PATTERN.sub(lambda m: anonymize_nickname(m.group(0), salt), content)


def count_records(input_path):
    """유효한 레코드 수를 센다."""
    with open(input_path, 'r', encoding='utf-8') as f:
        return sum(1 for line in f if line.strip())


def process(input_path, output_path, salt, sample):
    """입력 JSONL을 익명화하여 출력 JSONL로 기록한다.

    sample이 지정되면 파일 전체에서 균등 간격으로 추출한다. 앞부분만 잘라내면
    채팅 밀도의 시간 분포가 사라져 하이라이트 탐지 로직을 검증할 수 없기 때문에,
    구간별 밀도 비율을 유지하도록 등간격으로 뽑는다.
    """
    total = count_records(input_path)
    stride = 1
    if sample and total > sample:
        stride = total // sample

    seen = 0
    written = 0
    skipped = 0

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    with open(input_path, 'r', encoding='utf-8') as src, \
            open(output_path, 'w', encoding='utf-8') as dst:
        for line in src:
            line = line.strip()
            if not line:
                continue

            index = seen
            seen += 1
            if index % stride != 0:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                skipped += 1
                continue

            record['nickname'] = anonymize_nickname(record.get('nickname', ''), salt)
            record['content'] = anonymize_content(record.get('content', ''), salt)

            dst.write(json.dumps(record, ensure_ascii=False) + '\n')
            written += 1

    print(f"입력 {total}건 중 {written}건을 익명화하여 기록했습니다: {output_path}")
    if stride > 1:
        print(f"전체에서 {stride}건마다 1건씩 균등 추출했습니다.")
    if skipped:
        print(f"파싱에 실패해 건너뛴 레코드: {skipped}건")


def main():
    parser = argparse.ArgumentParser(description="채팅 JSONL의 닉네임을 익명화한다.")
    parser.add_argument('--input', required=True, help="원본 JSONL 경로")
    parser.add_argument('--output', required=True, help="익명화 결과를 저장할 경로")
    parser.add_argument('--salt', default=os.environ.get('CHAT_ANON_SALT', ''),
                        help="해시 salt (환경 변수 CHAT_ANON_SALT로도 지정 가능)")
    parser.add_argument('--sample', type=int, default=0,
                        help="기록할 최대 레코드 수 (0이면 전체)")
    args = parser.parse_args()

    if not args.salt:
        print("salt가 지정되지 않았습니다. --salt 또는 CHAT_ANON_SALT를 설정하세요.",
              file=sys.stderr)
        sys.exit(1)

    if not os.path.exists(args.input):
        print(f"입력 파일을 찾을 수 없습니다: {args.input}", file=sys.stderr)
        sys.exit(1)

    process(args.input, args.output, args.salt, args.sample)


if __name__ == "__main__":
    main()
