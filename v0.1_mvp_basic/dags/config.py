'''
pip install chzzk-sdk
'''

import os
from datetime import datetime
import boto3
import json

class Config:
    # 1. 버전 및 기본 정보
    VERSION = "v0.3"
    PROJECT_TAGS = ['real_data', 'eunbee']
    START_DATE = datetime(2026, 4, 15)
    
    # 2. 운영 변수 (중앙 제어)
    MATCH_ID = "WORLDS_2024_FINAL_T1_BLG"
    
    # 3. 연결 정보 (도커 환경변수 혹은 Airflow Conn ID)
    DB_CONN_ID = 'postgres_db_eunbee'
    S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "eunbee-data-lake")
    
    # 4. AWS 인증 (도커가 .env를 읽어서 환경변수로 넣어줌)
    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

    @staticmethod
    def upload_to_s3(data, folder, filename):
        """오늘 배운 boto3 로직을 공통 함수로 빼기!"""
        s3 = boto3.client(
            's3',
            aws_access_key_id=Config.AWS_ACCESS_KEY,
            aws_secret_access_key=Config.AWS_SECRET_KEY
        )
        s3.put_object(
            Bucket=Config.S3_BUCKET_NAME,
            Key=f"{folder}/{Config.VERSION}/{filename}.json",
            Body=json.dumps(data, ensure_ascii=False)
        )
        print(f"✅ S3 적재 완료: {folder}/{filename}")