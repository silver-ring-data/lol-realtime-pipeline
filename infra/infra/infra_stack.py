from aws_cdk import (
    Stack,
    aws_kinesis as kinesis,
    RemovalPolicy,
)
from constructs import Construct

class InfraStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 1. 게임 지표 수집용 Kinesis Data Stream
        game_stream = kinesis.Stream(self, "LolGameStream",
            stream_name="lol-game-stream",
            shard_count=1,
            # 실습용 스택이므로 삭제 시 스트림도 함께 제거한다.
            removal_policy=RemovalPolicy.DESTROY 
        )

        # 2. 채팅 수집용 Kinesis Data Stream
        chat_stream = kinesis.Stream(self, "LolChatStream",
            stream_name="lol-chat-stream",
            shard_count=1,
            removal_policy=RemovalPolicy.DESTROY
        )