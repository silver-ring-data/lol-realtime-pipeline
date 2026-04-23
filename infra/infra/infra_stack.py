from aws_cdk import (
    Stack,
    aws_kinesis as kinesis,
    RemovalPolicy,
)
from constructs import Construct

class InfraStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # 🎮 1. 게임 데이터용 Kinesis Stream (파이프라인 입구 1)
        game_stream = kinesis.Stream(self, "LolGameStream",
            stream_name="lol-game-stream",
            shard_count=1,
            # 프로젝트 끝나고 스택 지울 때 깔끔하게 같이 날아가게 설정!
            removal_policy=RemovalPolicy.DESTROY 
        )

        # 💬 2. 채팅 데이터용 Kinesis Stream (파이프라인 입구 2)
        chat_stream = kinesis.Stream(self, "LolChatStream",
            stream_name="lol-chat-stream",
            shard_count=1,
            removal_policy=RemovalPolicy.DESTROY
        )