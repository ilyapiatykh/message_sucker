import argparse
import sys
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, Field, SecretStr, TypeAdapter
from pydantic_settings import BaseSettings
from pytz import timezone
from telethon.sync import TelegramClient
from tqdm.asyncio import tqdm

DATA_DIR_NAME = "data"
TG_SESSION_FILE_NAME = "message_sucker_telethon"


class Config(BaseSettings):
    api_id: SecretStr = Field(validation_alias="API_ID")
    api_hash: SecretStr = Field(validation_alias="API_HASH")
    channel_id: SecretStr = Field(validation_alias="CHANNEL_ID")


class Message(BaseModel):
    id: int
    text: str | None
    reply_to_msg_id: int | None
    send_time: datetime | None


async def get_messages_from_channel(client: TelegramClient, channel_id: int, output: Path):
    offset_date = datetime(year=2025, month=3, day=8, tzinfo=timezone("Europe/Moscow"))
    wait_time = 3
    messages = []

    async for i in tqdm(
        client.iter_messages(
            entity=channel_id,
            offset_date=offset_date,
            wait_time=wait_time,
            reverse=True,
        )
    ):
        message = Message(
            id=i.id,
            text=i.raw_text,
            reply_to_msg_id=i.reply_to_msg_id,
            send_time=i.date,
        )

        messages.append(message)

    messages_to_json(messages, output)


def messages_to_json(messages: list[Message], output: Path):
    json_data = TypeAdapter(list[Message]).dump_json(messages, exclude_unset=True, round_trip=True)

    with open(output, "wb") as f:
        f.write(json_data)


def resolve_path():
    data_dir = Path(__file__).resolve().parent.joinpath(DATA_DIR_NAME)
    data_dir.mkdir(exist_ok=True)

    return data_dir.joinpath(TG_SESSION_FILE_NAME)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output", type=str)

    args = parser.parse_args()
    output = Path(args.output).resolve()

    if not output.parent.exists():
        print("output filepath should ba valid!")
        sys.exit(1)

    config = Config.model_validate({})

    session = resolve_path()

    telethon_client = TelegramClient(
        session,
        int(config.api_id.get_secret_value()),
        config.api_hash.get_secret_value(),
        device_model="arbuz phone",  # https://qna.habr.com/q/1232932
        system_version="Arbuz OS",
        app_version="1.0.0",
    )

    with telethon_client:
        telethon_client.loop.run_until_complete(
            get_messages_from_channel(telethon_client, int(config.channel_id.get_secret_value()), output)
        )


if __name__ == "__main__":
    main()
