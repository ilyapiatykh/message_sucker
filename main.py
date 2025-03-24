from datetime import datetime

from pydantic import BaseModel, Field, TypeAdapter
from pydantic_settings import BaseSettings
from pytz import timezone
from telethon.sync import TelegramClient

TG_SESSION_FILE_PATH = "telethon_session"


class Config(BaseSettings):
    api_id: str = Field(validation_alias="API_ID")
    api_hash: str = Field(validation_alias="API_HASH")
    channel_id: int = Field(validation_alias="CHANNEL_ID")


class Message(BaseModel):
    id: str
    text: str | None
    reply_to_msg_id: int | None
    send_time: datetime | None


async def get_messages_from_channel(client: TelegramClient, channel_id: int):
    offset_date = datetime(year=2025, month=3, day=8, tzinfo=timezone("Europe/Moscow"))
    limit = 1000
    wait_time = 2
    min_id = None
    messages = []

    while True:
        for i in client.iter_messages(
            entity=channel_id,
            limit=limit,
            offset_date=offset_date,
            min_id=min_id,
            wait_time=wait_time,
            reverse=True,
        ):
            message = Message(
                id=i.id,
                text=i.raw_text,
                reply_to_msg_id=i.reply_to_msg_id,
                send_time=i.date,
            )

            messages.append(message)

        if len(messages) % 2 != 0:
            break

        min_id = messages[-1].id

    messages_to_json(messages)


def messages_to_json(messages: list[Message]):
    json_data = TypeAdapter(list[Message]).dump_json(messages, exclude_unset=True, round_trip=True)

    with open("messages.json", "wb") as f:
        f.write(json_data)


def main():
    config = Config.model_validate()

    telethon_client = TelegramClient(
        TG_SESSION_FILE_PATH, int(config.api_id.get_secret_value()), config.api_hash.get_secret_value()
    )

    with telethon_client:
        telethon_client.loop.run_until_complete(get_messages_from_channel(telethon_client))


if __name__ == "__main__":
    main()
