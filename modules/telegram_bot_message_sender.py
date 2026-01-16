__version__ = "1.5"

import asyncio
import os
import sys
import logging

from pprint import pformat
from telegram import Bot, Message
from telegram.error import TelegramError
from typing import TypedDict, Optional
import html
from modules.logger import LoggerFactory

logger = LoggerFactory.get_logger(name="deal", log_filename="telegram_bot.log", level=logging.DEBUG,
                                       split_levels=False, use_timed_rotating=True, use_dated_folder=True,
                                       add_date_to_filename=False, add_time_to_filename=True,
                                       base_logs_dir=os.path.abspath(
                                               os.path.join(os.path.dirname(__file__), '..', 'deals_log')))

class MessageInfo(TypedDict):
    chat_id: int
    message_id: int
    original_text: str


class TelegramMessageSender:
    _env_loaded = False
    TELEGRAM_MAX_LEN = 4096

    def __init__(self, bot_token_env: str = "TELEGRAM_BOT_TOKEN", chat_id_env: str = "TELEGRAM_CHAT_ID") -> None:
        if not self.__class__._env_loaded:
            self._load_env()
            self.__class__._env_loaded = True

        bot_token = self._get_required_env(bot_token_env)
        chat_id_raw = self._get_required_env(chat_id_env)

        try:
            self.chat_id: int = int(chat_id_raw)
        except (ValueError, TypeError) as e:
            message = f"Значение переменной '{chat_id_env}' должно быть целым числом (chat_id). Получено: {chat_id_raw!r}"
            logger.error(message)
            raise ValueError(message) from e

        if not bot_token.strip():
            message = f"Переменная окружения '{bot_token_env}' пуста"
            logger.error(message)
            raise ValueError(message)

        self.bot = Bot(token=bot_token)
        self._last_message_info: Optional[MessageInfo] = None

    # === ENV ===
    @classmethod
    def _load_env(cls) -> None:
        project_root = os.path.dirname(os.path.dirname(__file__))
        env_path = os.path.join(project_root, ".env")
        if os.path.isfile(env_path):
            try:
                from dotenv import load_dotenv
                load_dotenv(dotenv_path=env_path)
                print(f"✅ Переменные окружения загружены из: {env_path}")
                logger.debug(f"✅ Переменные окружения загружены из: {env_path}")
            except ImportError:
                print("ℹ️ python-dotenv не установлен.")
                logger.warning("ℹ️ python-dotenv не установлен.")
        else:
            print("ℹ️ Файл .env не найден. Используются системные переменные окружения.")
            logger.warning("ℹ️ Файл .env не найден. Используются системные переменные окружения.")

    @staticmethod
    def _get_required_env(key: str) -> str:
        value = os.getenv(key)
        if value is None:
            print(f"❌ Ошибка: переменная окружения '{key}' не задана!")
            logger.error(f"❌ Ошибка: переменная окружения '{key}' не задана!")
            sys.exit(1)
        return value

    # === Форматирование ===
    @staticmethod
    def _format_message_as_dict(text: str) -> dict[int, str]:
        lines = text.strip().split("\n")
        return {i + 1: line for i, line in enumerate(lines)}

    def _format_numbered_text(self, text: str) -> str:
        msg_dict = self._format_message_as_dict(text)
        return "\n".join(f"{k}: {v}" for k, v in msg_dict.items())

    def _auto_format_plain(self, obj) -> str:
        """Форматирует объект в экранированный plain text (без HTML-тегов)."""
        if isinstance(obj, str):
            return html.escape(obj)
        formatted = pformat(obj, width=70, sort_dicts=False)
        return html.escape(formatted)

    def _split_and_wrap(self, plain_text: str) -> list[str]:
        """Разбивает plain-текст на части и оборачивает каждую в <pre>...</pre>."""
        max_content_len = self.TELEGRAM_MAX_LEN - len("<pre></pre>")
        chunks = []
        remaining = plain_text

        while len(remaining) > max_content_len:
            # Берём кусок до лимита
            cut = remaining[:max_content_len]
            # Ищем последний перенос строки для аккуратного разрыва
            last_newline = cut.rfind("\n")
            if last_newline == -1:
                # Если нет переноса — режем по максимуму
                split_pos = max_content_len
            else:
                split_pos = last_newline

            chunk = remaining[:split_pos].rstrip("\n")
            chunks.append(f"<pre>{chunk}</pre>")
            remaining = remaining[split_pos:].lstrip("\n")

        # Добавляем остаток
        if remaining.strip():
            chunks.append(f"<pre>{remaining.strip()}</pre>")

        return chunks

    # === Telegram ===
    async def send_numbered_message(self, text) -> bool:
        plain = self._format_numbered_text(self._auto_format_plain(text))
        chunks = self._split_and_wrap(plain)
        success = True

        for i, chunk in enumerate(chunks, start=1):
            try:
                message: Message = await self.bot.send_message(
                    chat_id=self.chat_id,
                    text=chunk,
                    parse_mode="HTML"
                )
                # Сохраняем информацию только о последнем сообщении
                self._last_message_info = {
                    "chat_id": self.chat_id,
                    "message_id": message.message_id,
                    "original_text": str(text),
                }
                logger.info(f'***"{text}"***')
                print(f"✅ Сообщение {i}/{len(chunks)} отправлено ({len(chunk)} символов)")
                logger.debug(f"✅ Сообщение {i}/{len(chunks)} отправлено ({len(chunk)} символов)")
            except TelegramError as e:
                print(f"❌ Ошибка Telegram API при отправке части {i}: {e}")
                logger.error(f"❌ Ошибка Telegram API при отправке части {i}: {e}")
                success = False
                break
        return success

    async def edit_last_message(self, new_text) -> bool:
        if self._last_message_info is None:
            print("⚠️ Нет сохранённого сообщения для редактирования.")
            logger.warning("⚠️ Нет сохранённого сообщения для редактирования.")
            return False

        plain = self._format_numbered_text(self._auto_format_plain(new_text))
        max_content_len = self.TELEGRAM_MAX_LEN - len("<pre></pre>")
        if len(plain) > max_content_len:
            print("⚠️ Сообщение слишком длинное для редактирования, будет отправлено как новое.")
            logger.warning("⚠️ Сообщение слишком длинное для редактирования, будет отправлено как новое.")
            return await self.send_numbered_message(new_text)

        try:
            await self.bot.edit_message_text(
                chat_id=self._last_message_info["chat_id"],
                message_id=self._last_message_info["message_id"],
                text=f"<pre>{plain}</pre>",
                parse_mode="HTML"
            )
            self._last_message_info["original_text"] = str(new_text)
            print("✅ Последнее сообщение успешно отредактировано!")
            logger.debug("✅ Последнее сообщение успешно отредактировано!")
            return True
        except TelegramError as e:
            print(f"❌ Ошибка Telegram API при редактировании: {e}")
            logger.error(f"❌ Ошибка Telegram API при редактировании: {e}")
            return False

    async def append_to_last_message(self, text_to_append) -> bool:
        if self._last_message_info is None:
            print("⚠️ Нет сохранённого сообщения для редактирования.")
            logger.warning("⚠️ Нет сохранённого сообщения для редактирования.")
            return False

        original = self._last_message_info["original_text"]
        updated_text = str(original) + "\n" + str(text_to_append)
        return await self.edit_last_message(updated_text)

    def clear_last_message_info(self) -> None:
        self._last_message_info = None
        print("🧹 Информация о последнем сообщении очищена.")
        logger.debug("🧹 Информация о последнем сообщении очищена.")


# === Пример использования ===
async def main():
    sender = TelegramMessageSender(bot_token_env="DEAL_BOT_TOKEN", chat_id_env="DEAL_CHAT_ID")

    # Пример с очень большим словарём
    big_dict = {f"key_{i}": i for i in range(300)}
    await sender.send_numbered_message(big_dict)

    # Добавим обновление
    await sender.append_to_last_message("✅ Обновление завершено")

    # Очистка (опционально)
    sender.clear_last_message_info()

    sender = TelegramMessageSender(
        bot_token_env="DEAL_BOT_TOKEN",
        chat_id_env="DEAL_CHAT_ID"
    )

    # Отправляем первое сообщение
    await sender.send_numbered_message("Первая строка")

    # Дописываем
    await sender.append_to_last_message("Вторая строка")

    # Теперь очищаем — следующее сообщение будет "новым"
    sender.clear_last_message_info()

    # Это уже не отредактирует старое, а отправит новое
    await sender.send_numbered_message("Новое сообщение")


if __name__ == "__main__":
    asyncio.run(main())