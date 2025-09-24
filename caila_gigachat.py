import os
from openai import OpenAI


def send_message_to_caila(prompt: str) -> dict:
    """
    Отправляет сообщение в Caila API через OpenAI-совместимый клиент.
    Возвращает полный объект completion (для доступа к token usage).
    """
    api_key = "1000240534.196114.1nshHszzdOz1Tr3lddQeJmjozj8xNKoPy4MdrvLa"
    if not api_key:
        raise ValueError("❌ Переменная окружения MLP_API_KEY не установлена. "
                         "Установите её с вашим API-ключом из Caila.")

    # Инициализация клиента
    client = OpenAI(
        api_key=api_key,
        base_url="https://caila.io/api/adapters/openai"
    )

    # Отправляем запрос
    completion = client.chat.completions.create(
        model="just-ai/gigachat/GigaChat-2-Pro",
        messages=[
            {"role": "user", "content": prompt}
        ]
    )

    return completion


if __name__ == "__main__":
    try:
        # 🔧 Заранее заданный промт (можно заменить на любой)
        user_prompt = "Придумай тост на день рождения в 2 предложениях, чтобы рассмешить гостей."
        print(f"📨 Отправляю запрос в Caila: {user_prompt}")

        completion = send_message_to_caila(user_prompt)
        response_text = completion.choices[0].message.content

        print("\n🤖 Ответ модели:\n")
        print(response_text)

        # ✅ Вывод статистики по токенам, если поддерживается API
        usage = getattr(completion, "usage", None)
        if usage:
            tokens_used = usage.total_tokens
            print("\n📊 Статистика по токенам:")
            print(f"   Входные токены:  {usage.prompt_tokens}")
            print(f"   Выходные токены: {usage.completion_tokens}")
            print(f"   Всего токенов:   {tokens_used}")

            # Расчёт стоимости
            PRICE_PER_1K = 2.25  # руб/1000 токенов для GigaChat-2-Pro
            cost_rub = (tokens_used / 1000) * PRICE_PER_1K
            print(f"💰 Примерная стоимость запроса: {cost_rub:.4f} ₽")
        else:
            print("\nℹ️ Токен-статистика недоступна для этой модели/запроса")

    except Exception as e:
        print(f"⚠️ Ошибка: {e}")
