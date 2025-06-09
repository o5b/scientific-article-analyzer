import json
from channels.generic.websocket import AsyncWebsocketConsumer
from channels.db import database_sync_to_async # Для доступа к моделям Django асинхронно

class NotificationConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.user = self.scope["user"]

        if not self.user.is_authenticated:
            await self.close() # Закрываем соединение для неаутентифицированных пользователей
        else:
            # Создаем имя группы, специфичное для пользователя
            self.group_name = f"user_{self.user.id}_notifications"

            # Присоединяемся к группе
            await self.channel_layer.group_add(
                self.group_name,
                self.channel_name
            )
            await self.accept()
            await self.send(text_data=json.dumps({
                'type': 'connection_established',
                'message': f'WebSocket-соединение установлено. Вы слушаете группу {self.group_name}.'
            }))

    async def disconnect(self, close_code):
        if hasattr(self, 'group_name') and self.user.is_authenticated:
            # Отсоединяемся от группы
            await self.channel_layer.group_discard(
                self.group_name,
                self.channel_name
            )

    # Этот метод будет вызываться, когда сообщение приходит от клиента (если нужно)
    async def receive(self, text_data):
        # Мы не ожидаем сообщений от клиента в этом примере,
        # но можно добавить логику, если потребуется
        pass

    # Этот метод будет вызываться, когда приходит сообщение из группы (от Celery task)
    async def send_notification(self, event):
        message_payload = event.get("payload", {}) # Ожидаем, что payload содержит message, status и т.д.

        # Отправляем сообщение клиенту через WebSocket
        await self.send(text_data=json.dumps({
            'type': 'task_notification', # Тип сообщения для клиента
            'payload': message_payload
        }))