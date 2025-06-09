# scientific_papers_project/asgi.py
import os
from django.core.asgi import get_asgi_application
from channels.routing import ProtocolTypeRouter, URLRouter
from channels.auth import AuthMiddlewareStack # Для аутентификации в WebSockets
import papers.routing # Мы создадим этот файл на следующем шаге

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'scientific_papers_project.settings')

# Приложение Django для обычных HTTP запросов
django_asgi_app = get_asgi_application()

application = ProtocolTypeRouter({
    "http": django_asgi_app, # Обычные HTTP запросы обрабатываются Django
    "websocket": AuthMiddlewareStack( # WebSocket запросы с аутентификацией Django
        URLRouter(
            papers.routing.websocket_urlpatterns # Маршруты для WebSocket
        )
    ),
})