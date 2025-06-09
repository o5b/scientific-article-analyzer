# papers/routing.py
from django.urls import re_path
from . import consumers # Мы создадим consumers.py на следующем шаге

websocket_urlpatterns = [
    re_path(r'ws/notifications/$', consumers.NotificationConsumer.as_asgi()),
    # r'ws/notifications/(?P<user_id>\d+)/$' # Если бы мы передавали user_id в URL
]