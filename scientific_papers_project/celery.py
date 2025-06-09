# scientific_papers_project/celery.py
import os
from celery import Celery

# Устанавливаем переменную окружения для настроек Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'scientific_papers_project.settings')

app = Celery('scientific_papers_project')

# Используем конфигурацию из настроек Django.
# Пространство имен 'CELERY' означает, что все настройки Celery в settings.py
# должны начинаться с 'CELERY_', например, CELERY_BROKER_URL.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Автоматически обнаруживаем и загружаем задачи из всех зарегистрированных приложений Django.
app.autodiscover_tasks()

@app.task(bind=True, ignore_result=True)
def debug_task(self):
    print(f'Request: {self.request!r}')