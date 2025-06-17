from django.urls import path
from . import views_site # Создадим этот файл ниже

urlpatterns = [
    path('', views_site.article_submission_page, name='submit_article_root'), # Корневой URL
    path('submit/', views_site.article_submission_page, name='submit_article'),
    path('articles/', views_site.article_list_page, name='article_list'),
    path('article/<int:pk>/', views_site.article_detail_page, name='article_detail'),
]
