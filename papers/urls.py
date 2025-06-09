from django.urls import path, include
from rest_framework.routers import DefaultRouter
from .views import (
    AuthorViewSet, ArticleViewSet, ArticleContentViewSet, ReferenceLinkViewSet, StartArticleProcessingView, LoadReferencedArticleAPIView, FindDoiForReferenceAPIView, FindAllReferenceDoisAPIView, LoadAllLinkedReferencesAPIView, ReprocessArticleAPIView, AnalyzedSegmentViewSet, RunLLMAnalysisForSegmentAPIView
)


# Создаем router и регистрируем наши viewsets
router = DefaultRouter()
router.register(r'authors', AuthorViewSet, basename='author')
router.register(r'articles', ArticleViewSet, basename='article')
router.register(r'articlecontents', ArticleContentViewSet, basename='articlecontent')
router.register(r'referencelinks', ReferenceLinkViewSet, basename='referencelink')
router.register(r'analyzed-segments', AnalyzedSegmentViewSet, basename='analyzedsegment')


# API URLs теперь автоматически определяются роутером.
urlpatterns = [
    # path('fetch-doi/', StartDOIFetchView.as_view(), name='fetch_doi'),
    # path('fetch-doi/', StartDOIFetchView.as_view(), name='fetch_doi'), # Старый URL, если еще используется
    # path('process-article/', StartDOIFetchView.as_view(), name='process_article'), # Обновленный URL для диспетчера
    # path('test-notifications/', notifications_test_page, name='test_notifications'),

    path('process-article/', StartArticleProcessingView.as_view(), name='process_article'),
    path('reference-links/<int:pk>/load-article/', LoadReferencedArticleAPIView.as_view(), name='load_referenced_article'),
    path('reference-links/<int:pk>/find-doi/', FindDoiForReferenceAPIView.as_view(), name='find_doi_for_reference'),
    path('articles/<int:pk>/find-all-reference-dois/', FindAllReferenceDoisAPIView.as_view(), name='find_all_reference_dois'),
    path('articles/<int:pk>/load-all-linked-references/', LoadAllLinkedReferencesAPIView.as_view(), name='load_all_linked_references'),
    path('articles/<int:pk>/reprocess/', ReprocessArticleAPIView.as_view(), name='reprocess_article'),
    path('analyzed-segments/<int:pk>/run-llm-analysis/', RunLLMAnalysisForSegmentAPIView.as_view(), name='run_llm_analysis_for_segment'),
    path('', include(router.urls)),
]
