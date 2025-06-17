from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from .models import Article, ArticleContent, ReferenceLink, AnalyzedSegment


@login_required
def article_submission_page(request):
    return render(request, 'papers/submit_article.html', {'user_id': request.user.id})


# @login_required
# def article_detail_page(request, pk):
#     article = get_object_or_404(Article, pk=pk, user=request.user)
#     contents = ArticleContent.objects.filter(article=article).order_by('source_api_name', 'format_type')
#     references_made_qs = ReferenceLink.objects.filter(source_article=article).select_related('resolved_article').order_by('order') # QuerySet
#     analyzed_segments = AnalyzedSegment.objects.filter(article=article).select_related('user').prefetch_related('cited_references').order_by('created_at')
#     # --- НОВОЕ: Подготовка данных для JavaScript ---
#     # Преобразуем QuerySet references_made_qs в список списков/кортежей
#     references_for_js = list(references_made_qs.values_list(
#         'pk',
#         'target_article_doi',
#         'raw_reference_text',
#         'manual_data_json'
#         # Если manual_data_json может быть None и вы хотите передавать его как есть:
#         # Если же manual_data_json всегда словарь или должен быть им, то можно оставить так.
#         # Если manual_data_json может содержать несериализуемые для values_list элементы, лучше передавать queryset и обрабатывать в шаблоне или сериализаторе
#     ))
#     # --------------------------------------------
#     context = {
#         'article': article,
#         'contents': contents,
#         'references_made': references_made_qs, # Оставляем QuerySet для Django-циклов в шаблоне
#         'references_for_js': references_for_js, # Передаем подготовленный список для json_script
#         'analyzed_segments': analyzed_segments,
#         'user_id': request.user.id,
#         'ref_status_choices': ReferenceLink.StatusChoices.choices,
#     }
#     return render(request, 'papers/article_detail.html', context)


@login_required
def article_detail_page(request, pk):
    article = get_object_or_404(Article, pk=pk, user=request.user)
    contents = ArticleContent.objects.filter(article=article).order_by('source_api_name', 'format_type')
    references_made = ReferenceLink.objects.filter(source_article=article).select_related('resolved_article').order_by('id')
    analyzed_segments = AnalyzedSegment.objects.filter(article=article).select_related('user').prefetch_related('cited_references__resolved_article').order_by('created_at')
    context = {
        'article': article,
        'contents': contents,
        'references_made': references_made,
        'analyzed_segments': analyzed_segments,
        'user_id': request.user.id,
        'ref_status_choices': ReferenceLink.StatusChoices.choices,
    }
    return render(request, 'papers/article_detail.html', context)


@login_required
def article_list_page(request):
    # Получаем ТОЛЬКО основные статьи пользователя (где is_user_initiated = True)
    primary_articles_qs = Article.objects.filter(
        user=request.user,
        is_user_initiated=True
    ).prefetch_related(
        'authors',
        'references_made',
        'references_made__resolved_article',
        'references_made__resolved_article__authors'
    ).order_by('-updated_at')

    articles_data_for_template = []
    for article in primary_articles_qs:
        linked_articles_list = []
        seen_linked_article_ids = set()
        for ref_link in article.references_made.all(): # references_made уже предзагружены
            if ref_link.resolved_article and ref_link.resolved_article.id not in seen_linked_article_ids:
                # Связанная статья может быть is_user_initiated=False или даже True, если пользователь добавил ее отдельно
                linked_articles_list.append(ref_link.resolved_article)
                seen_linked_article_ids.add(ref_link.resolved_article.id)

        articles_data_for_template.append({
            'primary_article': article,
            'linked_articles': linked_articles_list
        })

    return render(request, 'papers/article_list.html', {'articles_data': articles_data_for_template})
