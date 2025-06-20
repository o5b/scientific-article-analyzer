import re
import os
import json
import requests
import time # Для NCBI E-utils rate limiting
import xml.etree.ElementTree as ET # Для парсинга XML
from openai import OpenAI

from celery import shared_task
from django.utils import timezone
from django.core.files.base import ContentFile
from django.db import transaction, utils as db_utils # utils для OperationalError
# from asgiref.sync import async_to_sync
# from channels.layers import get_channel_layer
from django.contrib.auth.models import User
from django.conf import settings # Для доступа к API_SOURCE_...

from .models import Article, Author, ArticleContent, ArticleAuthorOrder, ReferenceLink, AnalyzedSegment
from .helpers import (
    send_user_notification,
    parse_crossref_authors,
    parse_europepmc_authors,
    parse_s2_authors,
    parse_arxiv_authors,
    parse_pubmed_authors,
    parse_rxiv_authors,
    # extract_text_from_jats_xml,
    # extract_text_from_bioc_json,
    parse_references_from_jats,
    reconstruct_abstract_from_inverted_index,
    parse_openalex_authors,
    extract_structured_text_from_jats,
    # extract_structured_text_from_bioc,
    # sanitize_for_json_serialization,
    # get_pmc_pdf,
    download_pdf,
)


# --- Константы API из настроек ---
# Эти константы теперь лучше брать из settings.API_SOURCE_NAMES внутри каждой задачи
# для большей гибкости, но для удобства можно определить их и здесь, если они не меняются.
# Однако, рекомендуется обращаться к settings.API_SOURCE_NAMES['KEY'] внутри задач.
APP_EMAIL = getattr(settings, 'APP_EMAIL', 'transposons.chat@gmail.com') # РЕАЛЬНЫЙ EMAIL в settings.py или здесь
NCBI_API_KEY = getattr(settings, 'NCBI_API_KEY', None) # Из настроек, если есть
NCBI_TOOL_NAME = 'ScientificPapersApp'
NCBI_ADMIN_EMAIL = APP_EMAIL
FIND_DOI_TASK_SOURCE_NAME = 'FindDOITask'
PIPELINE_DISPATCHER_SOURCE_NAME = 'PipelineDispatcher'

# Пространства имен для arXiv Atom XML
ARXIV_NS = {'atom': 'http://www.w3.org/2005/Atom', 'arxiv': 'http://arxiv.org/schemas/atom'}

USER_AGENT_LIST = [
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36', # Linux, Chrome
]


# --- Диспетчерская задача ---
@shared_task(bind=True)
def process_article_pipeline_task(
    self,
    identifier_value: str,
    identifier_type: str,
    user_id: int,
    originating_reference_link_id: int = None):

    pipeline_task_id = self.request.id # ID самой диспетчерской задачи
    pipeline_display_name = f"{identifier_type.upper()}:{identifier_value}"
    if originating_reference_link_id:
        pipeline_display_name += f" (для ref_link: {originating_reference_link_id})"

    send_user_notification(
        user_id, pipeline_task_id, pipeline_display_name, 'PIPELINE_START',
        f'Запуск конвейера обработки для: {pipeline_display_name}',
        progress_percent=0, source_api=PIPELINE_DISPATCHER_SOURCE_NAME,
        originating_reference_link_id=originating_reference_link_id
    )

    article_owner = None
    if user_id:
        try:
            article_owner = User.objects.get(id=user_id)
        except User.DoesNotExist:
            send_user_notification(user_id, pipeline_task_id, pipeline_display_name, 'PIPELINE_FAILURE', f'Пользователь ID {user_id} не найден.', source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)
            return {'status': 'error', 'message': f'User ID {user_id} not found.'}

    if not article_owner:
        send_user_notification(user_id, pipeline_task_id, pipeline_display_name, 'PIPELINE_FAILURE', 'Пользователь не указан для конвейера.', source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'error', 'message': 'User not specified for pipeline.'}

    # --- Шаг 1: Начальное создание/получение статьи и определение основного DOI ---
    article = None
    created_article_in_pipeline = False # Флаг, была ли статья создана именно этим запуском диспетчера

    # Готовим параметры для поиска/создания статьи
    initial_article_lookup_kwargs = {}
    effective_doi_for_subtasks = None # DOI, который будет использоваться для большинства подзадач
    identifier_type_upper = identifier_type.upper()

    if identifier_type_upper == 'DOI':
        effective_doi_for_subtasks = identifier_value.lower()
        initial_article_lookup_kwargs['doi'] = effective_doi_for_subtasks
    elif identifier_type_upper == 'PMID':
        initial_article_lookup_kwargs['pubmed_id'] = identifier_value
    elif identifier_type_upper == 'ARXIV':
        initial_article_lookup_kwargs['arxiv_id'] = identifier_value.replace('arXiv:', '').split('v')[0].strip()
    # Добавить другие типы идентификаторов при необходимости

    if not initial_article_lookup_kwargs:
        send_user_notification(user_id, pipeline_task_id, pipeline_display_name, 'PIPELINE_FAILURE', f'Неподдерживаемый тип идентификатора: {identifier_type}', source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'error', 'message': f'Unsupported identifier type: {identifier_type}'}

    try:
        with transaction.atomic():
            creation_defaults = {
                'user': article_owner,
                'title': f"Статья в обработке: {pipeline_display_name}",
                'is_user_initiated': originating_reference_link_id is None # True если это прямой вызов, False если для ссылки
            }

            article, created_article_in_pipeline = Article.objects.select_for_update().get_or_create(
                **initial_article_lookup_kwargs,
                defaults=creation_defaults
            )

            # Если статья уже существовала, но была не "основной", а пользователь сейчас добавляет ее напрямую
            if not created_article_in_pipeline and article.user == article_owner and \
                not article.is_user_initiated and originating_reference_link_id is None:
                    article.is_user_initiated = True
                    article.save(update_fields=['is_user_initiated', 'updated_at'])

        if created_article_in_pipeline:
            send_user_notification(user_id, pipeline_task_id, pipeline_display_name, 'PIPELINE_PROGRESS', f'Предварительная запись для статьи ID {article.id} создана (user_initiated={article.is_user_initiated}).', source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)
        else:
            send_user_notification(user_id, pipeline_task_id, pipeline_display_name, 'PIPELINE_PROGRESS', f'Найдена существующая статья ID {article.id} (user_initiated={article.is_user_initiated}). Обновление...', source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)

        # Обновляем effective_doi_for_subtasks, если он был найден/установлен в статье
        if not effective_doi_for_subtasks and article.doi:
            effective_doi_for_subtasks = article.doi

    except Exception as e:
        send_user_notification(user_id, pipeline_task_id, pipeline_display_name, 'PIPELINE_FAILURE', f'Ошибка при создании/получении статьи: {str(e)}', source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'error', 'message': f'Error accessing article entry: {str(e)}'}

    article_id_for_subtasks = article.id # Стабильный ID статьи для всех подзадач

    send_user_notification(user_id, pipeline_task_id, pipeline_display_name, 'PIPELINE_PROGRESS', 'Запуск получения данных из источников...', progress_percent=5, source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)

    # --- Шаг 2: Вызов задачи для CrossRef ---
    if effective_doi_for_subtasks:
        try:
            # Обработка ссылок включается только если это не обработка уже существующей ссылки (т.е. "корневой" вызов)
            should_process_refs_in_crossref = (originating_reference_link_id is None)
            # should_process_refs_in_crossref = True if originating_reference_link_id is None else False

            crossref_task_sig = fetch_data_from_crossref_task.s(
                doi=effective_doi_for_subtasks,
                user_id=user_id,
                process_references=should_process_refs_in_crossref,
                originating_reference_link_id=originating_reference_link_id,
                article_id_to_update=article_id_for_subtasks,
            )
            crossref_task_id_val = crossref_task_sig.apply_async().id
            send_user_notification(user_id, pipeline_task_id, effective_doi_for_subtasks, 'SUBTASK_STARTED', f'Задача CrossRef запущена (ID: {crossref_task_id_val}).', progress_percent=10, source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)
        except Exception as e_crossref:
            send_user_notification(user_id, pipeline_task_id, effective_doi_for_subtasks, 'PIPELINE_ERROR', f'Не удалось запустить задачу CrossRef: {str(e_crossref)}', source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)
    else:
        send_user_notification(user_id, pipeline_task_id, pipeline_display_name, 'PIPELINE_INFO', 'DOI не определен для вызова CrossRef, пропуск.', source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)

    # --- Шаг 3: Вызов задачи для Semantic Scholar ---
    s2_identifier_val = None
    s2_id_type_val = None
    if article.doi:
        s2_identifier_val = article.doi; s2_id_type_val = 'DOI'
    elif article.arxiv_id:
        s2_identifier_val = article.arxiv_id; s2_id_type_val = 'ARXIV'
    elif identifier_type_upper == 'ARXIV':
        s2_identifier_val = identifier_value; s2_id_type_val = 'ARXIV'

    if s2_identifier_val and s2_id_type_val:
        try:
            s2_task_sig = fetch_data_from_s2_task.s(
                identifier_value=s2_identifier_val,
                identifier_type=s2_id_type_val,
                user_id=user_id,
                originating_reference_link_id=originating_reference_link_id,
                article_id_to_update=article_id_for_subtasks,
            )
            s2_task_id_val = s2_task_sig.apply_async().id
            send_user_notification(user_id, pipeline_task_id, f"{s2_id_type_val}:{s2_identifier_val}", 'SUBTASK_STARTED', f'Задача Semantic Scholar запущена (ID: {s2_task_id_val}).', progress_percent=30, source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)
        except Exception as e_s2:
            send_user_notification(user_id, pipeline_task_id, pipeline_display_name, 'PIPELINE_ERROR', f'Не удалось запустить задачу Semantic Scholar: {str(e_s2)}', source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)
    else:
        send_user_notification(user_id, pipeline_task_id, pipeline_display_name, 'PIPELINE_INFO', 'Нет подходящего идентификатора для Semantic Scholar.', source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)

    # --- Шаг 4: Задача для arXiv ---
    # Вызываем, если исходный идентификатор был ARXIV, или если у статьи есть arxiv_id
    arxiv_id_for_task = None
    if identifier_type_upper == 'ARXIV':
        arxiv_id_for_task = identifier_value # Используем исходный, он может быть с версией
    elif article.arxiv_id:
        arxiv_id_for_task = article.arxiv_id # Используем тот, что в статье

    if arxiv_id_for_task:
        try:
            # Передаем article.id как article_id_to_update, чтобы задача знала, какую запись обновлять
            arxiv_task_sig = fetch_data_from_arxiv_task.s(
                arxiv_id_value=arxiv_id_for_task,
                user_id=user_id,
                originating_reference_link_id=originating_reference_link_id,
                article_id_to_update=article_id_for_subtasks, # ID существующей статьи
            )
            arxiv_task_id_val = arxiv_task_sig.apply_async().id
            send_user_notification(user_id, pipeline_task_id, f"ARXIV:{arxiv_id_for_task}", 'SUBTASK_STARTED', f'Задача arXiv запущена (ID: {arxiv_task_id_val}).', progress_percent=40, source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)
        except Exception as e_arxiv:
            send_user_notification(user_id, pipeline_task_id, f"ARXIV:{arxiv_id_for_task}", 'PIPELINE_ERROR', f'Не удалось запустить задачу arXiv: {str(e_arxiv)}', source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)
    else:
        send_user_notification(user_id, pipeline_task_id, pipeline_display_name, 'PIPELINE_INFO', 'Нет arXiv ID для запуска задачи arXiv.', source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)

    # --- Шаг 5: Вызов задачи для Unpaywall (если есть DOI у статьи) ---
    # Используем DOI, который у нас есть для статьи (мог быть обновлен предыдущими задачами, но мы не ждем их)
    # `effective_doi_for_subtasks` - это DOI, с которым мы вошли в конвейер (если type=DOI)
    # `article.doi` - это DOI, который есть у статьи в БД на данный момент (после get_or_create)
    final_doi_for_unpaywall_lookup = article.doi if article.doi else effective_doi_for_subtasks

    if final_doi_for_unpaywall_lookup:
        try:
            unpaywall_task_sig = fetch_data_from_unpaywall_task.s(
                doi=final_doi_for_unpaywall_lookup,
                article_id=article_id_for_subtasks,
                user_id=user_id
            )
            unpaywall_task_id_val = unpaywall_task_sig.apply_async().id
            send_user_notification(user_id, pipeline_task_id, final_doi_for_unpaywall_lookup, 'SUBTASK_STARTED', f'Задача Unpaywall запущена (ID: {unpaywall_task_id_val}).', progress_percent=50, source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)
        except Exception as e_unpaywall:
            send_user_notification(user_id, pipeline_task_id, final_doi_for_unpaywall_lookup, 'PIPELINE_ERROR', f'Не удалось запустить задачу Unpaywall: {str(e_unpaywall)}', source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)
    else:
        send_user_notification(user_id, pipeline_task_id, pipeline_display_name, 'PIPELINE_INFO', 'DOI не определен для Unpaywall, пропуск.', source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)

    # --- Шаг 6: Задача для OpenAlex ---
    # Вызываем, используя лучший доступный идентификатор для OpenAlex (DOI, PMID)
    oa_identifier_val = None
    oa_id_type_val = None

    # article должен быть уже определен здесь после get_or_create
    if article.doi:
        oa_identifier_val = article.doi
        oa_id_type_val = 'DOI'
    elif article.pubmed_id:
        oa_identifier_val = article.pubmed_id
        oa_id_type_val = 'PMID'
    elif article.arxiv_id: # OpenAlex может работать с arXiv ID, но обычно через DOI/PMID надежнее для /works/ endpoint
        pass # Пока не используем arXiv ID для прямого запроса к OpenAlex /works/{ID}, если нет DOI/PMID

    # Если исходный идентификатор был одним из тех, что OpenAlex принимает напрямую
    elif identifier_type.upper() in ['DOI', 'PMID']:
        oa_identifier_val = identifier_value
        oa_id_type_val = identifier_type

    if oa_identifier_val and oa_id_type_val:
        try:
            openalex_task_sig = fetch_data_from_openalex_task.s(
                identifier_value=oa_identifier_val,
                identifier_type=oa_id_type_val,
                user_id=user_id,
                originating_reference_link_id=originating_reference_link_id,
                article_id_to_update=article_id_for_subtasks
            )
            openalex_task_id_val = openalex_task_sig.apply_async().id
            send_user_notification(user_id, pipeline_task_id, f"{oa_id_type_val}:{oa_identifier_val}", 'SUBTASK_STARTED', f'Задача OpenAlex запущена (ID: {openalex_task_id_val}).', progress_percent=85, source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)
        except Exception as e_oa:
            send_user_notification(user_id, pipeline_task_id, pipeline_display_name, 'PIPELINE_ERROR', f'Не удалось запустить задачу OpenAlex: {str(e_oa)}', source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)
    else:
        send_user_notification(user_id, pipeline_task_id, pipeline_display_name, 'PIPELINE_INFO', 'Нет подходящего идентификатора для OpenAlex.', source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)

    # --- Шаг 7: Задача для PubMed ---
    # Вызываем, если есть PMID у статьи, или если исходный идентификатор был PMID, или если есть DOI (для поиска PMID)
    pubmed_identifier_val = None
    pubmed_id_type_val = None

    if article.pubmed_id:
        pubmed_identifier_val = article.pubmed_id
        pubmed_id_type_val = 'PMID'
    elif article.doi: # Если есть DOI, PubMed задача сможет найти PMID
        pubmed_identifier_val = article.doi
        pubmed_id_type_val = 'DOI'
    elif identifier_type.upper() == 'PMID': # Если исходный был PMID
        pubmed_identifier_val = identifier_value
        pubmed_id_type_val = 'PMID'

    if pubmed_identifier_val and pubmed_id_type_val:
        try:
            pubmed_task_sig = fetch_data_from_pubmed_task.s(
                identifier_value=pubmed_identifier_val,
                identifier_type=pubmed_id_type_val,
                user_id=user_id,
                originating_reference_link_id=originating_reference_link_id,
                article_id_to_update=article_id_for_subtasks
            )
            pubmed_task_id_val = pubmed_task_sig.apply_async().id
            send_user_notification(user_id, pipeline_task_id, f"{pubmed_id_type_val}:{pubmed_identifier_val}", 'SUBTASK_STARTED', f'Задача PubMed запущена (ID: {pubmed_task_id_val}).', progress_percent=60, source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)
        except Exception as e_pubmed:
            send_user_notification(user_id, pipeline_task_id, pipeline_display_name, 'PIPELINE_ERROR', f'Не удалось запустить задачу PubMed: {str(e_pubmed)}', source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)
    else:
        send_user_notification(user_id, pipeline_task_id, pipeline_display_name, 'PIPELINE_INFO', 'Нет подходящего идентификатора для PubMed.', source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)

    # --- Шаг 8: Вызов задачи для Europe PMC ---
    # Используем идентификаторы из объекта 'article', который мог быть обновлен CrossRef (если бы он выполнялся синхронно)
    # При асинхронном запуске, лучше передавать идентификаторы явно или использовать Celery chains.
    # Пока что, для параллельного запуска, используем то, что есть в 'article' на момент этого шага.
    # article.refresh_from_db() # Ненадежно в полностью асинхронном потоке без ожидания

    epmc_identifier_val = None
    epmc_id_type_val = None
    # Пытаемся получить актуальные данные из объекта article, который у нас есть
    if article.doi: # Предпочитаем DOI для EPMC, если есть
        epmc_identifier_val = article.doi
        epmc_id_type_val = 'DOI'
    elif article.pubmed_id:
        epmc_identifier_val = article.pubmed_id
        epmc_id_type_val = 'PMID'
    elif identifier_type_upper == 'PMID': # Если исходный был PMID и DOI не появился
         epmc_identifier_val = identifier_value
         epmc_id_type_val = 'PMID'

    if epmc_identifier_val and epmc_id_type_val:
        try:
            epmc_task_sig = fetch_data_from_europepmc_task.s(
                identifier_value=epmc_identifier_val,
                identifier_type=epmc_id_type_val,
                user_id=user_id,
                originating_reference_link_id=originating_reference_link_id,
                article_id_to_update=article_id_for_subtasks,
            )
            epmc_task_id_val = epmc_task_sig.apply_async().id
            send_user_notification(user_id, pipeline_task_id, f"{epmc_id_type_val}:{epmc_identifier_val}", 'SUBTASK_STARTED', f'Задача Europe PMC запущена (ID: {epmc_task_id_val}).', progress_percent=20, source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)
        except Exception as e_epmc:
             send_user_notification(user_id, pipeline_task_id, pipeline_display_name, 'PIPELINE_ERROR', f'Не удалось запустить задачу Europe PMC: {str(e_epmc)}', source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)
    else:
        send_user_notification(user_id, pipeline_task_id, pipeline_display_name, 'PIPELINE_INFO', 'Нет подходящего идентификатора для Europe PMC.', source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)

    # --- Шаг 9: Вызов задачи для bioRxiv/medRxiv (Rxiv) ---
    rxiv_doi_to_check = article.doi if article.doi else effective_doi_for_subtasks # Используем лучший доступный DOI
    if rxiv_doi_to_check and rxiv_doi_to_check.startswith("10.1101/"): # Характерный префикс для Rxiv
        try:
            rxiv_task_sig = fetch_data_from_rxiv_task.s(
                doi=rxiv_doi_to_check,
                user_id=user_id,
                originating_reference_link_id=originating_reference_link_id,
                article_id_to_update=article_id_for_subtasks
            )
            rxiv_task_id_val = rxiv_task_sig.apply_async().id
            send_user_notification(user_id, pipeline_task_id, rxiv_doi_to_check, 'SUBTASK_STARTED', f'Задача Rxiv (bio/medRxiv) запущена (ID: {rxiv_task_id_val}).', progress_percent=70, source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)
        except Exception as e_rxiv:
            send_user_notification(user_id, pipeline_task_id, rxiv_doi_to_check, 'PIPELINE_ERROR', f'Не удалось запустить задачу Rxiv: {str(e_rxiv)}', source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)
    else:
        send_user_notification(user_id, pipeline_task_id, pipeline_display_name, 'PIPELINE_INFO', 'DOI не похож на bioRxiv/medRxiv или отсутствует, пропуск задачи Rxiv.', source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)

    # Финальное уведомление от диспетчера (progress_percent=100)
    send_user_notification(user_id, pipeline_task_id, pipeline_display_name, 'PIPELINE_COMPLETE', 'Конвейер обработки завершил постановку всех задач.', progress_percent=100, source_api=PIPELINE_DISPATCHER_SOURCE_NAME, originating_reference_link_id=originating_reference_link_id)
    return {'status': 'success', 'message': 'Конвейер задач запущен.', 'pipeline_task_id': pipeline_task_id, 'article_id': article_id_for_subtasks}


# Запрос к CrossRef для поиска DOI для цитируемой ссылки
@shared_task(bind=True, max_retries=2, default_retry_delay=180)
def find_doi_for_reference_task(self, reference_link_id: int, user_id: int):
    task_id = self.request.id
    display_identifier = f"RefLinkID:{reference_link_id}"

    send_user_notification(user_id, task_id, display_identifier, 'PENDING', 'Начинаем поиск DOI для ссылки...', source_api=FIND_DOI_TASK_SOURCE_NAME, originating_reference_link_id=reference_link_id)

    try:
        ref_link = ReferenceLink.objects.select_related('source_article__user').get(id=reference_link_id)
        ref_link.status = ReferenceLink.StatusChoices.DOI_LOOKUP_IN_PROGRESS
        ref_link.save(update_fields=['status', 'updated_at'])
    except ReferenceLink.DoesNotExist:
        send_user_notification(user_id, task_id, display_identifier, 'FAILURE', 'Ссылка не найдена в базе данных.', source_api=FIND_DOI_TASK_SOURCE_NAME, originating_reference_link_id=reference_link_id)
        return {'status': 'error', 'message': 'ReferenceLink not found.'}

    if ref_link.source_article.user_id != user_id:
        send_user_notification(user_id, task_id, display_identifier, 'FAILURE', 'Нет прав для выполнения этого действия.', source_api=FIND_DOI_TASK_SOURCE_NAME, originating_reference_link_id=reference_link_id)
        return {'status': 'error', 'message': 'Permission denied.'}

    if ref_link.target_article_doi:
        send_user_notification(user_id, task_id, display_identifier, 'INFO', f'DOI ({ref_link.target_article_doi}) уже указан для этой ссылки.', source_api=FIND_DOI_TASK_SOURCE_NAME, originating_reference_link_id=reference_link_id)
        return {'status': 'info', 'message': 'DOI already exists for this reference link.'}

    # Формируем поисковый запрос для CrossRef
    # Используем данные из manual_data_json (если есть название, авторы, год) или raw_reference_text
    search_query_parts = []
    if ref_link.manual_data_json:
        if ref_link.manual_data_json.get('title'):
            search_query_parts.append(str(ref_link.manual_data_json['title']))
        # Можно добавить авторов, если они есть в structured виде, например, первый автор
        # if ref_link.manual_data_json.get('authors') and isinstance(ref_link.manual_data_json['authors'], list):
        #    search_query_parts.append(str(ref_link.manual_data_json['authors'][0].get('name')))
        if ref_link.manual_data_json.get('year'):
            search_query_parts.append(str(ref_link.manual_data_json['year']))

    if not search_query_parts and ref_link.raw_reference_text: # Если нет структурированных данных, берем сырой текст
        search_query_parts.append(ref_link.raw_reference_text[:300]) # Ограничим длину запроса

    if not search_query_parts:
        send_user_notification(user_id, task_id, display_identifier, 'FAILURE', 'Недостаточно данных для поиска DOI.', source_api=FIND_DOI_TASK_SOURCE_NAME, originating_reference_link_id=reference_link_id)
        ref_link.status = ReferenceLink.StatusChoices.ERROR_DOI_LOOKUP # Или новый статус "недостаточно данных"
        ref_link.save(update_fields=['status', 'updated_at'])
        return {'status': 'error', 'message': 'Not enough data to search for DOI.'}

    bibliographic_query = " ".join(search_query_parts)

    params = {
        'query.bibliographic': bibliographic_query,
        'rows': 1, # Нам нужен только самый релевантный результат
        'mailto': APP_EMAIL
    }
    api_url = "https://api.crossref.org/works"
    # headers = {'User-Agent': f'ScientificPapersApp/1.0 (mailto:{APP_EMAIL})'}
    headers = {'User-Agent': USER_AGENT_LIST[0]}

    try:
        send_user_notification(user_id, task_id, display_identifier, 'PROGRESS', f'Запрос к CrossRef для поиска DOI: "{bibliographic_query[:50]}..."', progress_percent=30, source_api=FIND_DOI_TASK_SOURCE_NAME, originating_reference_link_id=reference_link_id)
        response = requests.get(api_url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as exc:
        send_user_notification(user_id, task_id, display_identifier, 'RETRYING', f'Ошибка сети/API CrossRef при поиске DOI: {str(exc)}. Повтор...', source_api=FIND_DOI_TASK_SOURCE_NAME, originating_reference_link_id=reference_link_id)
        raise self.retry(exc=exc)

    if data and data.get('message') and data['message'].get('items'):
        found_item = data['message']['items'][0]
        found_doi = found_item.get('DOI')
        score = found_item.get('score', 0) # CrossRef возвращает score релевантности

        if found_doi:
            # Можно добавить проверку score, чтобы отсеять совсем нерелевантные результаты
            # Например, if score > некоторого порога (например 60-70)
            ref_link.target_article_doi = found_doi.lower()
            ref_link.status = ReferenceLink.StatusChoices.DOI_PROVIDED_NEEDS_LOOKUP
            # Можно сохранить и другую информацию, например, название найденной статьи, в manual_data_json для сверки
            if 'title' in found_item and isinstance(found_item['title'], list) and found_item['title']:
                ref_link.manual_data_json = ref_link.manual_data_json or {}
                ref_link.manual_data_json['found_title_by_doi_search'] = found_item['title'][0]
                ref_link.manual_data_json['found_doi_score'] = score

            ref_link.save(update_fields=['target_article_doi', 'status', 'manual_data_json', 'updated_at'])
            send_user_notification(user_id, task_id, display_identifier, 'SUCCESS', f'Найден DOI: {found_doi} (score: {score}) для ссылки.', progress_percent=100, source_api=FIND_DOI_TASK_SOURCE_NAME, originating_reference_link_id=reference_link_id)
            return {'status': 'success', 'message': f'DOI found: {found_doi}', 'found_doi': found_doi}
        else:
            ref_link.status = ReferenceLink.StatusChoices.ERROR_DOI_LOOKUP # Или новый статус "DOI не найден"
            ref_link.save(update_fields=['status', 'updated_at'])
            send_user_notification(user_id, task_id, display_identifier, 'FAILURE', 'DOI не найден в ответе CrossRef.', progress_percent=100, source_api=FIND_DOI_TASK_SOURCE_NAME, originating_reference_link_id=reference_link_id)
            return {'status': 'not_found', 'message': 'DOI not found in CrossRef response.'}
    else:
        ref_link.status = ReferenceLink.StatusChoices.ERROR_DOI_LOOKUP # Или новый статус "DOI не найден"
        ref_link.save(update_fields=['status', 'updated_at'])
        send_user_notification(user_id, task_id, display_identifier, 'FAILURE', 'DOI не найден (пустой ответ от CrossRef).', progress_percent=100, source_api=FIND_DOI_TASK_SOURCE_NAME, originating_reference_link_id=reference_link_id)
        return {'status': 'not_found', 'message': 'DOI not found (empty response from CrossRef).'}


@shared_task(bind=True, max_retries=3, default_retry_delay=60)
def fetch_data_from_crossref_task(
    self,
    doi: str,
    user_id: int = None,
    process_references: bool = False,
    originating_reference_link_id: int = None,
    article_id_to_update: int = None): # Параметр для обновления существующей статьи

    task_id = self.request.id
    current_api_name = settings.API_SOURCE_NAMES['CROSSREF']
    query_display_name = f"DOI:{doi}"

    send_user_notification(
        user_id, task_id, query_display_name, 'PENDING',
        f'Обработка {current_api_name} для {query_display_name}' + (' (включая ссылки)' if process_references else ''),
        progress_percent=0, source_api=current_api_name,
        originating_reference_link_id=originating_reference_link_id
    )

    if not doi:
        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', 'DOI не указан.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'error', 'message': 'DOI не указан.', 'doi': query_display_name}

    article_owner = None
    if user_id:
        try:
            article_owner = User.objects.get(id=user_id)
        except User.DoesNotExist:
            send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Пользователь ID {user_id} не найден.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            return {'status': 'error', 'message': f'User ID {user_id} not found.', 'doi': query_display_name}

    # headers = {'User-Agent': f'ScientificPapersApp/1.0 (mailto:{APP_EMAIL})'}
    headers = {'User-Agent': USER_AGENT_LIST[0]}
    # DOI в URL CrossRef обычно не чувствителен к регистру, но приведем к нижнему для единообразия
    safe_doi_url_part = requests.utils.quote(doi.lower()) # Кодируем DOI для URL
    api_url = f"https://api.crossref.org/works/{safe_doi_url_part}"

    try:
        send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'Запрос: {api_url}', progress_percent=10, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        response = requests.get(api_url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        api_data = data.get('message', {}) # основные данные от API
    except requests.exceptions.RequestException as exc:
        send_user_notification(user_id, task_id, query_display_name, 'RETRYING', f'Ошибка сети/API {current_api_name}: {str(exc)}. Повтор...', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        raise self.retry(exc=exc)
    except json.JSONDecodeError as json_exc:
        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Ошибка декодирования JSON от {current_api_name}: {str(json_exc)}', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'error', 'message': f'{current_api_name} JSON Decode Error: {str(json_exc)}'}

    if not api_data:
        send_user_notification(user_id, task_id, query_display_name, 'NOT_FOUND', f'{current_api_name} API: ответ не содержит "message" или статья не найдена.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'not_found', 'message': f'{current_api_name} API: no "message" in response or article not found.', 'doi': query_display_name, 'raw_response': data}

    send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'Данные {current_api_name} получены, обработка...', progress_percent=25, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

    try:
        with transaction.atomic():
            # Используем DOI из ответа API как канонический, если он есть, иначе исходный DOI
            api_doi_from_response = api_data.get('DOI', doi).lower()

            article = None
            created = False

            if article_id_to_update: # Если передан ID для обновления
                try:
                    article = Article.objects.select_for_update().get(id=article_id_to_update, user=article_owner)
                except Article.DoesNotExist:
                    send_user_notification(user_id, task_id, query_display_name, 'WARNING', f'Статья ID {article_id_to_update} (для обновления) не найдена/не принадлежит пользователю. Попытка найти по DOI.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

            if not article: # Если не нашли по article_id_to_update или он не был передан
                try:
                    article = Article.objects.select_for_update().get(doi=api_doi_from_response)
                except Article.DoesNotExist:
                    if not article_owner:
                        send_user_notification(user_id, task_id, api_doi_from_response, 'FAILURE', 'Пользователь не указан для создания новой статьи.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                        return {'status': 'error', 'message': 'User not specified for new article creation.'}

                    initial_title_list = api_data.get('title')
                    initial_title = ", ".join(initial_title_list).strip() if initial_title_list and isinstance(initial_title_list, list) and initial_title_list else f"Статья CrossRef: {api_doi_from_response}"

                    article = Article.objects.create(doi=api_doi_from_response, user=article_owner, title=initial_title)
                    created = True

            if not article: # Финальная проверка
                 send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Не удалось идентифицировать или создать запись для статьи {current_api_name}.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                 return {'status': 'error', 'message': f'Could not identify or create article entry for {current_api_name}.'}

            if not article.user and article_owner : article.user = article_owner

            # --- Применение логики приоритетов ---
            priority_list = getattr(settings, 'API_SOURCE_OVERALL_PRIORITY', [])
            try: current_api_priority = priority_list.index(current_api_name)
            except ValueError: current_api_priority = float('inf')
            article_primary_source_priority = float('inf')
            if article.primary_source_api:
                try: article_primary_source_priority = priority_list.index(article.primary_source_api)
                except ValueError: pass
            can_fully_overwrite = (created or not article.primary_source_api or current_api_priority <= article_primary_source_priority)

            if can_fully_overwrite:
                article.primary_source_api = current_api_name

            # Извлечение данных из api_data
            api_title_list = api_data.get('title')
            extracted_api_title = ", ".join(api_title_list).strip() if api_title_list and isinstance(api_title_list, list) and api_title_list else None

            api_abstract_raw = api_data.get('abstract')
            extracted_api_abstract = api_abstract_raw.replace('<i>', '').replace('</i>', '').strip() if api_abstract_raw else None

            api_pub_date_data = api_data.get('published-print') or api_data.get('published-online') or api_data.get('created')
            extracted_api_parsed_date = None
            if api_pub_date_data and api_pub_date_data.get('date-parts') and isinstance(api_pub_date_data['date-parts'], list) and api_pub_date_data['date-parts'][0]:
                date_parts = api_pub_date_data['date-parts'][0]
                if isinstance(date_parts, list) and date_parts:
                    try:
                        extracted_api_parsed_date = timezone.datetime(
                            year=int(date_parts[0]),
                            month=int(date_parts[1]) if len(date_parts) > 1 else 1,
                            day=int(date_parts[2]) if len(date_parts) > 2 else 1
                        ).date()
                    except (ValueError, TypeError, IndexError):
                        pass

            api_journal_list = api_data.get('container-title')
            extracted_api_journal_name = ", ".join(api_journal_list).strip() if api_journal_list and isinstance(api_journal_list, list) and api_journal_list else None

            extracted_api_authors = parse_crossref_authors(api_data.get('author'))

            # Обновление полей с учетом приоритетов
            if extracted_api_title and (can_fully_overwrite or not article.title):
                article.title = extracted_api_title
            if extracted_api_abstract and (can_fully_overwrite or not article.abstract):
                article.abstract = extracted_api_abstract
            if extracted_api_parsed_date and (can_fully_overwrite or not article.publication_date):
                article.publication_date = extracted_api_parsed_date
            if extracted_api_journal_name and (can_fully_overwrite or not article.journal_name):
                article.journal_name = extracted_api_journal_name

            if not article.doi:
                article.doi = api_doi_from_response # Убедимся, что DOI установлен

            # Обновление других идентификаторов, если CrossRef их вернул (пример для PMID)
            # CrossRef может возвращать ID других систем в поле 'alternative-id' или через другие поля
            if 'PMID' in api_data and api_data['PMID'] and not article.pubmed_id:
                 article.pubmed_id = str(api_data['PMID']) # Убедимся, что это строка

            if extracted_api_authors:
                if can_fully_overwrite or not article.authors.exists():
                    article.articleauthororder_set.all().delete()
                    for order, author_obj in enumerate(extracted_api_authors):
                        ArticleAuthorOrder.objects.create(article=article, author=author_obj, order=order)

            # if extracted_api_abstract and (not article.cleaned_text_for_llm or \
            #     len(extracted_api_abstract) > len(article.cleaned_text_for_llm or "") + 50 or \
            #     (can_fully_overwrite and article.primary_source_api == current_api_name)):
            #         article.cleaned_text_for_llm = extracted_api_abstract

            article.save()
            send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'Статья {current_api_name} сохранена.', progress_percent=50, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

            ArticleContent.objects.update_or_create(
                article=article, source_api_name=current_api_name, format_type='json_metadata',
                defaults={'content': json.dumps(api_data)}
            )

            if originating_reference_link_id and article:
                try:
                    ref_link = ReferenceLink.objects.get(id=originating_reference_link_id, source_article__user=article_owner)
                    ref_link.resolved_article = article
                    ref_link.status = ReferenceLink.StatusChoices.ARTICLE_LINKED
                    ref_link.save(update_fields=['resolved_article', 'status', 'updated_at'])
                    send_user_notification(user_id, task_id, query_display_name, 'INFO', f'Ссылка ID {ref_link.id} связана со статьей.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                except ReferenceLink.DoesNotExist: pass
                except Exception as e_ref: send_user_notification(user_id, task_id, query_display_name, 'ERROR', f'Ошибка обновления ref_link: {str(e_ref)}', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

            final_message = f'Статья {current_api_name} "{article.title[:30]}..." {"создана" if created else "обновлена"}.'
            send_user_notification(user_id, task_id, query_display_name, 'SUCCESS', final_message, progress_percent=100, article_id=article.id, created=created, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            return {'status': 'success', 'message': final_message, 'doi': api_doi_from_response, 'article_id': article.id}

    except db_utils.OperationalError as exc_db:
        if 'database is locked' in str(exc_db).lower():
            send_user_notification(user_id, task_id, query_display_name, 'RETRYING', f'База данных временно заблокирована ({current_api_name}), повторная попытка...', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            raise self.retry(exc=exc_db, countdown=15 + self.request.retries * 10)
        else:
            send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Операционная ошибка БД ({current_api_name}): {str(exc_db)}', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            return {'status': 'error', 'message': f'DB OperationalError: {str(exc_db)}'}
    except Exception as e:
        error_message_for_user = f'Внутренняя ошибка {current_api_name}: {type(e).__name__} - {str(e)}'
        self.update_state(state='FAILURE', meta={'identifier': query_display_name, 'error': error_message_for_user, 'traceback': self.request.exc_info if hasattr(self.request, 'exc_info') else str(e)})
        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', error_message_for_user, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'error', 'message': error_message_for_user, 'doi': query_display_name}


@shared_task(bind=True, max_retries=3, default_retry_delay=120)
def fetch_data_from_arxiv_task(
    self,
    arxiv_id_value: str,
    article_id_to_update: int = None, # Используем этот ID для обновления, если он есть
    user_id: int = None,
    originating_reference_link_id: int = None):

    task_id = self.request.id
    clean_arxiv_id = arxiv_id_value.replace('arXiv:', '').split('v')[0].strip()
    query_display_name = f"ARXIV:{clean_arxiv_id}"
    current_api_name = settings.API_SOURCE_NAMES['ARXIV']

    send_user_notification(user_id, task_id, query_display_name, 'PENDING', 'Начинаем обработку arXiv...', progress_percent=0, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

    if not clean_arxiv_id:
        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', 'ArXiv ID не указан.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'error', 'message': 'ArXiv ID не указан.'}

    api_url = f"http://export.arxiv.org/api/query?id_list={clean_arxiv_id}&max_results=1"
    try:
        # запрос к API arXiv, получение xml_content
        send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'Запрос к {api_url}', progress_percent=20, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        xml_content = response.text
    except requests.exceptions.RequestException as exc:
        # обработка ошибки сети/API
        send_user_notification(user_id, task_id, query_display_name, 'RETRYING', f'Ошибка сети/API arXiv: {str(exc)}. Повтор...', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        raise self.retry(exc=exc)

    try:
        root = ET.fromstring(xml_content)
        entry = root.find('atom:entry', ARXIV_NS)
        if entry is None:
            # если статья не найдена
            send_user_notification(user_id, task_id, query_display_name, 'NOT_FOUND', 'Статья не найдена в arXiv (нет <entry>).', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            return {'status': 'not_found', 'message': 'Article not found in arXiv (no <entry> tag).'}
    except ET.ParseError as xml_exc:
        # обработка ошибки парсинга XML
        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Ошибка парсинга XML от arXiv: {str(xml_exc)}', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'error', 'message': f'XML Parse Error: {str(xml_exc)}'}

    send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', 'Данные arXiv получены, обработка...', progress_percent=40, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)


    api_pdf_link = None
    for link_el in entry.findall('atom:link', ARXIV_NS):
        if link_el.get('title') == 'pdf' and link_el.get('href'):
            api_pdf_link = link_el.get('href')
            break

    if api_pdf_link:
        send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'Для: {arxiv_id_value} найден PDF URL: {api_pdf_link}. Начало получния PDF файла...', source_api=current_api_name)
        try:
            time.sleep(5)
            pdf_to_save = download_pdf(api_pdf_link, arxiv_id_value)
            if pdf_to_save:
                send_user_notification(user_id, task_id, query_display_name, 'INFO', f'PDF файл для: {arxiv_id_value} успешно получен из: {api_pdf_link}.', source_api=current_api_name)
            else:
                send_user_notification(user_id, task_id, query_display_name, 'WARNING', f'Не удалось получить PDF файл для: {arxiv_id_value} из {api_pdf_link}.', source_api=current_api_name)
        except Exception as exc:
            send_user_notification(user_id, task_id, query_display_name, 'WARNING', f'Ошибка при запросе PDF файла для: {arxiv_id_value} из: {api_pdf_link}. \nError: {exc}', source_api=current_api_name)

    try:
        with transaction.atomic(): # Используем select_for_update ниже, поэтому транзакция нужна
            article_owner = None
            if user_id:
                try:
                    article_owner = User.objects.get(id=user_id)
                except User.DoesNotExist:
                    send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Пользователь ID {user_id} не найден.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                    return {'status': 'error', 'message': f'User with ID {user_id} not found.'}

            # Извлечение данных из XML <entry>
            arxiv_title_el = entry.find('atom:title', ARXIV_NS)
            api_title = arxiv_title_el.text.strip().replace('\n', ' ').replace('  ', ' ') if arxiv_title_el is not None and arxiv_title_el.text else None
            arxiv_summary_el = entry.find('atom:summary', ARXIV_NS)
            api_abstract = arxiv_summary_el.text.strip().replace('\n', ' ') if arxiv_summary_el is not None and arxiv_summary_el.text else None
            arxiv_published_el = entry.find('atom:published', ARXIV_NS)
            arxiv_updated_el = entry.find('atom:updated', ARXIV_NS)
            api_publication_date_str = arxiv_updated_el.text if arxiv_updated_el is not None and arxiv_updated_el.text else (arxiv_published_el.text if arxiv_published_el is not None and arxiv_published_el.text else None)
            api_parsed_publication_date = None
            if api_publication_date_str:
                try:
                    api_parsed_publication_date = timezone.datetime.strptime(api_publication_date_str, '%Y-%m-%dT%H:%M:%SZ').date()
                except ValueError:
                    pass
            arxiv_doi_el = entry.find('arxiv:doi', ARXIV_NS)
            api_doi = arxiv_doi_el.text.strip().lower() if arxiv_doi_el is not None and arxiv_doi_el.text else None
            # api_pdf_link = None
            # for link_el in entry.findall('atom:link', ARXIV_NS):
            #     if link_el.get('title') == 'pdf' and link_el.get('href'):
            #         api_pdf_link = link_el.get('href')
            #         break
            api_parsed_authors = parse_arxiv_authors(entry)

            # --- Логика поиска или создания статьи ---
            article = None
            created = False

            if article_id_to_update: # Если диспетчер передал ID существующей статьи
                try:
                    article = Article.objects.select_for_update().get(id=article_id_to_update, user=article_owner)
                except Article.DoesNotExist:
                     send_user_notification(user_id, task_id, query_display_name, 'WARNING', f'Статья ID {article_id_to_update} (переданная для обновления) не найдена или не принадлежит пользователю.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

            if not article and api_doi: # Ищем по DOI, если есть
                try:
                    article = Article.objects.select_for_update().get(doi=api_doi)
                except Article.DoesNotExist:
                    pass

            if not article: # Ищем по arXiv ID
                try:
                    article = Article.objects.select_for_update().get(arxiv_id=clean_arxiv_id)
                except Article.DoesNotExist:
                    if not article_owner: # Пользователь обязателен для создания
                        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', 'Пользователь не указан для создания новой arXiv статьи.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                        return {'status': 'error', 'message': 'User not specified for new arXiv article.'}

                    creation_defaults = {'user': article_owner, 'title': api_title or f"Статья arXiv:{clean_arxiv_id}"}
                    if api_doi:
                        creation_defaults['doi'] = api_doi # Если DOI есть из arXiv, сразу его ставим

                    article = Article.objects.create(arxiv_id=clean_arxiv_id, **creation_defaults)
                    created = True

            if not article:
                 send_user_notification(user_id, task_id, query_display_name, 'FAILURE', 'Не удалось идентифицировать или создать запись для статьи arXiv.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                 return {'status': 'error', 'message': 'Could not identify or create article entry for arXiv.'}

            # --- Применение логики приоритетов ---
            priority_list = getattr(settings, 'API_SOURCE_OVERALL_PRIORITY', [])
            try:
                current_api_priority = priority_list.index(current_api_name)
            except ValueError:
                current_api_priority = float('inf')
            article_primary_source_priority = float('inf')
            if article.primary_source_api:
                try:
                    article_primary_source_priority = priority_list.index(article.primary_source_api)
                except ValueError:
                    pass

            can_fully_overwrite = (created or not article.primary_source_api or current_api_priority <= article_primary_source_priority)

            if can_fully_overwrite:
                article.primary_source_api = current_api_name

            if api_title and (can_fully_overwrite or not article.title):
                article.title = api_title
            if api_abstract and (can_fully_overwrite or not article.abstract):
                article.abstract = api_abstract
            if api_parsed_publication_date and (can_fully_overwrite or not article.publication_date):
                article.publication_date = api_parsed_publication_date

            if not article.arxiv_id:
                article.arxiv_id = clean_arxiv_id # Гарантируем, что arXiv ID установлен
            if api_doi and (not article.doi or (can_fully_overwrite and article.doi != api_doi)):
                article.doi = api_doi # Обновляем DOI если можем или его нет

            if api_parsed_authors:
                if can_fully_overwrite or not article.authors.exists():
                    article.articleauthororder_set.all().delete()
                    for order, author_obj in enumerate(api_parsed_authors):
                        ArticleAuthorOrder.objects.create(article=article, author=author_obj, order=order)

            # if api_abstract and (not article.cleaned_text_for_llm or len(api_abstract) > len(article.cleaned_text_for_llm or "")):
            #     article.cleaned_text_for_llm = api_abstract

            if api_pdf_link:
                if not article.best_oa_pdf_url or can_fully_overwrite or current_api_name == settings.API_SOURCE_NAMES['ARXIV']:
                    article.best_oa_pdf_url = api_pdf_link
                ArticleContent.objects.update_or_create(
                    article=article, source_api_name=current_api_name, format_type='link_pdf',
                    defaults={'content': api_pdf_link}
                )

                if pdf_to_save and not article.pdf_file:
                    extracted_markitdown_text = None
                    try:
                        pdf_file_name = f"article_{clean_arxiv_id}_{timezone.now().strftime('%Y%m%d%H%M%S')}.pdf"
                        article.pdf_file.save(pdf_file_name, ContentFile(pdf_to_save), save=False)
                        markitdown_service_url = 'http://localhost:8181/convert-document/'
                        pdf_file_path = article.pdf_file.path
                        if pdf_file_path:
                            send_user_notification(user_id, task_id, query_display_name, 'INFO', f'MarkItDown: Начало конвертации: {api_pdf_link} PDF файла: {pdf_file_path} в текст...', source_api=current_api_name)
                            with open(pdf_file_path, 'rb') as pdf_file_content_stream:
                                files = {'file': (os.path.basename(pdf_file_path), pdf_file_content_stream, 'application/pdf')}
                                time.sleep(1)
                                response = requests.post(markitdown_service_url, files=files, timeout=310)
                                response.raise_for_status()
                                data = response.json()
                                extracted_markitdown_text = data.get("markdown_text")
                                # source_version_info = data.get("source_tool", "markitdown_service_1.0")
                                if extracted_markitdown_text:
                                    send_user_notification(user_id, task_id, query_display_name, 'SUCCESS', f'MarkItDown: {api_pdf_link} для PDF файла: {pdf_file_path} верунл текст длинной: {len(extracted_markitdown_text)}.', source_api=current_api_name)
                                    article.pdf_text = extracted_markitdown_text
                                else:
                                    send_user_notification(user_id, task_id, query_display_name, 'INFO', f'MarkItDown: {api_pdf_link} для PDF файла: {pdf_file_path} не вернул текст. Ответ: {data}', source_api=current_api_name)
                    except requests.exceptions.RequestException as exc:
                        send_user_notification(user_id, task_id, query_display_name, 'RETRYING', f'MarkItDown: {api_pdf_link} для PDF файла: {pdf_file_path}. Ошибка Сети/API: {str(exc)}. Повтор...', source_api=current_api_name, riginating_reference_link_id=originating_reference_link_id)
                    except Exception as err:
                        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'MarkItDown: {api_pdf_link} для PDF файла: {pdf_file_path}. Ошибка: {str(err)}.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

            article.save()

            send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', 'Статья arXiv сохранена в БД.', progress_percent=70, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

            raw_xml_entry_string = ET.tostring(entry, encoding='unicode')
            ArticleContent.objects.update_or_create(
                article=article, source_api_name=current_api_name, format_type='xml_atom_entry',
                defaults={'content': raw_xml_entry_string}
            )

            if originating_reference_link_id and article:
                try:
                    ref_link = ReferenceLink.objects.get(id=originating_reference_link_id, source_article__user=article_owner)
                    ref_link.resolved_article = article
                    ref_link.status = ReferenceLink.StatusChoices.ARTICLE_LINKED
                    ref_link.save(update_fields=['resolved_article', 'status', 'updated_at'])
                    send_user_notification(user_id, task_id, query_display_name, 'INFO', f'Ссылка ID {ref_link.id} связана со статьей arXiv.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                except ReferenceLink.DoesNotExist:
                    pass
                except Exception as e_ref:
                    send_user_notification(user_id, task_id, query_display_name, 'ERROR', f'Ошибка обновления ref_link для arXiv: {str(e_ref)}', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

            final_message = f'Статья arXiv "{article.title[:30]}..." {"создана" if created else "обновлена"}.'
            send_user_notification(user_id, task_id, query_display_name, 'SUCCESS', final_message, progress_percent=100, article_id=article.id, created=created, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            return {'status': 'success', 'message': final_message, 'identifier': query_display_name, 'article_id': article.id}

    except db_utils.OperationalError as exc_db:
        if 'database is locked' in str(exc_db).lower():
            send_user_notification(user_id, task_id, query_display_name, 'RETRYING', f'База данных временно заблокирована ({current_api_name}), повторная попытка...', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            raise self.retry(exc=exc_db, countdown=15 + self.request.retries * 10)
        else:
            send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Операционная ошибка БД ({current_api_name}): {str(exc_db)}', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            return {'status': 'error', 'message': f'DB OperationalError: {str(exc_db)}'}
    except Exception as e:
        error_message_for_user = f'Внутренняя ошибка {current_api_name}: {type(e).__name__} - {str(e)}'
        self.update_state(state='FAILURE', meta={'identifier': query_display_name, 'error': error_message_for_user, 'traceback': self.request.exc_info if hasattr(self.request, 'exc_info') else str(e)})
        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', error_message_for_user, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'error', 'message': f'Внутренняя ошибка: {str(e)}', 'identifier': query_display_name}


@shared_task(bind=True, max_retries=3, default_retry_delay=120)
def fetch_data_from_europepmc_task(
    self,
    identifier_value: str,
    identifier_type: str = 'DOI',
    user_id: int = None,
    originating_reference_link_id: int = None,
    article_id_to_update: int = None):

    task_id = self.request.id
    query_display_name = f"{identifier_type.upper()}:{identifier_value}"
    current_api_name = settings.API_SOURCE_NAMES['EUROPEPMC']

    send_user_notification(user_id, task_id, query_display_name, 'PENDING', f'Начинаем обработку {current_api_name}...', progress_percent=0, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

    if not identifier_value:
        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', 'Идентификатор не указан.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'error', 'message': 'Идентификатор не указан.', 'identifier': query_display_name}

    article_owner = None
    if user_id:
        try:
            article_owner = User.objects.get(id=user_id)
        except User.DoesNotExist:
            send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Пользователь ID {user_id} не найден.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            return {'status': 'error', 'message': f'User with ID {user_id} not found.'}

    # Запрос к API поиска для получения метаданных и PMCID
    query_string = f"{identifier_type.upper()}:{identifier_value}"
    search_params = {'query': query_string, 'format': 'json', 'resultType': 'core', 'email': APP_EMAIL}
    api_search_url = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"

    try:
        send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'Запрос к {api_search_url}', progress_percent=20, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        response = requests.get(api_search_url, params=search_params, timeout=45)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as exc:
        send_user_notification(user_id, task_id, query_display_name, 'RETRYING', f'Ошибка сети/API {current_api_name}: {str(exc)}. Повтор...', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        raise self.retry(exc=exc)
    except json.JSONDecodeError as json_exc:
        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Ошибка декодирования JSON от {current_api_name}: {str(json_exc)}', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'error', 'message': f'{current_api_name} JSON Decode Error: {str(json_exc)}'}

    if not data or not data.get('resultList') or not data['resultList'].get('result'):
        send_user_notification(user_id, task_id, query_display_name, 'NOT_FOUND', 'Статья не найдена в Europe PMC.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'not_found', 'message': 'Article not found in Europe PMC.', 'identifier': query_display_name}

    api_article_data = data['resultList']['result'][0]
    send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', 'Метаданные получены, обработка...', progress_percent=40, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

    # Извлечение данных из ответа API EPMC
    api_title = api_article_data.get('title')
    api_abstract = api_article_data.get('abstractText')
    api_doi = api_article_data.get('doi')
    if api_doi:
        api_doi = api_doi.lower()
    api_pmid = api_article_data.get('pmid')
    api_pmcid = api_article_data.get('pmcid')

    api_pub_date_str = api_article_data.get('firstPublicationDate')
    api_parsed_date = None
    if api_pub_date_str:
        try:
            api_parsed_date = timezone.datetime.strptime(api_pub_date_str, '%Y-%m-%d').date()
        except ValueError:
            pass

    api_journal_name = None
    journal_info = api_article_data.get('journalInfo')
    if journal_info and isinstance(journal_info, dict):
        journal_details = journal_info.get('journal')
        if journal_details and isinstance(journal_details, dict) and journal_details.get('title'):
            api_journal_name = journal_details['title']

    api_parsed_authors = parse_europepmc_authors(api_article_data.get('authorList', {}).get('author'))

    # --- Загрузка полного текста по PMCID, если он есть ---
    full_text_xml_content = None
    if api_pmcid: # api_pmcid извлекается из api_article_data
        # PMCID в Europe PMC API используется с префиксом 'PMC'
        pmcid_for_url = api_pmcid if api_pmcid.upper().startswith('PMC') else f"PMC{api_pmcid}"
        full_text_url = f"https://www.ebi.ac.uk/europepmc/webservices/rest/{pmcid_for_url}/fullTextXML"
        send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'Найден PMCID: {pmcid_for_url}. Запрос полного текста...', progress_percent=50, source_api=current_api_name)
        try:
            # headers={'User-Agent': f'ScientificPapersApp/1.0 ({APP_EMAIL})'}
            headers = {'User-Agent': USER_AGENT_LIST[0]}
            full_text_response = requests.get(full_text_url, timeout=90, headers=headers)
            if full_text_response.status_code == 200:
                full_text_xml_content = full_text_response.text
                # print(f'******* EUROPEPMC**** pmcid_for_url: {pmcid_for_url}, full_text_xml_content: {full_text_xml_content}')
                send_user_notification(user_id, task_id, query_display_name, 'INFO', 'Полный текст JATS XML успешно получен из Europe PMC.', source_api=current_api_name)
            else:
                send_user_notification(user_id, task_id, query_display_name, 'WARNING', f'Не удалось получить полный текст из Europe PMC для {pmcid_for_url} (статус: {full_text_response.status_code}).', source_api=current_api_name)
        except Exception as exc:
            send_user_notification(user_id, task_id, query_display_name, 'WARNING', f'Ошибка при запросе полного текста из Europe PMC: {exc}', source_api=current_api_name)

    try:
        with transaction.atomic():
            # --- Логика поиска или создания статьи ---
            article = None
            created = False

            if article_id_to_update:
                try:
                    article = Article.objects.select_for_update().get(id=article_id_to_update, user=article_owner)
                except Article.DoesNotExist:
                    send_user_notification(user_id, task_id, query_display_name, 'WARNING', f'Статья ID {article_id_to_update} (для обновления) не найдена/не принадлежит пользователю.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

            if not article and api_doi:
                try:
                    article = Article.objects.select_for_update().get(doi=api_doi)
                except Article.DoesNotExist:
                    pass

            if not article and api_pmid:
                try:
                    article = Article.objects.select_for_update().get(pubmed_id=api_pmid)
                except Article.DoesNotExist:
                    pass

            if not article:
                if not article_owner:
                    send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Пользователь не указан для создания новой статьи {current_api_name}.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                    return {'status': 'error', 'message': f'User not specified for new {current_api_name} article.'}

                creation_kwargs = {'user': article_owner, 'title': api_title or f"Статья {current_api_name}: {query_display_name}"}

                if api_doi:
                    creation_kwargs['doi'] = api_doi
                elif api_pmid:
                    creation_kwargs['pubmed_id'] = api_pmid
                else:
                    send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'{current_api_name} API не вернул DOI или PMID для создания статьи.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                    return {'status': 'error', 'message': f'{current_api_name} API did not return DOI or PMID for creation.'}

                # is_user_initiated будет False по умолчанию (из модели), если это ссылка.
                # Если это прямой вызов задачи (не через диспетчер для ссылки), то is_user_initiated должен быть True.
                # Диспетчер уже должен был создать "корневую" статью с is_user_initiated=True.
                if originating_reference_link_id is None and article_id_to_update is None:
                    creation_kwargs['is_user_initiated'] = True

                article = Article.objects.create(**creation_kwargs)
                created = True

            if not article:
                 send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Не удалось идентифицировать или создать запись для статьи {current_api_name}.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                 return {'status': 'error', 'message': f'Could not identify or create article entry for {current_api_name}.'}

            # --- Применение логики приоритетов ---
            priority_list = getattr(settings, 'API_SOURCE_OVERALL_PRIORITY', [])
            try:
                current_api_priority = priority_list.index(current_api_name)
            except ValueError:
                current_api_priority = float('inf')
            article_primary_source_priority = float('inf')
            if article.primary_source_api:
                try:
                    article_primary_source_priority = priority_list.index(article.primary_source_api)
                except ValueError:
                    pass
            can_fully_overwrite = (created or not article.primary_source_api or current_api_priority <= article_primary_source_priority)

            if can_fully_overwrite:
                article.primary_source_api = current_api_name
            if api_title and (can_fully_overwrite or not article.title):
                article.title = api_title
            if api_abstract and (can_fully_overwrite or not article.abstract):
                article.abstract = api_abstract
            if api_parsed_date and (can_fully_overwrite or not article.publication_date):
                article.publication_date = api_parsed_date
            if api_journal_name and (can_fully_overwrite or not article.journal_name):
                article.journal_name = api_journal_name
            if api_doi and not article.doi:
                article.doi = api_doi
            if api_pmid and not article.pubmed_id:
                article.pubmed_id = api_pmid
            if api_parsed_authors:
                if can_fully_overwrite or not article.authors.exists():
                    article.articleauthororder_set.all().delete()
                    for order, author_obj in enumerate(api_parsed_authors):
                        ArticleAuthorOrder.objects.create(article=article, author=author_obj, order=order)

            # Если получен полный текст, он в приоритете
            if full_text_xml_content:
                # Сохраняем полный XML
                ArticleContent.objects.update_or_create(
                    article=article, source_api_name=current_api_name, format_type='epmc_fulltext_xml',
                    defaults={'content': full_text_xml_content}
                )

                # Извлекаем структурированный текст
                structured_data = extract_structured_text_from_jats(full_text_xml_content)
                if structured_data:
                    article.structured_content = structured_data
                    article.regenerate_cleaned_text_from_structured()

            # # Если полного текста нет, используем абстракт
            # elif api_abstract and (not article.cleaned_text_for_llm or len(api_abstract) > len(article.cleaned_text_for_llm or "")):
            #     article.cleaned_text_for_llm = api_abstract
            #     if not article.structured_content:
            #         article.structured_content = {'abstract': api_abstract}

            article.save()
            send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'Статья {current_api_name} сохранена в БД (до ссылок).', progress_percent=80, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

            # Сохраняем сырые метаданные из поиска
            ArticleContent.objects.update_or_create(
                article=article, source_api_name=current_api_name, format_type='json_metadata',
                defaults={'content': json.dumps(api_article_data)}
            )

            if originating_reference_link_id and article:
                try:
                    ref_link_to_resolve = ReferenceLink.objects.get(id=originating_reference_link_id, source_article__user=article_owner)
                    ref_link_to_resolve.resolved_article = article
                    ref_link_to_resolve.status = ReferenceLink.StatusChoices.ARTICLE_LINKED
                    ref_link_to_resolve.save(update_fields=['resolved_article', 'status', 'updated_at'])
                    send_user_notification(user_id, task_id, query_display_name, 'INFO', f'Ссылка ID {ref_link_to_resolve.id} связана со статьей {current_api_name}.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                except ReferenceLink.DoesNotExist:
                    pass
                except Exception as e_ref:
                    send_user_notification(user_id, task_id, query_display_name, 'ERROR', f'Ошибка обновления ref_link для {current_api_name}: {str(e_ref)}', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

            # --- ЗАПУСК ЗАДАЧИ СЕГМЕНТАЦИИ И СВЯЗЫВАНИЯ ---
            if article.is_user_initiated and full_text_xml_content:
                process_full_text_and_create_segments_task.delay(
                    article_id=article.id,
                    user_id=user_id
                )
                send_user_notification(user_id, task_id, query_display_name, 'INFO', 'Поставлена задача на автоматическое связывание текста и ссылок.', source_api=current_api_name)

            final_message = f'Статья {current_api_name} "{article.title[:30]}..." {"создана" if created else "обновлена"}.'
            if full_text_xml_content:
                final_message += " Получен полный текст, запущена сегментация."
            send_user_notification(user_id, task_id, query_display_name, 'SUCCESS', final_message, progress_percent=100, article_id=article.id, created=created, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            return {'status': 'success', 'message': final_message, 'identifier': query_display_name, 'article_id': article.id}

    except db_utils.OperationalError as exc_db:
        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Операционная ошибка БД ({current_api_name}): {str(exc_db)}', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        if 'database is locked' in str(exc_db).lower():
            raise self.retry(exc=exc_db, countdown=15 + self.request.retries * 10)
        return {'status': 'error', 'message': f'DB OperationalError: {str(exc_db)}'}
    except Exception as e:
        error_message_for_user = f'Внутренняя ошибка {current_api_name}: {type(e).__name__} - {str(e)}'
        self.update_state(state='FAILURE', meta={'identifier': query_display_name, 'error': error_message_for_user, 'traceback': self.request.exc_info if hasattr(self.request, 'exc_info') else str(e)})
        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', error_message_for_user, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'error', 'message': f'Внутренняя ошибка: {str(e)}', 'identifier': query_display_name}


@shared_task(bind=True, max_retries=3, default_retry_delay=180)
def fetch_data_from_s2_task(
    self,
    identifier_value: str,
    identifier_type: str = 'DOI',
    user_id: int = None,
    originating_reference_link_id: int = None,
    article_id_to_update: int = None):

    task_id = self.request.id
    current_api_name = settings.API_SOURCE_NAMES['SEMANTICSCHOLAR']
    s2_paper_id_prefix = ""

    if identifier_type.upper() == 'DOI':
        s2_paper_id_prefix = "DOI:"
    elif identifier_type.upper() == 'ARXIV':
        s2_paper_id_prefix = "ARXIV:"
    # Добавить другие типы, которые S2 поддерживает
    # TODO: узнать поддерживает ли S2 префиксы ниже
    elif identifier_type.upper() == 'PMID':
        s2_paper_id_prefix = "PMID:"
    elif identifier_type.upper() == 'PMCID':
        s2_paper_id_prefix = "PMCID:"

    s2_paper_id_for_api = f"{s2_paper_id_prefix}{identifier_value}"
    query_display_name = s2_paper_id_for_api

    send_user_notification(user_id, task_id, query_display_name, 'PENDING', f'Начинаем обработку {current_api_name}...', progress_percent=0, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

    if not identifier_value:
        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', 'Идентификатор не указан.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'error', 'message': 'Идентификатор не указан.'}

    # --- ИЗМЕНЕННЫЙ СПИСОК ПОЛЕЙ ---
    s2_fields = [
        'externalIds',
        'url',
        'title',
        'abstract',
        'venue',
        'year',
        'referenceCount',
        'citationCount',
        'influentialCitationCount',
        'isOpenAccess',
        'openAccessPdf',
        'fieldsOfStudy',
        'publicationTypes',
        'publicationDate',
        'journal',
        'authors',
        'tldr',
        'references.paperId', 'references.title', 'references.year', # Запрашиваем paperId, title, year для ссылок
        'citations.paperId', 'citations.title'  # Запрашиваем paperId, title для цитирующих статей (год удален из-за ошибки)
    ]
    # из-за ошибкит не запрашиваем 'references.doi', 'citations.doi', 'citations.year'

    params = {'fields': ",".join(s2_fields)}
    # headers = {'User-Agent': f'ScientificPapersApp/1.0 (mailto:{APP_EMAIL})'}
    headers = {'User-Agent': USER_AGENT_LIST[0]}
    # Если есть API ключ S2: headers['x-api-key'] = 'YOUR_S2_API_KEY_HERE'

    api_url = f"https://api.semanticscholar.org/graph/v1/paper/{s2_paper_id_for_api.replace(' ', '%20')}"

    try:
        send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'Запрос к {api_url}', progress_percent=20, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        response = requests.get(api_url, params=params, headers=headers, timeout=45)
        if response.status_code == 404:
            send_user_notification(user_id, task_id, query_display_name, 'NOT_FOUND', f'Статья не найдена в {current_api_name}.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            return {'status': 'not_found', 'message': f'Статья не найдена в {current_api_name}.'}
        response.raise_for_status()
        api_data = response.json()
    except requests.exceptions.RequestException as exc:
        send_user_notification(user_id, task_id, query_display_name, 'RETRYING', f'Ошибка сети/API {current_api_name}: {str(exc)}. Повтор...', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        raise self.retry(exc=exc)
    except json.JSONDecodeError as json_exc:
        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Ошибка декодирования JSON от {current_api_name}: {str(json_exc)}', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'error', 'message': f'{current_api_name} JSON Decode Error: {str(json_exc)}'}

    if not api_data or api_data.get('error'):
        error_msg = api_data.get('error', f'Неизвестная ошибка от {current_api_name} API.')
        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Ошибка {current_api_name} API: {error_msg}', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'error', 'message': f'Ошибка {current_api_name} API: {error_msg}'}

    send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'Данные {current_api_name} получены, обработка...', progress_percent=40, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

    pdf_to_save = None
    api_oa_pdf_url = api_data.get('openAccessPdf', {}).get('url') if isinstance(api_data.get('openAccessPdf'), dict) else None
    # if api_oa_pdf_url:
    #     send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'Для: {identifier_value} найден PDF URL: {api_oa_pdf_url}. Начало получния PDF файла...', source_api=current_api_name)
    #     try:
    #         time.sleep(5)
    #         pdf_to_save = download_pdf(api_oa_pdf_url, identifier_value)
    #         if pdf_to_save:
    #             send_user_notification(user_id, task_id, query_display_name, 'INFO', f'PDF файл для: {identifier_value} успешно получен из: {api_oa_pdf_url}.', source_api=current_api_name)
    #         else:
    #             send_user_notification(user_id, task_id, query_display_name, 'WARNING', f'Не удалось получить PDF файл для: {identifier_value} из {api_oa_pdf_url}.', source_api=current_api_name)
    #     except Exception as exc:
    #         send_user_notification(user_id, task_id, query_display_name, 'WARNING', f'Ошибка при запросе PDF файла для: {identifier_value} из: {api_oa_pdf_url}. \nError: {exc}', source_api=current_api_name)

    try:
        with transaction.atomic():
            article_owner = None
            if user_id:
                try:
                    article_owner = User.objects.get(id=user_id)
                except User.DoesNotExist:
                    send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Пользователь ID {user_id} не найден.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                    return {'status': 'error', 'message': f'User with ID {user_id} not found.'}

            # Извлечение данных из ответа API S2
            api_title = api_data.get('title')
            api_abstract = api_data.get('abstract')
            api_pub_date_str = api_data.get('publicationDate')
            api_parsed_date = None
            if api_pub_date_str:
                try:
                    api_parsed_date = timezone.datetime.strptime(api_pub_date_str, '%Y-%m-%d').date()
                except (ValueError, TypeError):
                    pass

            api_journal_info = api_data.get('journal')
            api_journal_name = None
            if api_journal_info and isinstance(api_journal_info, dict) and api_journal_info.get('name'):
                api_journal_name = api_journal_info['name']
            elif api_data.get('venue'): # Иногда S2 использует 'venue'
                api_journal_name = api_data.get('venue')

            api_parsed_authors = parse_s2_authors(api_data.get('authors'))

            ext_ids = api_data.get('externalIds', {})
            api_doi = ext_ids.get('DOI')
            if api_doi:
                api_doi = api_doi.lower()
            api_arxiv_id = ext_ids.get('ArXiv') # S2 использует 'ArXiv'
            api_pmid = ext_ids.get('PubMed')

            s2_tldr_data = api_data.get('tldr')
            api_tldr_text = s2_tldr_data.get('text') if isinstance(s2_tldr_data, dict) else None

            # api_oa_pdf_url = api_data.get('openAccessPdf', {}).get('url') if isinstance(api_data.get('openAccessPdf'), dict) else None

            # --- Логика поиска или создания статьи ---
            article = None
            created = False

            if article_id_to_update:
                try:
                    article = Article.objects.select_for_update().get(id=article_id_to_update, user=article_owner)
                except Article.DoesNotExist:
                    send_user_notification(user_id, task_id, query_display_name, 'WARNING', f'Статья ID {article_id_to_update} (для обновления) не найдена/не принадлежит пользователю.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

            # Пытаемся найти по идентификаторам из ответа S2, если не нашли по article_id_to_update
            if not article and api_doi:
                try:
                    article = Article.objects.select_for_update().get(doi=api_doi)
                except Article.DoesNotExist:
                    pass

            if not article and api_arxiv_id: # Если искали по arXiv ID или S2 его вернул
                try:
                    article = Article.objects.select_for_update().get(arxiv_id=api_arxiv_id)
                except Article.DoesNotExist:
                    pass

            if not article and api_pmid:
                try:
                    article = Article.objects.select_for_update().get(pubmed_id=api_pmid)
                except Article.DoesNotExist:
                    pass

            # Если статья не найдена и у нас есть основной идентификатор для создания (например, DOI или исходный идентификатор)
            if not article:
                primary_id_for_creation = None
                creation_kwargs = {'user': article_owner, 'title': api_title or f"Статья S2: {query_display_name}"}

                if api_doi: # Если S2 вернул DOI, используем его как основной для создания
                    primary_id_for_creation = api_doi
                    creation_kwargs['doi'] = api_doi
                elif identifier_type.upper() == 'DOI': # Если исходный запрос был по DOI
                    primary_id_for_creation = identifier_value.lower()
                    creation_kwargs['doi'] = primary_id_for_creation
                elif identifier_type.upper() == 'ARXIV' and (api_arxiv_id == identifier_value or not api_arxiv_id):
                     primary_id_for_creation = identifier_value # S2 paper_id был ARVIX:id
                     creation_kwargs['arxiv_id'] = identifier_value # Сохраняем чистый arXiv ID
                # Добавить другие идентификаторы для создания, если необходимо

                if primary_id_for_creation or creation_kwargs.get('arxiv_id'): # Если есть по чему создавать
                    if not article_owner:
                        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', 'Пользователь не указан для создания новой статьи S2.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                        return {'status': 'error', 'message': 'User not specified for new S2 article.'}
                    article = Article.objects.create(**creation_kwargs)
                    created = True

            if not article:
                 send_user_notification(user_id, task_id, query_display_name, 'FAILURE', 'Не удалось идентифицировать или создать запись для статьи S2.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                 return {'status': 'error', 'message': 'Could not identify or create article entry for S2.'}

            # --- Применение логики приоритетов ---
            priority_list = getattr(settings, 'API_SOURCE_OVERALL_PRIORITY', [])
            try:
                current_api_priority = priority_list.index(current_api_name)
            except ValueError:
                current_api_priority = float('inf')
            article_primary_source_priority = float('inf')
            if article.primary_source_api:
                try:
                    article_primary_source_priority = priority_list.index(article.primary_source_api)
                except ValueError:
                    pass
            can_fully_overwrite = (created or not article.primary_source_api or current_api_priority <= article_primary_source_priority)

            if can_fully_overwrite:
                article.primary_source_api = current_api_name

            if api_title and (can_fully_overwrite or not article.title):
                article.title = api_title
            if api_abstract and (can_fully_overwrite or not article.abstract):
                article.abstract = api_abstract
            if api_parsed_date and (can_fully_overwrite or not article.publication_date):
                article.publication_date = api_parsed_date
            if api_journal_name and (can_fully_overwrite or not article.journal_name):
                article.journal_name = api_journal_name

            # Идентификаторы (добавляем, если отсутствуют)
            if api_doi and not article.doi:
                article.doi = api_doi
            if api_arxiv_id and not article.arxiv_id:
                article.arxiv_id = api_arxiv_id
            if api_pmid and not article.pubmed_id:
                article.pubmed_id = api_pmid

            if api_parsed_authors:
                if can_fully_overwrite or not article.authors.exists():
                    article.articleauthororder_set.all().delete()
                    for order, author_obj in enumerate(api_parsed_authors):
                        ArticleAuthorOrder.objects.create(article=article, author=author_obj, order=order)

            # cleaned_text_for_llm (S2 дает абстракт или TLDR)
            text_for_llm_candidate = api_abstract
            if api_tldr_text and (not text_for_llm_candidate or len(api_tldr_text) > 10): # Если TLDR есть, и абстракт пуст или TLDR достаточно содержателен
                if not text_for_llm_candidate :
                    text_for_llm_candidate = f"TLDR: {api_tldr_text}"
                # Можно добавить TLDR в article.abstract, если он пуст, или в отдельное поле
                # article.tldr = api_tldr_text (если есть такое поле)

            # if text_for_llm_candidate and \
            #    (not article.cleaned_text_for_llm or len(text_for_llm_candidate) > len(article.cleaned_text_for_llm or "") + 50 or \
            #    (can_fully_overwrite and article.primary_source_api == current_api_name)):
            #     article.cleaned_text_for_llm = text_for_llm_candidate

            # OA PDF ссылка
            if api_oa_pdf_url and (not article.best_oa_pdf_url or can_fully_overwrite):
                article.best_oa_pdf_url = api_oa_pdf_url

                # if pdf_to_save and not article.pdf_file:
                #     extracted_markitdown_text = None
                #     try:
                #         pdf_file_name = f"article_{identifier_value}_{timezone.now().strftime('%Y%m%d%H%M%S')}.pdf"
                #         article.pdf_file.save(pdf_file_name, ContentFile(pdf_to_save), save=False)
                #         markitdown_service_url = 'http://localhost:8181/convert-document/'
                #         pdf_file_path = article.pdf_file.path
                #         if pdf_file_path:
                #             send_user_notification(user_id, task_id, query_display_name, 'INFO', f'MarkItDown: Начало конвертации: {identifier_value} PDF файла: {pdf_file_path} в текст...', source_api=current_api_name)
                #             with open(pdf_file_path, 'rb') as pdf_file_content_stream:
                #                 files = {'file': (os.path.basename(pdf_file_path), pdf_file_content_stream, 'application/pdf')}
                #                 time.sleep(1)
                #                 response = requests.post(markitdown_service_url, files=files, timeout=310)
                #                 response.raise_for_status()
                #                 data = response.json()
                #                 extracted_markitdown_text = data.get("markdown_text")
                #                 # source_version_info = data.get("source_tool", "markitdown_service_1.0")
                #                 if extracted_markitdown_text:
                #                     send_user_notification(user_id, task_id, query_display_name, 'SUCCESS', f'MarkItDown: {identifier_value} для PDF файла: {pdf_file_path} верунл текст длинной: {len(extracted_markitdown_text)}.', source_api=current_api_name)
                #                     article.pdf_text = extracted_markitdown_text
                #                 else:
                #                     send_user_notification(user_id, task_id, query_display_name, 'INFO', f'MarkItDown: {identifier_value} для PDF файла: {pdf_file_path} не вернул текст. Ответ: {data}', source_api=current_api_name)
                #     except requests.exceptions.RequestException as exc:
                #         send_user_notification(user_id, task_id, query_display_name, 'RETRYING', f'MarkItDown: {identifier_value} для PDF файла: {pdf_file_path}. Ошибка Сети/API: {str(exc)}. Повтор...', source_api=current_api_name, riginating_reference_link_id=originating_reference_link_id)
                #     except Exception as err:
                #         send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'MarkItDown: {identifier_value} для PDF файла: {pdf_file_path}. Ошибка: {str(err)}.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

            article.save()
            send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'Статья {current_api_name} сохранена в БД.', progress_percent=60, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

            ArticleContent.objects.update_or_create(
                article=article, source_api_name=current_api_name, format_type='json_metadata',
                defaults={'content': json.dumps(api_data)}
            )

            # Обновление ReferenceLink, если текущая задача S2 была вызвана для обработки конкретной ссылки
            if originating_reference_link_id and article:
                try:
                    ref_link_to_resolve = ReferenceLink.objects.get(id=originating_reference_link_id, source_article__user=article_owner)
                    ref_link_to_resolve.resolved_article = article
                    ref_link_to_resolve.status = ReferenceLink.StatusChoices.ARTICLE_LINKED
                    ref_link_to_resolve.save(update_fields=['resolved_article', 'status', 'updated_at'])
                    send_user_notification(user_id, task_id, query_display_name, 'INFO', f'Ссылка ID {ref_link_to_resolve.id} связана со статьей S2.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                except ReferenceLink.DoesNotExist:
                    pass
                except Exception as e_ref:
                    send_user_notification(user_id, task_id, query_display_name, 'ERROR', f'Ошибка обновления ref_link для S2: {str(e_ref)}', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

            final_message = f'Статья {current_api_name} "{article.title[:30]}..." {"создана" if created else "обновлена"}.'
            send_user_notification(user_id, task_id, query_display_name, 'SUCCESS', final_message, progress_percent=100, article_id=article.id, created=created, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            return {'status': 'success', 'message': final_message, 'identifier': query_display_name, 'article_id': article.id}

    except db_utils.OperationalError as exc_db:
        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Операционная ошибка БД ({current_api_name}): {str(exc_db)}', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        if 'database is locked' in str(exc_db).lower():
            raise self.retry(exc=exc_db, countdown=15 + self.request.retries * 10)
        return {'status': 'error', 'message': f'DB OperationalError: {str(exc_db)}'}
    except Exception as e:
        error_message_for_user = f'Внутренняя ошибка {current_api_name}: {type(e).__name__} - {str(e)}'
        self.update_state(state='FAILURE', meta={'identifier': query_display_name, 'error': error_message_for_user, 'traceback': self.request.exc_info if hasattr(self.request, 'exc_info') else str(e)})
        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', error_message_for_user, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'error', 'message': f'Внутренняя ошибка: {str(e)}', 'identifier': query_display_name}


@shared_task(bind=True, max_retries=3, default_retry_delay=120)
def fetch_data_from_pubmed_task(
    self,
    identifier_value: str,
    identifier_type: str = 'PMID',
    user_id: int = None,
    originating_reference_link_id: int = None,
    article_id_to_update: int = None):

    task_id = self.request.id
    query_display_name = f"{identifier_type.upper()}:{identifier_value}"
    current_api_name = settings.API_SOURCE_NAMES['PUBMED']

    send_user_notification(user_id, task_id, query_display_name, 'PENDING', f'Начинаем обработку {current_api_name}...', progress_percent=0, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

    if not identifier_value:
        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', 'Идентификатор не указан.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'error', 'message': 'Идентификатор не указан.'}

    article_owner = None
    if user_id:
        try:
            article_owner = User.objects.get(id=user_id)
        except User.DoesNotExist:
            send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Пользователь ID {user_id} не найден.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            return {'status': 'error', 'message': f'User ID {user_id} not found.'}

    eutils_base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    common_params = {'tool': NCBI_TOOL_NAME, 'email': NCBI_ADMIN_EMAIL}
    if NCBI_API_KEY:
        common_params['api_key'] = NCBI_API_KEY

    pmid_to_fetch = None
    original_doi = None
    if identifier_type.upper() == 'DOI':
        send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'Поиск PMID для DOI {identifier_value} через ESearch...', progress_percent=10, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        esearch_params = {**common_params, 'db': 'pubmed', 'term': f"{identifier_value}[DOI]", 'retmode': 'json'}
        try:
            # if not NCBI_API_KEY: time.sleep(0.35)
            time.sleep(0.35)
            esearch_response = requests.get(f"{eutils_base}esearch.fcgi", params=esearch_params, timeout=30)
            esearch_response.raise_for_status()
            esearch_data = esearch_response.json()
            if esearch_data.get('esearchresult', {}).get('idlist') and len(esearch_data['esearchresult']['idlist']) > 0:
                pmid_to_fetch = esearch_data['esearchresult']['idlist'][0]
                query_display_name = f"PMID:{pmid_to_fetch} (найден по DOI:{identifier_value})"
                send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'PMID {pmid_to_fetch} найден. Запрос EFetch...', progress_percent=20, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            else:
                send_user_notification(user_id, task_id, query_display_name, 'NOT_FOUND', 'PMID не найден для указанного DOI.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                return {'status': 'not_found', 'message': 'PMID not found for DOI.'}
        except requests.exceptions.RequestException as exc:
            send_user_notification(user_id, task_id, query_display_name, 'RETRYING', f'Ошибка ESearch {current_api_name}: {str(exc)}. Повтор...', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            raise self.retry(exc=exc)
        except json.JSONDecodeError as json_exc:
            send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Ошибка декодирования JSON от ESearch {current_api_name}: {str(json_exc)}.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            return {'status': 'error', 'message': f'ESearch JSON Decode Error: {str(json_exc)}'}
    elif identifier_type.upper() == 'PMID':
        pmid_to_fetch = identifier_value
    else:
        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Неподдерживаемый тип идентификатора для {current_api_name}: {identifier_type}.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'error', 'message': f'Unsupported identifier type for {current_api_name}: {identifier_type}.'}

    pmcid_to_fetch = None
    pmc_esearch_data = {}
    if identifier_type.upper() == 'DOI':
        send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'Поиск PMCID для DOI {identifier_value} через ESearch...', progress_percent=15, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        pmc_esearch_params = {**common_params, 'db': 'pmc', 'term': f"{identifier_value}[DOI]", 'retmode': 'json'}
        try:
            time.sleep(0.35)
            pmc_esearch_response = requests.get(f"{eutils_base}esearch.fcgi", params=pmc_esearch_params, timeout=30)
            pmc_esearch_response.raise_for_status()
            pmc_esearch_data = pmc_esearch_response.json()
            if pmc_esearch_data.get('esearchresult', {}).get('idlist') and len(pmc_esearch_data['esearchresult']['idlist']) > 0:
                pmcid_to_fetch = esearch_data['esearchresult']['idlist'][0]
                query_display_name = f"PMCID:{pmid_to_fetch} (найден по DOI:{identifier_value})"
                send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'PMCID {pmid_to_fetch} найден. Запрос EFetch...', progress_percent=20, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            else:
                send_user_notification(user_id, task_id, query_display_name, 'NOT_FOUND', 'PMCID не найден для указанного DOI.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                return {'status': 'not_found', 'message': 'PMCID not found for DOI.'}
        except requests.exceptions.RequestException as exc:
            send_user_notification(user_id, task_id, query_display_name, 'RETRYING', f'Ошибка ESearch {current_api_name}: {str(exc)}. Повтор...', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            raise self.retry(exc=exc)
        except json.JSONDecodeError as json_exc:
            send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Ошибка декодирования JSON от ESearch {current_api_name}: {str(json_exc)}.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            return {'status': 'error', 'message': f'ESearch JSON Decode Error: {str(json_exc)}'}
    elif identifier_type.upper() == 'PMID':
        pmc_esearch_from_pmid_params = {**common_params, 'db': 'pmc', 'term': f"{pmid_to_fetch}[PMID]", 'retmode': 'json'}
        try:
            time.sleep(0.35)
            pmc_esearch_from_pmid_response = requests.get(f"{eutils_base}esearch.fcgi", params=pmc_esearch_from_pmid_params, timeout=30)
            pmc_esearch_from_pmid_response.raise_for_status()
            pmc_esearch_from_pmid_data = pmc_esearch_from_pmid_response.json()
            if pmc_esearch_from_pmid_data.get('esearchresult', {}).get('idlist') and len(pmc_esearch_from_pmid_data['esearchresult']['idlist']) > 0:
                pmcid_to_fetch = pmc_esearch_from_pmid_data['esearchresult']['idlist'][0]
                query_display_name = f"PMCID:{pmid_to_fetch} (найден по DOI:{identifier_value})"
                send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'PMCID {pmid_to_fetch} найден. Запрос EFetch...', progress_percent=20, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        except requests.exceptions.RequestException as exc:
            send_user_notification(user_id, task_id, query_display_name, 'RETRYING', f'Ошибка ESearch {current_api_name}: {str(exc)}. Повтор...', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            raise self.retry(exc=exc)
        except json.JSONDecodeError as json_exc:
            send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Ошибка декодирования JSON от ESearch {current_api_name}: {str(json_exc)}.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            return {'status': 'error', 'message': f'ESearch JSON Decode Error: {str(json_exc)}'}
    else:
        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Неподдерживаемый тип идентификатора для {current_api_name}: {identifier_type}.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'error', 'message': f'Unsupported identifier type for {current_api_name}: {identifier_type}.'}

    if not pmid_to_fetch and not pmcid_to_fetch:
        # ESearch не нашёл PMID и PMCID
        send_user_notification(user_id, task_id, query_display_name, 'NOT_FOUND', 'PMID не найден для указанного DOI.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'not_found', 'message': 'PMID and PMCID not found for DOI.'}

    # Обновляем имя для отображения, если нашли PMID
    if original_doi:
        query_display_name = f"PMID:{pmid_to_fetch} (из DOI:{original_doi})"

    # Запрос метаданных и PMCID из db=pubmed
    efetch_params = {**common_params, 'db': 'pubmed', 'id': pmid_to_fetch, 'retmode': 'xml', 'rettype': 'abstract'}
    xml_content_pubmed = None
    try:
        # if not NCBI_API_KEY: time.sleep(0.35)
        time.sleep(0.35)
        send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', 'Запрос метаданных из PubMed...', progress_percent=25, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        efetch_response = requests.get(f"{eutils_base}efetch.fcgi", params=efetch_params, timeout=45)
        efetch_response.raise_for_status()
        xml_content_pubmed = efetch_response.text
    except Exception as exc:
        send_user_notification(user_id, task_id, query_display_name, 'RETRYING', f'Ошибка EFetch (db=pubmed): {exc}. Повтор...', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        raise self.retry(exc=exc)

    # Парсинг метаданных из ответа 'pubmed'
    api_title, api_abstract, api_journal_title, api_doi, api_pmcid = None, None, None, None, None
    api_parsed_authors, api_mesh_terms = [], []
    api_parsed_publication_date = None

    try:
        root = ET.fromstring(xml_content_pubmed)
        pubmed_article_node = root.find('.//PubmedArticle')
        if pubmed_article_node is None:
            raise ValueError("Структура PubmedArticle не найдена в XML.")
        article_node = pubmed_article_node.find('.//Article')
        if article_node is None:
            send_user_notification(user_id, task_id, query_display_name, 'NOT_FOUND', 'Тег Article не найден в PubmedArticle.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            # return {'status': 'not_found', 'message': 'Article tag not found in PubmedArticle.'}
            raise ValueError("Тег Article не найден в PubmedArticle.")

        title_el = article_node.find('.//ArticleTitle')
        api_title = title_el.text.strip() if title_el is not None and title_el.text else None

        abstract_text_parts = []
        for abst_text_node in article_node.findall('.//AbstractText'):
            if abst_text_node.text:
                label = abst_text_node.get('Label')
                if label:
                    abstract_text_parts.append(f"{label.upper()}: {abst_text_node.text.strip()}")
                else:
                    abstract_text_parts.append(abst_text_node.text.strip())
        api_abstract = "\n\n".join(abstract_text_parts) if abstract_text_parts else None # Разделяем параграфы абстракта

        api_parsed_authors = parse_pubmed_authors(article_node.find('.//AuthorList'))

        journal_node = article_node.find('.//Journal')
        if journal_node is not None:
            journal_title_el = journal_node.find('./Title')
            if journal_title_el is not None and journal_title_el.text:
                api_journal_title = journal_title_el.text.strip()

        pubdate_node = article_node.find('.//Journal/JournalIssue/PubDate')
        if pubdate_node is not None:
            year_el, month_el, day_el = pubdate_node.find('./Year'), pubdate_node.find('./Month'), pubdate_node.find('./Day')
            if year_el is not None and year_el.text:
                try:
                    year = int(year_el.text)
                    month_str = month_el.text if month_el is not None and month_el.text else "1"
                    day_str = day_el.text if day_el is not None and day_el.text else "1"
                    month_map = {'jan':1,'feb':2,'mar':3,'apr':4,'may':5,'jun':6,'jul':7,'aug':8,'sep':9,'oct':10,'nov':11,'dec':12}
                    month = int(month_str) if month_str.isdigit() else month_map.get(month_str.lower()[:3], 1)
                    api_parsed_publication_date = timezone.datetime(year, month, int(day_str)).date()
                except (ValueError, TypeError):
                    pass

        # ВАЖНО: ищем PMCID именно для основной статьи
        pmcid_node = pubmed_article_node.find(".//PubmedData/ArticleIdList/ArticleId[@IdType='pmc']")
        if pmcid_node is not None and pmcid_node.text:
            api_pmcid = pmcid_node.text.strip() # Получаем полный PMCID, например "PMC1234567"

        doi_node = pubmed_article_node.find(".//ArticleIdList/ArticleId[@IdType='doi']")
        if doi_node is not None and doi_node.text:
            api_doi = doi_node.text.strip().lower()

        mesh_list_node = pubmed_article_node.find('.//MeshHeadingList')
        if mesh_list_node is not None:
            for mesh_node in mesh_list_node.findall('./MeshHeading'):
                desc_el = mesh_node.find('./DescriptorName')
                if desc_el is not None and desc_el.text:
                    api_mesh_terms.append(desc_el.text.strip())
    except Exception as e_xml:
        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Ошибка парсинга XML PubMed: {e_xml}', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'error', 'message': f'Error parsing PubMed XML: {e_xml}'}

    # --- ЭТАП 2: ПОЛУЧЕНИЕ ПОЛНОГО ТЕКСТА ИЗ PMC (db=pmc), ЕСЛИ ЕСТЬ PMCID ---
    if not api_pmcid and pmcid_to_fetch:
        api_pmcid = pmcid_to_fetch

    full_text_xml_pmc = None
    pdf_to_save = None

    if api_pmcid:
        send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'Найден PMCID: {api_pmcid}. Запрос полного текста...', progress_percent=50, source_api=current_api_name)
        api_pmcid = api_pmcid if api_pmcid.upper().startswith('PMC') else f"PMC{api_pmcid}"
        pmc_efetch_params = {**common_params, 'db': 'pmc', 'id': api_pmcid, 'retmode': 'xml'} # rettype не нужен, вернет полный XML
        try:
            # if not NCBI_API_KEY: time.sleep(0.35)
            time.sleep(0.35)
            pmc_response = requests.get(f"{eutils_base}efetch.fcgi", params=pmc_efetch_params, timeout=90)
            if pmc_response.status_code == 200:
                full_text_xml_pmc = pmc_response.text
                # print(f'******** PUBMED *** api_pmcid: {api_pmcid}, \nfull_text_xml_pmc: {full_text_xml_pmc}')
                send_user_notification(user_id, task_id, query_display_name, 'INFO', 'Полный текст JATS XML успешно получен из PMC.', source_api=current_api_name)
            else:
                send_user_notification(user_id, task_id, query_display_name, 'WARNING', f'Не удалось получить полный текст из PMC для {api_pmcid} (статус: {pmc_response.status_code}).', source_api=current_api_name)
        except Exception as exc:
            send_user_notification(user_id, task_id, query_display_name, 'WARNING', f'Ошибка при запросе полного текста из PMC: {exc}', source_api=current_api_name)

        send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'Найден PMCID: {api_pmcid}. Начало получния PDF файла...', progress_percent=55, source_api=current_api_name)
        try:
            time.sleep(5)
            pdf_file_name = f"article_{api_pmcid}_{timezone.now().strftime('%Y%m%d%H%M%S')}.pdf"
            pmc_pdf_url = f'https://pmc.ncbi.nlm.nih.gov/articles/{api_pmcid}/pdf/'
            pdf_to_save = download_pdf(pmc_pdf_url, identifier_value)
            if pdf_to_save:
                send_user_notification(user_id, task_id, query_display_name, 'INFO', 'PDF файл успешно получен из PMC.', source_api=current_api_name)
            else:
                send_user_notification(user_id, task_id, query_display_name, 'WARNING', f'Не удалось получить PDF из PMC для {api_pmcid}.', source_api=current_api_name)
        except Exception as exc:
            send_user_notification(user_id, task_id, query_display_name, 'WARNING', f'Ошибка при запросе PDF файла из PMC: {exc}', source_api=current_api_name)

    # --- ЭТАП 3: СОХРАНЕНИЕ В БД ---
    try:
        with transaction.atomic():
            article = None
            created = False

            if article_id_to_update:
                try:
                    article = Article.objects.select_for_update().get(id=article_id_to_update, user=article_owner)
                except Article.DoesNotExist:
                    send_user_notification(user_id, task_id, query_display_name, 'WARNING', f'Статья ID {article_id_to_update} (для обновления) не найдена/не принадлежит пользователю.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

            if not article and api_doi:
                try:
                    article = Article.objects.select_for_update().get(doi=api_doi)
                except Article.DoesNotExist:
                    pass

            if not article and pmid_to_fetch:
                try:
                    article = Article.objects.select_for_update().get(pubmed_id=pmid_to_fetch)
                except Article.DoesNotExist:
                    if not article_owner:
                        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Пользователь не определен для создания новой {current_api_name} статьи.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                        return {'status': 'error', 'message': f'User not determined for new {current_api_name} article.'}

                    creation_defaults = {'user': article_owner, 'title': api_title or f"Статья {current_api_name}:{pmid_to_fetch}"}
                    if api_doi:
                        creation_defaults['doi'] = api_doi
                    # is_user_initiated будет False по умолчанию, если это не основной вызов
                    if originating_reference_link_id is None and article_id_to_update is None: # Только если это прямой вызов для новой статьи
                         creation_defaults['is_user_initiated'] = True

                    article = Article.objects.create(pubmed_id=pmid_to_fetch, **creation_defaults)
                    created = True

            if not article:
                send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Не удалось создать/найти запись для {current_api_name} статьи.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                return {'status': 'error', 'message': f'Failed to create/find article entry for {current_api_name}.'}

            # Применение логики приоритетов
            priority_list = getattr(settings, 'API_SOURCE_OVERALL_PRIORITY', [])
            try:
                current_api_priority = priority_list.index(current_api_name)
            except ValueError:
                current_api_priority = float('inf')
            article_primary_source_priority = float('inf')
            if article.primary_source_api:
                try:
                    article_primary_source_priority = priority_list.index(article.primary_source_api)
                except ValueError:
                    pass
            can_fully_overwrite = (created or not article.primary_source_api or current_api_priority <= article_primary_source_priority)

            if can_fully_overwrite:
                article.primary_source_api = current_api_name

            # Обновляем метаданные из 'pubmed' (EFetch)
            if api_title and (can_fully_overwrite or not article.title):
                article.title = api_title
            if api_abstract and (can_fully_overwrite or not article.abstract):
                article.abstract = api_abstract
            if api_parsed_publication_date and (can_fully_overwrite or not article.publication_date):
                article.publication_date = api_parsed_publication_date
            if api_journal_title and (can_fully_overwrite or not article.journal_name):
                article.journal_name = api_journal_title

            if api_doi and not article.doi:
                article.doi = api_doi
            if not article.pubmed_id:
                article.pubmed_id = pmid_to_fetch
            if api_pmcid and not article.pmc_id:
                article.pmc_id = api_pmcid

            if api_parsed_authors:
                if can_fully_overwrite or not article.authors.exists():
                    article.articleauthororder_set.all().delete()
                    for order, author_obj in enumerate(api_parsed_authors):
                        ArticleAuthorOrder.objects.create(article=article, author=author_obj, order=order)

            # Если получен полный текст из PMC, он имеет приоритет
            if full_text_xml_pmc:
                structured_data = extract_structured_text_from_jats(full_text_xml_pmc)
                if structured_data:
                    article.structured_content = structured_data
                    article.regenerate_cleaned_text_from_structured() # Вызываем метод модели для обновления cleaned_text_for_llm

                # --- Парсинг и сохранение ссылок из полного текста ---
                process_references_flag = (originating_reference_link_id is None)
                if process_references_flag and full_text_xml_pmc:
                    send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', 'Извлечение ссылок из полного текста PMC...', progress_percent=85, source_api=current_api_name)

                    print('******** pubmed > parse_references_from_jats')
                    parsed_references = parse_references_from_jats(full_text_xml_pmc)
                    if parsed_references:
                        send_user_notification(user_id, task_id, query_display_name, 'INFO', f'Найдено {len(parsed_references)} ссылок в полном тексте. Обработка...', source_api=current_api_name)

                        processed_jats_ref_count = 0
                        for ref_data in parsed_references:

                            ref_doi_jats = ref_data.get('doi')
                            ref_raw_text_jats = ref_data.get('raw_text')
                            jats_ref_id = ref_data.get('jats_ref_id')

                            # Нам нужен хотя бы какой-то идентификатор для ссылки (DOI, текст или внутренний ID)
                            if not (ref_doi_jats or ref_raw_text_jats or jats_ref_id):
                                continue

                            # Готовим данные для сохранения
                            ref_link_defaults = {
                                'raw_reference_text': ref_raw_text_jats,
                                'manual_data_json': { # Сохраняем все, что распарсили
                                    'jats_ref_id': jats_ref_id,
                                    'title': ref_data.get('title'),
                                    'year': ref_data.get('year'),
                                    'authors_str': ref_data.get('authors_str'),
                                    'journal_title': ref_data.get('journal_title'),
                                    'doi_from_source': ref_doi_jats # DOI, извлеченный из JATS
                                }
                            }
                            # Удаляем None значения из manual_data_json
                            ref_link_defaults['manual_data_json'] = {k: v for k, v in ref_link_defaults['manual_data_json'].items() if v is not None}
                            if not ref_link_defaults['manual_data_json']:
                                ref_link_defaults['manual_data_json'] = None

                            # Определяем параметры для поиска существующей ссылки (чтобы избежать дублей)
                            lookup_params = {'source_article': article}
                            if ref_doi_jats:
                                lookup_params['target_article_doi'] = ref_doi_jats
                            # Если нет DOI, но есть jats_ref_id, можно искать по нему (требует поддержки БД для JSON-поиска)
                            # Для PostgreSQL можно так:
                            elif jats_ref_id:
                                lookup_params['manual_data_json__jats_ref_id'] = jats_ref_id
                            # Если нет ни DOI, ни jats_ref_id, используем сырой текст как последний вариант
                            elif ref_raw_text_jats:
                                lookup_params['raw_reference_text'] = ref_raw_text_jats
                            else:
                                continue # Пропускаем, если не за что зацепиться

                            # Устанавливаем статус и DOI в defaults
                            if ref_doi_jats:
                                ref_link_defaults['target_article_doi'] = ref_doi_jats
                                ref_link_defaults['status'] = ReferenceLink.StatusChoices.DOI_PROVIDED_NEEDS_LOOKUP
                            else:
                                ref_link_defaults['status'] = ReferenceLink.StatusChoices.PENDING_DOI_INPUT

                            # Создаем или обновляем объект ReferenceLink
                            ref_obj, ref_created = ReferenceLink.objects.update_or_create(
                                **lookup_params,
                                defaults=ref_link_defaults
                            )
                            processed_jats_ref_count += 1

                            # Если у ссылки есть DOI, ставим в очередь на обработку
                            if ref_doi_jats:
                                send_user_notification(user_id, task_id, query_display_name, 'INFO', f'PMC JATS: Найдена ссылка {ref_obj.id} с DOI: {ref_doi_jats}. Запуск конвейера.', source_api=current_api_name)
                                process_article_pipeline_task.delay(
                                    identifier_value=ref_doi_jats,
                                    identifier_type='DOI',
                                    user_id=user_id,
                                    originating_reference_link_id=ref_obj.id # Передаем ID созданной/обновленной ссылки
                                )

                        if processed_jats_ref_count > 0:
                            send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'PMC JATS: Обработано {processed_jats_ref_count} ссылок.', progress_percent=90, source_api=current_api_name)

            # # Если полного текста нет, используем абстракт из 'pubmed'
            # elif api_abstract and (not article.cleaned_text_for_llm or len(api_abstract) > len(article.cleaned_text_for_llm or "")):
            #     article.cleaned_text_for_llm = api_abstract
            #     if not article.structured_content: # Если нет и структурированного контента
            #         article.structured_content = {'abstract': api_abstract}

            extracted_markitdown_text = None
            if pdf_to_save and not article.pdf_file:
                try:
                    article.pdf_file.save(pdf_file_name, ContentFile(pdf_to_save), save=False)
                    markitdown_service_url = 'http://localhost:8181/convert-document/'
                    pdf_file_path = article.pdf_file.path
                    if pdf_file_path:
                        send_user_notification(user_id, task_id, query_display_name, 'INFO', f'MarkItDown: Начало конвертации: {api_pmcid} PDF файла: {pdf_file_path} в текст...', source_api=current_api_name)
                        with open(pdf_file_path, 'rb') as pdf_file_content_stream:
                            files = {'file': (os.path.basename(pdf_file_path), pdf_file_content_stream, 'application/pdf')}
                            time.sleep(1)
                            response = requests.post(markitdown_service_url, files=files, timeout=310)
                            response.raise_for_status()
                            data = response.json()
                            extracted_markitdown_text = data.get("markdown_text")
                            # source_version_info = data.get("source_tool", "markitdown_service_1.0")
                            if extracted_markitdown_text:
                                send_user_notification(user_id, task_id, query_display_name, 'SUCCESS', f'MarkItDown: {api_pmcid} для PDF файла: {pdf_file_path} верунл текст длинной: {len(extracted_markitdown_text)}.', source_api=current_api_name)
                                article.pdf_text = extracted_markitdown_text
                            else:
                                send_user_notification(user_id, task_id, query_display_name, 'INFO', f'MarkItDown: {api_pmcid} для PDF файла: {pdf_file_path} не вернул текст. Ответ: {data}', source_api=current_api_name)
                except requests.exceptions.RequestException as exc:
                    send_user_notification(user_id, task_id, query_display_name, 'RETRYING', f'MarkItDown: {api_pmcid} для PDF файла: {pdf_file_path}. Ошибка Сети/API: {str(exc)}. Повтор...', source_api=current_api_name, riginating_reference_link_id=originating_reference_link_id)
                except Exception as err:
                    send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'MarkItDown: {api_pmcid} для PDF файла: {pdf_file_path}. Ошибка: {str(err)}.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

            article.save()

            # сохранение ArticleContent для pubmed_entry, pmc_fulltext_xml, mesh_terms
            if xml_content_pubmed:
                ArticleContent.objects.update_or_create(
                    article=article, source_api_name=current_api_name, format_type='xml_pubmed_entry',
                    defaults={'content': xml_content_pubmed}
                )
            if full_text_xml_pmc:
                ArticleContent.objects.update_or_create(
                    article=article, source_api_name=current_api_name, format_type='pmc_fulltext_xml',
                    defaults={'content': full_text_xml_pmc}
                )
            if api_mesh_terms:
                ArticleContent.objects.update_or_create(
                    article=article, source_api_name=current_api_name, format_type='mesh_terms',
                    defaults={'content': api_mesh_terms}
                )
            # if extracted_markitdown_text:
            #     ArticleContent.objects.update_or_create(
            #         article=article, source_api_name=current_api_name, format_type='pdf_markitdown_text',
            #         defaults={'content': extracted_markitdown_text}
            #     )

            # обновление ReferenceLink, если originating_reference_link_id
            if originating_reference_link_id and article:
                try:
                    ref_link = ReferenceLink.objects.get(id=originating_reference_link_id, source_article__user=article_owner)
                    ref_link.resolved_article = article
                    ref_link.status = ReferenceLink.StatusChoices.ARTICLE_LINKED
                    ref_link.save(update_fields=['resolved_article', 'status', 'updated_at'])
                    send_user_notification(user_id, task_id, query_display_name, 'INFO', f'Ссылка ID {ref_link.id} связана со статьей {current_api_name}.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                except ReferenceLink.DoesNotExist:
                    pass
                except Exception as e_ref:
                    send_user_notification(user_id, task_id, query_display_name, 'ERROR', f'Ошибка обновления ref_link для {current_api_name}: {str(e_ref)}', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

            # Если был получен полный текст из PMC, запускаем задачу для его анализа и связывания
            # if full_text_xml_pmc:
            if article.is_user_initiated and full_text_xml_pmc:
                # Мы передаем ID статьи и ID пользователя
                process_full_text_and_create_segments_task.delay(
                    article_id=article.id,
                    user_id=user_id
                )
                send_user_notification(user_id, task_id, query_display_name, 'INFO', 'Поставлена задача на автоматическое связывание текста и ссылок.', source_api=current_api_name)

            # финальное уведомление
            final_message = f'Статья {current_api_name} "{article.title[:30]}..." {"создана" if created else "обновлена"}.'
            if full_text_xml_pmc:
                final_message += " Получен полный текст, запущена сегментация."
            send_user_notification(user_id, task_id, query_display_name, 'SUCCESS', final_message, progress_percent=100, article_id=article.id, created=created, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            return {'status': 'success', 'message': final_message, 'identifier': query_display_name, 'article_id': article.id}

    except db_utils.OperationalError as exc_db:
        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Операционная ошибка БД ({current_api_name}): {str(exc_db)}', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        if 'database is locked' in str(exc_db).lower():
            raise self.retry(exc=exc_db, countdown=15 + self.request.retries * 10)
        return {'status': 'error', 'message': f'DB OperationalError: {str(exc_db)}'}
    except Exception as e:
        error_message_for_user = f'Внутренняя ошибка {current_api_name}: {type(e).__name__} - {str(e)}'
        self.update_state(state='FAILURE', meta={'identifier': query_display_name, 'error': error_message_for_user, 'traceback': self.request.exc_info if hasattr(self.request, 'exc_info') else str(e)})
        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', error_message_for_user, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'error', 'message': f'Внутренняя ошибка: {str(e)}', 'identifier': query_display_name}


@shared_task(bind=True, max_retries=3, default_retry_delay=60)
def fetch_data_from_rxiv_task(
    self,
    doi: str,
    user_id: int = None,
    originating_reference_link_id: int = None,
    article_id_to_update: int = None):

    task_id = self.request.id
    clean_doi_for_query = doi.replace('DOI:', '').strip().lower()
    query_display_name = f"DOI:{clean_doi_for_query} (Rxiv)"
    current_api_name = settings.API_SOURCE_NAMES['RXIV']

    send_user_notification(user_id, task_id, query_display_name, 'PENDING', f'Начинаем обработку {current_api_name}...', progress_percent=0, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

    if not clean_doi_for_query:
        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', 'DOI не указан.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'error', 'message': 'DOI не указан.'}

    # --- Попытка получить данные с серверов bioRxiv/medRxiv ---
    servers_to_try = ['biorxiv', 'medrxiv']
    api_preprint_data_collection = None
    actual_server_name = None

    for server_name_attempt in servers_to_try:
        temp_api_url = f"https://api.biorxiv.org/details/{server_name_attempt}/{clean_doi_for_query}/na/json"
        # headers = {'User-Agent': f'ScientificPapersApp/1.0 (mailto:{APP_EMAIL})'}
        headers = {'User-Agent': USER_AGENT_LIST[0]}
        send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'Попытка запроса к {server_name_attempt.upper()}: {temp_api_url}', progress_percent=10, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

        try:
            response = requests.get(temp_api_url, headers=headers, timeout=30)
            if response.status_code == 200:
                data = response.json()
                if data and data.get('collection') and isinstance(data['collection'], list) and data['collection']:
                    # Проверяем DOI в ответе для большей точности
                    # (иногда API может вернуть ближайшее совпадение, если точного нет)
                    # Хотя для /details/[server]/[DOI] это маловероятно, но проверка не помешает.
                    # Первый элемент коллекции обычно самый свежий или единственный.
                    first_result_in_collection = data['collection'][0]
                    doi_in_response = first_result_in_collection.get('doi','').lower()
                    if doi_in_response == clean_doi_for_query:
                        api_preprint_data_collection = data['collection'] # Сохраняем всю коллекцию, если понадобится (но используем первый)
                        # final_api_url = temp_api_url
                        actual_server_name = server_name_attempt
                        send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'Данные успешно получены с {actual_server_name.upper()}.', progress_percent=20, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                        break
                    else:
                        send_user_notification(user_id, task_id, query_display_name, 'INFO', f'DOI в ответе {actual_server_name.upper()} ({doi_in_response}) не совпал с запрошенным ({clean_doi_for_query}).', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                else:
                    send_user_notification(user_id, task_id, query_display_name, 'INFO', f'{server_name_attempt.upper()} вернул пустую коллекцию для DOI.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            elif response.status_code == 404:
                send_user_notification(user_id, task_id, query_display_name, 'INFO', f'Препринт не найден на {server_name_attempt.upper()}.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            else:
                response.raise_for_status() # Вызовет HTTPError для других кодов ошибок
        except requests.exceptions.RequestException as exc:
            send_user_notification(user_id, task_id, query_display_name, 'WARNING', f'Ошибка сети/API {server_name_attempt.upper()}: {str(exc)}.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            if server_name_attempt == servers_to_try[-1]: # Если это последняя попытка
                raise self.retry(exc=exc) # Повторяем задачу целиком
            time.sleep(1) # Пауза перед попыткой следующего сервера
        except json.JSONDecodeError as json_exc:
            send_user_notification(user_id, task_id, query_display_name, 'WARNING', f'Ошибка декодирования JSON от {server_name_attempt.upper()}: {str(json_exc)}.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            if server_name_attempt == servers_to_try[-1]:
                 return {'status': 'error', 'message': f'{server_name_attempt.upper()} JSON Decode Error: {str(json_exc)}'}
            time.sleep(1)
        except Exception as e_inner:
            send_user_notification(user_id, task_id, query_display_name, 'WARNING', f'Неожиданная ошибка при запросе к {server_name_attempt.upper()}: {str(e_inner)}.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            if server_name_attempt == servers_to_try[-1]:
                 return {'status': 'error', 'message': f'Unexpected error during {server_name_attempt.upper()} request: {str(e_inner)}'}
            time.sleep(1)

    if not api_preprint_data_collection:
        send_user_notification(user_id, task_id, query_display_name, 'NOT_FOUND', f'Препринт не найден ни на одном из Rxiv серверов ({", ".join(servers_to_try)}).', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'not_found', 'message': 'Preprint not found on Rxiv servers.'}

    # Используем данные первого (и обычно единственного релевантного) элемента коллекции
    api_data = api_preprint_data_collection[0]
    send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'Данные {current_api_name} ({actual_server_name}) получены, обработка...', progress_percent=40, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

    # Пытаемся получить PDF файл
    pdf_to_save = None
    api_pdf_link = None
    api_server_name_from_data = api_data.get('server', actual_server_name)
    api_doi_from_rxiv = api_data.get('doi', clean_doi_for_query).lower() # Используем исходный DOI, если API его не вернул
    version_str = api_data.get('version', '1')

    if api_server_name_from_data and api_doi_from_rxiv:
        api_pdf_link = f"https://{api_server_name_from_data}.org/content/{api_doi_from_rxiv}v{version_str}.full.pdf"
        if api_pdf_link:
            send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'Для: {doi} найден PDF URL: {api_pdf_link}. Начало получния PDF файла...', source_api=current_api_name)
            try:
                time.sleep(5)
                pdf_to_save = download_pdf(api_pdf_link, doi)
                if pdf_to_save:
                    send_user_notification(user_id, task_id, query_display_name, 'INFO', f'PDF файл для: {doi} успешно получен из: {api_pdf_link}.', source_api=current_api_name)
                else:
                    send_user_notification(user_id, task_id, query_display_name, 'WARNING', f'Не удалось получить PDF файл для: {doi} из {api_pdf_link}.', source_api=current_api_name)
            except Exception as exc:
                send_user_notification(user_id, task_id, query_display_name, 'WARNING', f'Ошибка при запросе PDF файла для: {doi} из: {api_pdf_link}. \nError: {exc}', source_api=current_api_name)

    try:
        with transaction.atomic():
            article_owner = None
            if user_id:
                try:
                    article_owner = User.objects.get(id=user_id)
                except User.DoesNotExist:
                    send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Пользователь ID {user_id} не найден.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                    return {'status': 'error', 'message': f'User with ID {user_id} not found.'}

            # Извлечение данных из ответа API Rxiv
            # api_doi_from_rxiv = api_data.get('doi', clean_doi_for_query).lower() # Используем исходный DOI, если API его не вернул
            api_title = api_data.get('title')
            api_abstract = api_data.get('abstract')
            api_date_str = api_data.get('date') # Дата постинга
            api_parsed_date = None
            if api_date_str:
                try:
                    api_parsed_date = timezone.datetime.strptime(api_date_str, '%Y-%m-%d').date()
                except ValueError:
                    pass

            # api_server_name_from_data = api_data.get('server', actual_server_name)
            api_category = api_data.get('category')
            api_journal_name_construct = f"{api_server_name_from_data.upper()} ({api_category or 'N/A'})" if api_server_name_from_data else None

            api_parsed_authors = parse_rxiv_authors(api_data.get('authors'))
            api_jats_xml_url = api_data.get('jatsxml')

            # --- Логика поиска или создания статьи ---
            article = None
            created = False

            if article_id_to_update:
                try:
                    article = Article.objects.select_for_update().get(id=article_id_to_update, user=article_owner)
                except Article.DoesNotExist:
                    send_user_notification(user_id, task_id, query_display_name, 'WARNING', f'Статья ID {article_id_to_update} (для обновления) не найдена/не принадлежит пользователю.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

            if not article: # Если не нашли по ID или ID не был передан
                try:
                    article = Article.objects.select_for_update().get(doi=api_doi_from_rxiv)
                except Article.DoesNotExist:
                    if not article_owner:
                        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Пользователь не указан для создания новой {current_api_name} статьи.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                        return {'status': 'error', 'message': f'User not specified for new {current_api_name} article.'}

                    # article = Article.objects.create(doi=api_doi_from_rxiv, user=article_owner, title=api_title or f"Препринт {current_api_name}: {api_doi_from_rxiv}")
                    # created = True

                    is_user_initiated = False
                    if originating_reference_link_id is None and article_id_to_update is None:
                        is_user_initiated = True

                    article = Article.objects.create(
                        doi=api_doi_from_rxiv,
                        user=article_owner,
                        title=api_title or f"Препринт {current_api_name}: {api_doi_from_rxiv}",
                        is_user_initiated=is_user_initiated,
                    )
                    created = True

            if not article:
                send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Не удалось создать/найти запись для {current_api_name} препринта.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                return {'status': 'error', 'message': f'Failed to create/find article entry for {current_api_name}.'}

            # --- Применение логики приоритетов ---
            priority_list = getattr(settings, 'API_SOURCE_OVERALL_PRIORITY', [])
            try:
                current_api_priority = priority_list.index(current_api_name)
            except ValueError:
                current_api_priority = float('inf')
            article_primary_source_priority = float('inf')
            if article.primary_source_api:
                try:
                    article_primary_source_priority = priority_list.index(article.primary_source_api)
                except ValueError:
                    pass

            can_fully_overwrite = (created or not article.primary_source_api or current_api_priority <= article_primary_source_priority)

            if can_fully_overwrite:
                article.primary_source_api = current_api_name
            if api_title and (can_fully_overwrite or not article.title):
                article.title = api_title
            if api_abstract and (can_fully_overwrite or not article.abstract):
                article.abstract = api_abstract
            if api_parsed_date and (can_fully_overwrite or not article.publication_date):
                article.publication_date = api_parsed_date
            if api_journal_name_construct and (can_fully_overwrite or not article.journal_name):
                article.journal_name = api_journal_name_construct

            if not article.doi:
                article.doi = api_doi_from_rxiv # Убедимся, что DOI сохранен

            if api_parsed_authors:
                if can_fully_overwrite or not article.authors.exists():
                    article.articleauthororder_set.all().delete()
                    for order, author_obj in enumerate(api_parsed_authors):
                        ArticleAuthorOrder.objects.create(article=article, author=author_obj, order=order)

            # --- Обработка полного текста JATS XML ---
            full_text_xml_content = None
            api_jats_xml_url = api_data.get('jatsxml')
            if api_jats_xml_url:
                send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', 'Найден JATS XML Rxiv. Загрузка...', progress_percent=60, source_api=current_api_name)
                try:
                    # headers={'User-Agent': f'ScientificPapersApp/1.0 ({APP_EMAIL})'}
                    headers = {'User-Agent': f'{USER_AGENT_LIST[0]} (mailto:{APP_EMAIL})'}
                    jats_response = requests.get(api_jats_xml_url, timeout=60, headers=headers)
                    print(f'**** RXIV headers: {headers} \napi_jats_xml_url: {api_jats_xml_url} \njats_response: {jats_response}')
                    jats_response.raise_for_status()
                    full_text_xml_content = jats_response.text
                    print(f'****** RXIV * api_jats_xml_url: {api_jats_xml_url}, \nfull_text_xml_content: {full_text_xml_content}')
                except Exception as e_jats:
                    send_user_notification(user_id, task_id, query_display_name, 'WARNING', f'Ошибка при обработке JATS XML от Rxiv: {str(e_jats)}', source_api=current_api_name)

            # Если получен полный текст, он в приоритете
            if full_text_xml_content:
                ArticleContent.objects.update_or_create(
                    article=article, source_api_name=current_api_name, format_type='rxiv_jats_xml_fulltext',
                    defaults={'content': full_text_xml_content}
                )
                structured_data = extract_structured_text_from_jats(full_text_xml_content)
                if structured_data:
                    article.structured_content = structured_data
                    article.regenerate_cleaned_text_from_structured() # Обновляем cleaned_text_for_llm


                # --- Парсинг и сохранение ссылок из полного текста ---
                process_references_flag = (originating_reference_link_id is None)
                if process_references_flag and not article.references_made.all():
                    send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', 'Извлечение ссылок из полного текста RXIV...', source_api=current_api_name)

                    print('******** rxiv > parse_references_from_jats')
                    parsed_references = parse_references_from_jats(full_text_xml_content)
                    if parsed_references:
                        send_user_notification(user_id, task_id, query_display_name, 'INFO', f'RXIV: Найдено {len(parsed_references)} ссылок в полном тексте. Обработка...', source_api=current_api_name)

                        processed_jats_ref_count = 0
                        for ref_data in parsed_references:

                            ref_doi_jats = ref_data.get('doi')
                            ref_raw_text_jats = ref_data.get('raw_text')
                            jats_ref_id = ref_data.get('jats_ref_id')

                            # Нам нужен хотя бы какой-то идентификатор для ссылки (DOI, текст или внутренний ID)
                            if not (ref_doi_jats or ref_raw_text_jats or jats_ref_id):
                                continue

                            # Готовим данные для сохранения
                            ref_link_defaults = {
                                'raw_reference_text': ref_raw_text_jats,
                                'manual_data_json': { # Сохраняем все, что распарсили
                                    'jats_ref_id': jats_ref_id,
                                    'title': ref_data.get('title'),
                                    'year': ref_data.get('year'),
                                    'authors_str': ref_data.get('authors_str'),
                                    'journal_title': ref_data.get('journal_title'),
                                    'doi_from_source': ref_doi_jats # DOI, извлеченный из JATS
                                }
                            }
                            print(f'********* RXIV ref_link_defaults: {ref_link_defaults}')
                            # Удаляем None значения из manual_data_json
                            ref_link_defaults['manual_data_json'] = {k: v for k, v in ref_link_defaults['manual_data_json'].items() if v is not None}
                            if not ref_link_defaults['manual_data_json']:
                                ref_link_defaults['manual_data_json'] = None

                            # Определяем параметры для поиска существующей ссылки (чтобы избежать дублей)
                            lookup_params = {'source_article': article}
                            if ref_doi_jats:
                                lookup_params['target_article_doi'] = ref_doi_jats
                            # Если нет DOI, но есть jats_ref_id, можно искать по нему (требует поддержки БД для JSON-поиска)
                            # Для PostgreSQL можно так:
                            elif jats_ref_id:
                                lookup_params['manual_data_json__jats_ref_id'] = jats_ref_id
                            # Если нет ни DOI, ни jats_ref_id, используем сырой текст как последний вариант
                            elif ref_raw_text_jats:
                                lookup_params['raw_reference_text'] = ref_raw_text_jats
                            else:
                                continue # Пропускаем, если не за что зацепиться

                            # Устанавливаем статус и DOI в defaults
                            if ref_doi_jats:
                                ref_link_defaults['target_article_doi'] = ref_doi_jats
                                ref_link_defaults['status'] = ReferenceLink.StatusChoices.DOI_PROVIDED_NEEDS_LOOKUP
                            else:
                                ref_link_defaults['status'] = ReferenceLink.StatusChoices.PENDING_DOI_INPUT

                            # Создаем или обновляем объект ReferenceLink
                            ref_obj, ref_created = ReferenceLink.objects.update_or_create(
                                **lookup_params,
                                defaults=ref_link_defaults
                            )
                            processed_jats_ref_count += 1

                            # Если у ссылки есть DOI, ставим в очередь на обработку
                            if ref_doi_jats:
                                send_user_notification(user_id, task_id, query_display_name, 'INFO', f'RXIV JATS: Найдена ссылка {ref_obj.id} с DOI: {ref_doi_jats}. Запуск конвейера.', source_api=current_api_name)
                                process_article_pipeline_task.delay(
                                    identifier_value=ref_doi_jats,
                                    identifier_type='DOI',
                                    user_id=user_id,
                                    originating_reference_link_id=ref_obj.id # Передаем ID созданной/обновленной ссылки
                                )

                        if processed_jats_ref_count > 0:
                            send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'RXIV JATS: Обработано {processed_jats_ref_count} ссылок.', source_api=current_api_name)

            # # Если полного текста нет, используем абстракт
            # elif api_abstract and (not article.cleaned_text_for_llm or len(api_abstract) > len(article.cleaned_text_for_llm or "")):
            #     article.cleaned_text_for_llm = api_abstract
            #     if not article.structured_content:
            #         article.structured_content = {'abstract': api_abstract}

            # Ссылка на PDF
            # version_str = api_preprint_data.get('version', '1')

            # version_str = api_data.get('version', '1')
            # if api_server_name_from_data and api_doi_from_rxiv:
            #     api_pdf_link = f"https://{api_server_name_from_data}.org/content/{api_doi_from_rxiv}v{version_str}.full.pdf"
            if api_pdf_link and (not article.best_oa_pdf_url or can_fully_overwrite):
                article.best_oa_pdf_url = api_pdf_link
                ArticleContent.objects.update_or_create(
                    article=article, source_api_name=current_api_name, format_type='link_pdf',
                    defaults={'content': api_pdf_link}
                )

            if pdf_to_save and not article.pdf_file:
                extracted_markitdown_text = None
                try:
                    pdf_file_name = f"article_{doi}_{timezone.now().strftime('%Y%m%d%H%M%S')}.pdf"
                    article.pdf_file.save(pdf_file_name, ContentFile(pdf_to_save), save=False)
                    markitdown_service_url = 'http://localhost:8181/convert-document/'
                    pdf_file_path = article.pdf_file.path
                    if pdf_file_path:
                        send_user_notification(user_id, task_id, query_display_name, 'INFO', f'MarkItDown: Начало конвертации: {doi} PDF файла: {pdf_file_path} в текст...', source_api=current_api_name)
                        with open(pdf_file_path, 'rb') as pdf_file_content_stream:
                            files = {'file': (os.path.basename(pdf_file_path), pdf_file_content_stream, 'application/pdf')}
                            time.sleep(1)
                            response = requests.post(markitdown_service_url, files=files, timeout=310)
                            response.raise_for_status()
                            data = response.json()
                            extracted_markitdown_text = data.get("markdown_text")
                            # source_version_info = data.get("source_tool", "markitdown_service_1.0")
                            if extracted_markitdown_text:
                                send_user_notification(user_id, task_id, query_display_name, 'SUCCESS', f'MarkItDown: {doi} для PDF файла: {pdf_file_path} верунл текст длинной: {len(extracted_markitdown_text)}.', source_api=current_api_name)
                                article.pdf_text = extracted_markitdown_text
                            else:
                                send_user_notification(user_id, task_id, query_display_name, 'INFO', f'MarkItDown: {doi} для PDF файла: {pdf_file_path} не вернул текст. Ответ: {data}', source_api=current_api_name)
                except requests.exceptions.RequestException as exc:
                    send_user_notification(user_id, task_id, query_display_name, 'RETRYING', f'MarkItDown: {doi} для PDF файла: {pdf_file_path}. Ошибка Сети/API: {str(exc)}. Повтор...', source_api=current_api_name, riginating_reference_link_id=originating_reference_link_id)
                except Exception as err:
                    send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'MarkItDown: {doi} для PDF файла: {pdf_file_path}. Ошибка: {str(err)}.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

            article.save()
            send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'Препринт {current_api_name} сохранен.', progress_percent=80, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

            # Сохраняем сырые метаданные из API
            ArticleContent.objects.update_or_create(
                article=article, source_api_name=current_api_name, format_type='json_metadata',
                defaults={'content': json.dumps(api_data)}
            )

            if originating_reference_link_id and article:
                try:
                    ref_link = ReferenceLink.objects.get(id=originating_reference_link_id, source_article__user=article_owner)
                    ref_link.resolved_article = article
                    ref_link.status = ReferenceLink.StatusChoices.ARTICLE_LINKED
                    ref_link.save(update_fields=['resolved_article', 'status', 'updated_at'])
                    send_user_notification(user_id, task_id, query_display_name, 'INFO', f'Ссылка ID {ref_link.id} связана с {current_api_name} препринтом.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                except ReferenceLink.DoesNotExist:
                    pass
                except Exception as e_ref:
                    send_user_notification(user_id, task_id, query_display_name, 'ERROR', f'Ошибка обновления ref_link для {current_api_name}: {str(e_ref)}', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

            # --- ЗАПУСК ЗАДАЧИ СЕГМЕНТАЦИИ И СВЯЗЫВАНИЯ ---
            if article.is_user_initiated and full_text_xml_content:
                process_full_text_and_create_segments_task.delay(
                    article_id=article.id,
                    user_id=user_id
                )
                send_user_notification(user_id, task_id, query_display_name, 'INFO', 'Поставлена задача на автоматическое связывание текста и ссылок.', source_api=current_api_name)

            final_message = f'Препринт {current_api_name} ({api_server_name_from_data}) "{article.title[:30]}..." {"создана" if created else "обновлена"}.'
            send_user_notification(user_id, task_id, query_display_name, 'SUCCESS', final_message, progress_percent=100, article_id=article.id, created=created, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            return {'status': 'success', 'message': final_message, 'identifier': query_display_name, 'article_id': article.id}

    except db_utils.OperationalError as exc_db:
        if 'database is locked' in str(exc_db).lower():
            send_user_notification(user_id, task_id, query_display_name, 'RETRYING', f'База данных временно заблокирована ({current_api_name}), повторная попытка...', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            raise self.retry(exc=exc_db, countdown=15 + self.request.retries * 10)
        else:
            send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Операционная ошибка БД ({current_api_name}): {str(exc_db)}', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            return {'status': 'error', 'message': f'DB OperationalError: {str(exc_db)}'}
    except Exception as e:
        error_message_for_user = f'Внутренняя ошибка {current_api_name}: {type(e).__name__} - {str(e)}'
        self.update_state(state='FAILURE', meta={'identifier': query_display_name, 'error': error_message_for_user, 'traceback': self.request.exc_info if hasattr(self.request, 'exc_info') else str(e)})
        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', error_message_for_user, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'error', 'message': f'Внутренняя ошибка: {str(e)}', 'identifier': query_display_name}


@shared_task(bind=True, max_retries=3, default_retry_delay=60)
def fetch_data_from_unpaywall_task(
    self,
    doi: str,
    article_id: int,
    user_id: int = None):

    task_id = self.request.id
    query_display_name = f"DOI:{doi} (Unpaywall)"

    send_user_notification(user_id, task_id, doi, 'PENDING', 'Запрос OA-статуса из Unpaywall...', progress_percent=0, source_api=settings.API_SOURCE_NAMES['UNPAYWALL'])

    if not doi or not article_id:
        error_msg = "DOI или ID статьи не указаны для Unpaywall."
        send_user_notification(user_id, task_id, doi, 'FAILURE', error_msg, source_api=settings.API_SOURCE_NAMES['UNPAYWALL'])
        return {'status': 'error', 'message': error_msg}

    try:
        article = Article.objects.get(id=article_id)
    except Article.DoesNotExist:
        error_msg = f"Статья с ID {article_id} не найдена для обновления Unpaywall."
        send_user_notification(user_id, task_id, doi, 'FAILURE', error_msg, source_api=settings.API_SOURCE_NAMES['UNPAYWALL'])
        return {'status': 'error', 'message': error_msg}

    api_url = f"https://api.unpaywall.org/v2/{doi}?email={APP_EMAIL}"

    try:
        send_user_notification(user_id, task_id, doi, 'PROGRESS', f'Запрос к {api_url}', progress_percent=30, source_api=settings.API_SOURCE_NAMES['UNPAYWALL'])
        response = requests.get(api_url, timeout=30)

        if response.status_code == 404:
            send_user_notification(user_id, task_id, doi, 'NOT_FOUND', 'DOI не найден в Unpaywall.', source_api=settings.API_SOURCE_NAMES['UNPAYWALL'])
            article.oa_status = 'not_found_in_unpaywall' # Отмечаем, что искали
            article.save(update_fields=['oa_status', 'updated_at'])
            return {'status': 'not_found', 'message': 'DOI not found in Unpaywall.', 'doi': doi}

        response.raise_for_status() # Ошибки кроме 404
        data = response.json()

    except requests.exceptions.RequestException as exc:
        send_user_notification(user_id, task_id, doi, 'RETRYING', f'Ошибка сети/API Unpaywall: {str(exc)}. Повтор...', source_api=settings.API_SOURCE_NAMES['UNPAYWALL'])
        raise self.retry(exc=exc)

    if not data: # Unpaywall может вернуть 200 OK и null/пустой объект, если DOI некорректен или нет данных
        send_user_notification(user_id, task_id, doi, 'NOT_FOUND', 'Unpaywall вернул пустые данные для DOI.', source_api=settings.API_SOURCE_NAMES['UNPAYWALL'])
        article.oa_status = 'no_data_from_unpaywall'
        article.save(update_fields=['oa_status', 'updated_at'])
        return {'status': 'no_data', 'message': 'Unpaywall returned no data for DOI.', 'doi': doi}

    send_user_notification(user_id, task_id, doi, 'PROGRESS', 'Данные Unpaywall получены, обновление статьи...', progress_percent=70, source_api=settings.API_SOURCE_NAMES['UNPAYWALL'])

    updated_fields = []

    if 'oa_status' in data:
        article.oa_status = data['oa_status']
        updated_fields.append('oa_status')

    api_pdf_link = None
    best_oa_location = data.get('best_oa_location')
    if isinstance(best_oa_location, dict):
        if best_oa_location.get('url'):
            article.best_oa_url = best_oa_location['url']
            updated_fields.append('best_oa_url')
        if best_oa_location.get('url_for_pdf'):
            api_pdf_link = best_oa_location['url_for_pdf']
            article.best_oa_pdf_url = api_pdf_link
            updated_fields.append('best_oa_pdf_url')
        if best_oa_location.get('license'):
            article.oa_license = best_oa_location['license']
            updated_fields.append('oa_license')

        # Сохраняем информацию о best_oa_location в ArticleContent
        ArticleContent.objects.update_or_create(
            article=article,
            source_api_name=settings.API_SOURCE_NAMES['UNPAYWALL'],
            format_type='json_oadata', # Тип для данных Unpaywall
            defaults={'content': json.dumps(best_oa_location)} # Сохраняем как JSON-строку
        )

    if updated_fields:
        updated_fields.append('updated_at') # Всегда обновляем updated_at
        article.save(update_fields=updated_fields)

    # # Пытаемся получить PDF файл
    # pdf_to_save = None
    # if api_pdf_link:
    #     send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'Для: {doi} найден PDF URL: {api_pdf_link}. Начало получния PDF файла...', source_api=settings.API_SOURCE_NAMES['UNPAYWALL'])
    #     try:
    #         time.sleep(5)
    #         pdf_to_save = download_pdf(api_pdf_link, doi)
    #         if pdf_to_save:
    #             send_user_notification(user_id, task_id, query_display_name, 'INFO', f'PDF файл для: {doi} успешно получен из: {api_pdf_link}.', source_api=settings.API_SOURCE_NAMES['UNPAYWALL'])
    #         else:
    #             send_user_notification(user_id, task_id, query_display_name, 'WARNING', f'Не удалось получить PDF файл для: {doi} из {api_pdf_link}.', source_api=settings.API_SOURCE_NAMES['UNPAYWALL'])
    #     except Exception as exc:
    #         send_user_notification(user_id, task_id, query_display_name, 'WARNING', f'Ошибка при запросе PDF файла для: {doi} из: {api_pdf_link}. \nError: {exc}', source_api=settings.API_SOURCE_NAMES['UNPAYWALL'])

    # if pdf_to_save and not article.pdf_file:
    #     extracted_markitdown_text = None
    #     try:
    #         pdf_file_name = f"article_{doi}_{timezone.now().strftime('%Y%m%d%H%M%S')}.pdf"
    #         article.pdf_file.save(pdf_file_name, ContentFile(pdf_to_save), save=False)
    #         markitdown_service_url = 'http://localhost:8181/convert-document/'
    #         pdf_file_path = article.pdf_file.path
    #         if pdf_file_path:
    #             send_user_notification(user_id, task_id, query_display_name, 'INFO', f'MarkItDown: Начало конвертации: {doi} PDF файла: {pdf_file_path} в текст...', source_api=settings.API_SOURCE_NAMES['UNPAYWALL'])
    #             with open(pdf_file_path, 'rb') as pdf_file_content_stream:
    #                 files = {'file': (os.path.basename(pdf_file_path), pdf_file_content_stream, 'application/pdf')}
    #                 time.sleep(1)
    #                 response = requests.post(markitdown_service_url, files=files, timeout=310)
    #                 response.raise_for_status()
    #                 data = response.json()
    #                 extracted_markitdown_text = data.get("markdown_text")
    #                 # source_version_info = data.get("source_tool", "markitdown_service_1.0")
    #                 if extracted_markitdown_text:
    #                     send_user_notification(user_id, task_id, query_display_name, 'SUCCESS', f'MarkItDown: {doi} для PDF файла: {pdf_file_path} верунл текст длинной: {len(extracted_markitdown_text)}.', source_api=settings.API_SOURCE_NAMES['UNPAYWALL'])
    #                     article.pdf_text = extracted_markitdown_text
    #                     article.save()
    #                 else:
    #                     send_user_notification(user_id, task_id, query_display_name, 'INFO', f'MarkItDown: {doi} для PDF файла: {pdf_file_path} не вернул текст. Ответ: {data}', source_api=settings.API_SOURCE_NAMES['UNPAYWALL'])
    #     except requests.exceptions.RequestException as exc:
    #         send_user_notification(user_id, task_id, query_display_name, 'RETRYING', f'MarkItDown: {doi} для PDF файла: {pdf_file_path}. Ошибка Сети/API: {str(exc)}. Повтор...', source_api=settings.API_SOURCE_NAMES['UNPAYWALL'])
    #     except Exception as err:
    #         send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'MarkItDown: {doi} для PDF файла: {pdf_file_path}. Ошибка: {str(err)}.', source_api=settings.API_SOURCE_NAMES['UNPAYWALL'])

    send_user_notification(user_id, task_id, doi, 'SUCCESS', f'OA-статус для DOI {doi}: {article.oa_status}.', progress_percent=100, article_id=article.id, source_api=settings.API_SOURCE_NAMES['UNPAYWALL'])
    return {'status': 'success', 'message': f'OA status processed: {article.oa_status}', 'doi': doi, 'article_id': article.id}


@shared_task(bind=True, max_retries=3, default_retry_delay=180)
def fetch_data_from_openalex_task(
    self,
    identifier_value: str,
    identifier_type: str = 'DOI',
    user_id: int = None,
    originating_reference_link_id: int = None,
    article_id_to_update: int = None):

    task_id = self.request.id
    current_api_name = settings.API_SOURCE_NAMES['OPENALEX']

    # Формируем идентификатор для OpenAlex API
    oa_identifier = None
    id_type_upper = identifier_type.upper()
    if id_type_upper == 'DOI':
        oa_identifier = f"doi:{identifier_value.lower()}"
    elif id_type_upper == 'PMID':
        oa_identifier = f"pmid:{identifier_value}"
    elif id_type_upper == 'OPENALEXID': # Если передаем собственный ID OpenAlex
        oa_identifier = identifier_value
    elif id_type_upper == 'PMCID': # OpenAlex ожидает PMCID без префикса 'PMC' в URL, но с префиксом 'pmc:' в запросе к works/pmc:ID
        # Для https://api.openalex.org/works/pmc:{pmcid}
        # где pmcid это сам номер, например PMC12345 -> https://api.openalex.org/works/pmc:PMC12345
        # Но если мы используем /works/{VALUE}, то значение должно быть правильным ID, а не просто номером.
        # Проще всего, если есть DOI или PMID, использовать их.
        # Если только PMCID, то нужно сформировать URL типа /works/https://pubmed.ncbi.nlm.nih.gov/PMC + номер
        # Либо использовать /works/pmcid:{номер} если API это поддерживает.
        # Документация OpenAlex говорит: /works/pmc:PMC1234567 - это значит, что identifier_value должен быть полным PMC ID
        # или если PMCID дан как просто номер '1234567', то `oa_identifier = f"pmc:PMC{identifier_value}"`
        # Для нашего примера, если identifier_type 'PMCID', то identifier_value должен быть полным, например 'PMC1234567'
        if identifier_value.upper().startswith('PMC'):
            oa_identifier = f"pmcid:{identifier_value}" # Пример, уточнить по документации OpenAlex
        else: # если передан просто номер
            oa_identifier = f"pmcid:PMC{identifier_value}"


    # Если мы не смогли сформировать oa_identifier, или исходный identifier_value был пуст
    if not oa_identifier or not identifier_value:
        send_user_notification(user_id, task_id, identifier_value, 'FAILURE', 'Идентификатор не указан или не поддерживается для OpenAlex.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'error', 'message': 'Identifier not provided or not supported for OpenAlex.'}

    query_display_name = oa_identifier # Для уведомлений

    send_user_notification(user_id, task_id, query_display_name, 'PENDING', f'Начинаем обработку {current_api_name}...', progress_percent=0, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

    api_url = f"https://api.openalex.org/works/{oa_identifier}?mailto={APP_EMAIL}"
    # headers = {'User-Agent': f'ScientificPapersApp/1.0 (mailto:{APP_EMAIL})'}
    headers = {'User-Agent': USER_AGENT_LIST[0]}

    try:
        send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'Запрос к {api_url}', progress_percent=20, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        response = requests.get(api_url, headers=headers, timeout=45)
        if response.status_code == 404:
            send_user_notification(user_id, task_id, query_display_name, 'NOT_FOUND', f'Статья не найдена в {current_api_name}.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            return {'status': 'not_found', 'message': f'Article not found in {current_api_name}.'}
        response.raise_for_status()
        api_data = response.json()
    except requests.exceptions.RequestException as exc:
        send_user_notification(user_id, task_id, query_display_name, 'RETRYING', f'Ошибка сети/API {current_api_name}: {str(exc)}. Повтор...', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        raise self.retry(exc=exc)
    except json.JSONDecodeError as json_exc:
        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Ошибка декодирования JSON от {current_api_name}: {str(json_exc)}', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'error', 'message': f'{current_api_name} JSON Decode Error: {str(json_exc)}'}

    if not api_data or isinstance(api_data, list): # OpenAlex возвращает один объект, не список для /works/{id}
        send_user_notification(user_id, task_id, query_display_name, 'NOT_FOUND', f'{current_api_name} API вернул неожиданный формат или пустой результат.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'not_found', 'message': f'{current_api_name} API returned unexpected format or empty result.'}

    send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'Данные {current_api_name} получены, обработка...', progress_percent=40, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

    try:
        with transaction.atomic():
            article_owner = None
            if user_id:
                try:
                    article_owner = User.objects.get(id=user_id)
                except User.DoesNotExist:
                    send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Пользователь ID {user_id} не найден.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                    return {'status': 'error', 'message': f'User with ID {user_id} not found.'}

            # Извлечение данных из ответа OpenAlex
            api_title = api_data.get('display_name') or api_data.get('title') # display_name предпочтительнее

            api_abstract = None
            abstract_inverted_index = api_data.get('abstract_inverted_index')
            if abstract_inverted_index:
                # Пытаемся получить длину абстракта из поля 'abstract_length', если оно есть,
                # или считаем по максимальному индексу + 1.
                # OpenAlex API должен предоставлять `counts_by_year` и другие мета, но не `abstract_length` напрямую в `work` объекте.
                # Длину нужно вычислять по самому инвертированному индексу, если это возможно.
                # Простой способ: найти максимальный индекс в значениях inverted_index.
                max_pos = -1
                if isinstance(abstract_inverted_index, dict):
                    for word_positions in abstract_inverted_index.values():
                        if isinstance(word_positions, list) and word_positions:
                            max_pos = max(max_pos, max(word_positions))
                abstract_len_calculated = max_pos + 1 if max_pos != -1 else 0

                if abstract_len_calculated > 0:
                    api_abstract = reconstruct_abstract_from_inverted_index(abstract_inverted_index, abstract_len_calculated)

            api_doi = api_data.get('doi') # URL вида https://doi.org/DOI
            if api_doi and api_doi.startswith('https://doi.org/'):
                api_doi = api_doi[len('https://doi.org/'):].lower()
            elif api_doi: # Если вдруг другой формат
                api_doi = api_doi.lower()

            api_ids = api_data.get('ids', {})
            api_pmid = api_ids.get('pmid')
            if api_pmid and api_pmid.startswith('https://pubmed.ncbi.nlm.nih.gov/'):
                api_pmid = api_pmid.split('/')[-1]

            api_pmcid = api_ids.get('pmcid') # Уже должен быть без префикса 'PMC' здесь
            if api_pmcid and api_pmcid.startswith('PMC'):
                api_pmcid = api_pmcid[3:]

            api_arxiv_id = None # OpenAlex может хранить arXiv ID в 'ids' или в 'locations'
            # Для простоты, пока не будем глубоко искать arXiv ID здесь.

            api_publication_date_str = api_data.get('publication_date')
            api_parsed_date = None
            if api_publication_date_str:
                try:
                    api_parsed_date = timezone.datetime.strptime(api_publication_date_str, '%Y-%m-%d').date()
                except ValueError:
                    pass

            api_journal_name = None
            host_venue = api_data.get('host_venue')
            if host_venue and isinstance(host_venue, dict):
                api_journal_name = host_venue.get('display_name') or host_venue.get('publisher')

            api_parsed_authors = parse_openalex_authors(api_data.get('authorships'))

            # --- Логика поиска или создания статьи ---
            article = None
            created = False

            if article_id_to_update:
                try:
                    article = Article.objects.select_for_update().get(id=article_id_to_update, user=article_owner)
                except Article.DoesNotExist:
                    pass # Продолжаем поиск по идентификаторам

            if not article and api_doi:
                try:
                    article = Article.objects.select_for_update().get(doi=api_doi)
                except Article.DoesNotExist:
                    pass
            if not article and api_pmid:
                try:
                    article = Article.objects.select_for_update().get(pubmed_id=api_pmid)
                except Article.DoesNotExist:
                    pass
            # Добавить поиск по arXiv ID если извлечем его

            if not article:
                if not article_owner:
                    send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Пользователь не указан для создания новой статьи {current_api_name}.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                    return {'status': 'error', 'message': f'User not specified for new {current_api_name} article.'}

                creation_kwargs = {'user': article_owner, 'title': api_title or f"Статья OpenAlex: {query_display_name}"}
                # Устанавливаем тот идентификатор, по которому мы ТОЧНО нашли эту статью в OpenAlex
                if identifier_type.upper() == 'DOI' and api_doi == identifier_value.lower():
                    creation_kwargs['doi'] = api_doi
                elif identifier_type.upper() == 'PMID' and api_pmid == identifier_value:
                    creation_kwargs['pubmed_id'] = api_pmid
                elif api_doi :
                    creation_kwargs['doi'] = api_doi # Если OpenAlex вернул DOI, он приоритетен
                elif api_pmid:
                    creation_kwargs['pubmed_id'] = api_pmid
                else: # Если нет ключевых идентификаторов для создания
                     send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'{current_api_name} не предоставил DOI/PMID для создания новой статьи.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                     return {'status': 'error', 'message': f'{current_api_name} did not provide DOI/PMID for new article.'}

                article = Article.objects.create(**creation_kwargs)
                created = True

            if not article:
                 send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Не удалось идентифицировать или создать запись для статьи {current_api_name}.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                 return {'status': 'error', 'message': f'Could not identify or create article entry for {current_api_name}.'}

            # --- Применение логики приоритетов ---
            priority_list = getattr(settings, 'API_SOURCE_OVERALL_PRIORITY', [])
            try:
                current_api_priority = priority_list.index(current_api_name)
            except ValueError:
                current_api_priority = float('inf')
            article_primary_source_priority = float('inf')
            if article.primary_source_api:
                try:
                    article_primary_source_priority = priority_list.index(article.primary_source_api)
                except ValueError:
                    pass
            can_fully_overwrite = (created or not article.primary_source_api or current_api_priority <= article_primary_source_priority)

            if can_fully_overwrite:
                article.primary_source_api = current_api_name

            if api_title and (can_fully_overwrite or not article.title):
                article.title = api_title
            if api_abstract and (can_fully_overwrite or not article.abstract):
                article.abstract = api_abstract
            if api_parsed_date and (can_fully_overwrite or not article.publication_date):
                article.publication_date = api_parsed_date
            if api_journal_name and (can_fully_overwrite or not article.journal_name):
                article.journal_name = api_journal_name

            if api_doi and not article.doi:
                article.doi = api_doi
            if api_pmid and not article.pubmed_id:
                article.pubmed_id = api_pmid
            # if api_arxiv_id and not article.arxiv_id: article.arxiv_id = api_arxiv_id (если будем извлекать)

            if api_parsed_authors:
                if can_fully_overwrite or not article.authors.exists():
                    article.articleauthororder_set.all().delete()
                    for order, author_obj in enumerate(api_parsed_authors):
                        ArticleAuthorOrder.objects.create(article=article, author=author_obj, order=order)

            # if api_abstract and (not article.cleaned_text_for_llm or len(api_abstract) > len(article.cleaned_text_for_llm or "") + 50): # Упрощенное сравнение
            #     article.cleaned_text_for_llm = api_abstract

            # OA информация от OpenAlex (если есть и Unpaywall еще не дал более точную)
            if api_data.get('open_access', {}).get('oa_url') and (not article.best_oa_url or can_fully_overwrite):
                article.best_oa_url = api_data['open_access']['oa_url']
            if api_data.get('open_access', {}).get('oa_status') and (not article.oa_status or can_fully_overwrite):
                article.oa_status = api_data['open_access']['oa_status']

            article.save()
            send_user_notification(user_id, task_id, query_display_name, 'PROGRESS', f'Статья {current_api_name} сохранена в БД.', progress_percent=70, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

            ArticleContent.objects.update_or_create(
                article=article, source_api_name=current_api_name, format_type='json_metadata',
                defaults={'content': json.dumps(api_data)}
            )

            if originating_reference_link_id and article:
                try:
                    ref_link = ReferenceLink.objects.get(id=originating_reference_link_id, source_article__user=article_owner)
                    ref_link.resolved_article = article
                    ref_link.status = ReferenceLink.StatusChoices.ARTICLE_LINKED
                    ref_link.save(update_fields=['resolved_article', 'status', 'updated_at'])
                    send_user_notification(user_id, task_id, query_display_name, 'INFO', f'Ссылка ID {ref_link.id} связана со статьей {current_api_name}.', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
                except ReferenceLink.DoesNotExist:
                    pass
                except Exception as e_ref:
                    send_user_notification(user_id, task_id, query_display_name, 'ERROR', f'Ошибка обновления ref_link для {current_api_name}: {str(e_ref)}', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)

            final_message = f'Статья {current_api_name} "{article.title[:30]}..." {"создана" if created else "обновлена"}.'
            send_user_notification(user_id, task_id, query_display_name, 'SUCCESS', final_message, progress_percent=100, article_id=article.id, created=created, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
            return {'status': 'success', 'message': final_message, 'identifier': query_display_name, 'article_id': article.id}

    except db_utils.OperationalError as exc_db:
        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', f'Операционная ошибка БД ({current_api_name}): {str(exc_db)}', source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        if 'database is locked' in str(exc_db).lower():
            raise self.retry(exc=exc_db, countdown=15 + self.request.retries * 10)
        return {'status': 'error', 'message': f'DB OperationalError: {str(exc_db)}'}
    except Exception as e:
        error_message_for_user = f'Внутренняя ошибка {current_api_name}: {type(e).__name__} - {str(e)}'
        self.update_state(state='FAILURE', meta={'identifier': query_display_name, 'error': error_message_for_user, 'traceback': self.request.exc_info if hasattr(self.request, 'exc_info') else str(e)})
        send_user_notification(user_id, task_id, query_display_name, 'FAILURE', error_message_for_user, source_api=current_api_name, originating_reference_link_id=originating_reference_link_id)
        return {'status': 'error', 'message': f'Внутренняя ошибка: {str(e)}', 'identifier': query_display_name}


@shared_task(bind=True, max_retries=2, default_retry_delay=300) # LLM запросы могут быть долгими
def analyze_segment_with_llm_task(self, analyzed_segment_id: int, user_id: int):
    task_id = self.request.id
    current_api_name = "LLM_Analysis"
    display_identifier = f"AnalyzedSegmentID:{analyzed_segment_id}"

    send_user_notification(user_id, task_id, display_identifier, 'PENDING', 'Начинаем LLM анализ сегмента...', progress_percent=0, source_api=current_api_name)

    try:
        segment = AnalyzedSegment.objects.select_related('article__user').prefetch_related('cited_references__resolved_article').get(id=analyzed_segment_id)
    except AnalyzedSegment.DoesNotExist:
        send_user_notification(user_id, task_id, display_identifier, 'FAILURE', 'Анализируемый сегмент не найден.', source_api=current_api_name)
        return {'status': 'error', 'message': 'AnalyzedSegment not found.'}

    if segment.article.user_id != user_id:
        send_user_notification(user_id, task_id, display_identifier, 'FAILURE', 'Нет прав для анализа этого сегмента.', source_api=current_api_name)
        return {'status': 'error', 'message': 'Permission denied for this segment.'}

    if not segment.segment_text:
        send_user_notification(user_id, task_id, display_identifier, 'FAILURE', 'Текст сегмента пуст, анализ невозможен.', source_api=current_api_name)
        return {'status': 'error', 'message': 'Segment text is empty.'}

    # # --- Подготовка данных для промпта ---
    # cited_references_info = []
    # for i, ref_link in enumerate(segment.cited_references.all()):
    #     ref_title = "N/A"
    #     ref_abstract = "N/A"
    #     if ref_link.resolved_article:
    #         ref_title = ref_link.resolved_article.title
    #         ref_abstract = ref_link.resolved_article.abstract or ref_link.resolved_article.cleaned_text_for_llm
    #         if ref_abstract and len(ref_abstract) > 500: # Ограничиваем длину
    #             ref_abstract = ref_abstract[:500] + "..."
    #     elif ref_link.manual_data_json and ref_link.manual_data_json.get('title'):
    #         ref_title = ref_link.manual_data_json.get('title')
    #     elif ref_link.raw_reference_text:
    #         ref_title = ref_link.raw_reference_text[:150]

    #     cited_references_info.append(f"Источник [{i+1}]:\nЗаголовок: {ref_title}\nРезюме/Абстракт: {ref_abstract}\n---")

    # cited_references_text = "\n".join(cited_references_info) if cited_references_info else "Информация о цитируемых источниках не предоставлена."

    # --- Подготовка данных для промпта ---
    cited_references_info = []
    for i, ref_link in enumerate(segment.cited_references.all()):
        ref_title = "N/A"
        ref_abstract = "N/A"
        if ref_link.resolved_article:
            ref_title = ref_link.resolved_article.title
            ref_abstract = ref_link.resolved_article.cleaned_text_for_llm or ref_link.resolved_article.abstract
            if ref_abstract and len(ref_abstract) > 500: # Ограничиваем длину
                ref_abstract = ref_abstract[:500] + "..."
        elif ref_link.manual_data_json and ref_link.manual_data_json.get('title'):
            ref_title = ref_link.manual_data_json.get('title')
        elif ref_link.raw_reference_text:
            ref_title = ref_link.raw_reference_text[:150]

        cited_references_info.append(f"Источник [{i+1}]:\nЗаголовок: {ref_title}\nРезюме/Абстракт: {ref_abstract}\n---")

    cited_references_text = "\n".join(cited_references_info) if cited_references_info else "Информация о цитируемых источниках не предоставлена."

    # Формирование промпта
    prompt = f"""Ты выступаешь в роли научного ассистента. Тебе дан текстовый сегмент из научной статьи и информация о цитируемых в нем (или релевантных для него) источниках.
    Твоя задача:
    1. Внимательно прочитать текстовый сегмент.
    2. Изучить предоставленную информацию о цитируемых источниках.
    3. Оценить, насколько утверждения, сделанные в текстовом сегменте, подтверждаются или соотносятся с информацией из этих источников.
    4. Сформировать краткий текстовый анализ (2-5 предложений), описывающий твою оценку.
    5. Дать числовую оценку уверенности в поддержке утверждений сегмента источниками по шкале от 1 (нет поддержки/противоречие) до 5 (полная и ясная поддержка).

    Текстовый сегмент для анализа:
    --- СЕГМЕНТ ---
    {segment.segment_text}
    --- КОНЕЦ СЕГМЕНТА ---

    Информация о цитируемых/релевантных источниках:
    {cited_references_text}
    --- КОНЕЦ ИНФОРМАЦИИ ОБ ИСТОЧНИКАХ ---

    Предоставь свой ответ строго в формате JSON со следующими ключами:
    "analysis_notes": "Твой текстовый анализ здесь.",
    "veracity_score": число от 1 до 5 (например, 3 или 4.5).
    """

    send_user_notification(user_id, task_id, display_identifier, 'PROGRESS', 'Отправка запроса к LLM...', progress_percent=30, source_api=current_api_name)

    llm_response_content = None
    llm_model_used = getattr(settings, 'OPENAI_DEFAULT_MODEL', 'gpt-4o-mini')

    try:
        if getattr(settings, 'LLM_PROVIDER_FOR_ANALYSIS', None) == "OpenAI" and settings.OPENAI_API_KEY:
            client = OpenAI(api_key=settings.OPENAI_API_KEY)
            # llm_model_used = "gpt-3.5-turbo" # или gpt-4o, gpt-4-turbo

            chat_completion = client.chat.completions.create(
                model=llm_model_used,
                messages=[
                    {"role": "system", "content": "Ты - внимательный научный ассистент, который анализирует текст и его источники и всегда отвечает в формате JSON."},
                    {"role": "user", "content": prompt}
                ],
                # temperature=0.3, # Более детерминированный ответ
                response_format={"type": "json_object"} # Если используете GPT-4 Turbo или новее с поддержкой JSON mode
            )
            raw_llm_output = chat_completion.choices[0].message.content
            print(f"***** raw_llm_output: {raw_llm_output}") # Для отладки
            try:
                llm_response_content = json.loads(raw_llm_output)
            except json.JSONDecodeError:
                json_match = re.search(r'\{.*\}', raw_llm_output, re.DOTALL)
                if json_match:
                    try:
                        llm_response_content = json.loads(json_match.group(0))
                    except json.JSONDecodeError:
                        llm_response_content = {"analysis_notes": f"LLM вернул текст, но не удалось извлечь JSON: {raw_llm_output}", "veracity_score": None}
                else:
                     llm_response_content = {"analysis_notes": f"LLM вернул не JSON ответ: {raw_llm_output}", "veracity_score": None}
        else:
            send_user_notification(user_id, task_id, display_identifier, 'WARNING', 'LLM не настроен. Используется заглушка.', source_api=current_api_name)
            time.sleep(2)
            llm_response_content = {
                "analysis_notes": "Заглушка: LLM анализ не выполнен, так как LLM не настроен.",
                "veracity_score": None
            }
            llm_model_used = "stub_model"

        if llm_response_content and isinstance(llm_response_content, dict):
            segment.llm_analysis_notes = llm_response_content.get("analysis_notes", "Нет текстового анализа от LLM.")
            score = llm_response_content.get("veracity_score")
            try:
                segment.llm_veracity_score = float(score) if score is not None else None
            except (ValueError, TypeError):
                segment.llm_veracity_score = None
            segment.llm_model_name = llm_model_used
            segment.prompt_used = prompt
            segment.save(update_fields=['llm_analysis_notes', 'llm_veracity_score', 'llm_model_name', 'prompt_used', 'updated_at'])

            send_user_notification(user_id, task_id, display_identifier, 'SUCCESS', 'LLM анализ сегмента успешно завершен.',
            progress_percent=100, source_api=current_api_name, analysis_data={'segment_id': segment.id, 'notes': segment.llm_analysis_notes, 'score': segment.llm_veracity_score, 'model': llm_model_used})
            return {'status': 'success', 'message': 'LLM analysis complete.', 'analyzed_segment_id': segment.id}
        else:
            send_user_notification(user_id, task_id, display_identifier, 'FAILURE', 'LLM вернул некорректный или пустой ответ.', source_api=current_api_name)
            # ... (сохранение ошибки в segment.llm_analysis_notes) ...
            return {'status': 'error', 'message': 'LLM returned invalid or empty response.'}

    except Exception as e:
        error_message_for_user = f'Ошибка во время LLM анализа: {type(e).__name__} - {str(e)}'
        # ... (сохранение ошибки в segment.llm_analysis_notes) ...
        send_user_notification(user_id, task_id, display_identifier, 'FAILURE', error_message_for_user, source_api=current_api_name, #originating_reference_link_id=originating_reference_link_id
        )
        return {'status': 'error', 'message': f'LLM analysis failed: {str(e)}'}


@shared_task(bind=True, max_retries=2, default_retry_delay=180)
def process_full_text_and_create_segments_task(self, article_id: int, user_id: int):
    """
    Обрабатывает полный текст статьи в JATS XML для автоматического создания
    анализируемых сегментов (AnalyzedSegment) и их связи с библиографическими ссылками.
    """
    task_id = self.request.id
    current_api_name = "SegmentLinker"
    display_identifier = f"ArticleID:{article_id}"

    send_user_notification(user_id, task_id, display_identifier, 'PENDING', 'Начинаем автоматическое связывание текста и ссылок...', source_api=current_api_name)

    try:
        article = Article.objects.get(id=article_id, user_id=user_id)
        # Находим JATS XML для этой статьи. Предпочитаем PMC, затем Rxiv, затем любой другой JATS.
        xml_content_entry = ArticleContent.objects.filter(
            article=article,
            format_type__in=['pmc_fulltext_xml', 'rxiv_jats_xml_fulltext', 'xml_jats_fulltext']
        ).order_by('-retrieved_at').first()

        if not xml_content_entry or not xml_content_entry.content:
            send_user_notification(user_id, task_id, display_identifier, 'INFO', 'Не найден полный текст JATS XML для создания сегментов.', source_api=current_api_name)
            return {'status': 'info', 'message': 'No JATS XML found for segmentation.'}

        xml_string = xml_content_entry.content

        # --- Этап А: Парсинг списка литературы и создание/обновление ReferenceLink ---
        send_user_notification(user_id, task_id, display_identifier, 'PROGRESS', 'Парсинг списка литературы из XML...', progress_percent=20, source_api=current_api_name)

        print('******** segments > parse_references_from_jats')
        parsed_references = parse_references_from_jats(xml_string)
        if not parsed_references:
            send_user_notification(user_id, task_id, display_identifier, 'WARNING', 'Не удалось извлечь ссылки из JATS XML. Связывание будет неполным.', source_api=current_api_name)

        for ref_data in parsed_references:
            if not ref_data.get('jats_ref_id'):
                continue # Пропускаем ссылки без внутреннего ID, их не связать

            defaults = {
                'raw_reference_text': ref_data.get('raw_text'),
                'manual_data_json': { # Сохраняем все, что распарсили
                    'jats_ref_id': ref_data.get('jats_ref_id'),
                    'title': ref_data.get('title'),
                    'year': ref_data.get('year'),
                    'doi_from_source': ref_data.get('doi')
                }
            }
            if ref_data.get('doi'):
                defaults['target_article_doi'] = ref_data['doi']
                defaults['status'] = ReferenceLink.StatusChoices.DOI_PROVIDED_NEEDS_LOOKUP
            else:
                defaults['status'] = ReferenceLink.StatusChoices.PENDING_DOI_INPUT

            # Ищем/создаем ссылку по ее JATS ID, который уникален в рамках статьи
            ReferenceLink.objects.update_or_create(
                source_article=article,
                manual_data_json__jats_ref_id=ref_data.get('jats_ref_id'),
                defaults=defaults
            )

        # --- Этап Б: Создание карты ссылок для быстрого сопоставления ---
        ref_map = {
            ref.manual_data_json['jats_ref_id']: ref
            for ref in ReferenceLink.objects.filter(source_article=article)
            if ref.manual_data_json and 'jats_ref_id' in ref.manual_data_json
        }

        # --- Этап В: Парсинг тела статьи, создание и связывание сегментов ---
        send_user_notification(user_id, task_id, display_identifier, 'PROGRESS', 'Анализ текста и создание сегментов...', progress_percent=50, source_api=current_api_name)

        # Удаляем существующие автоматически созданные сегменты, чтобы избежать дублей при перезапуске
        # Мы договорились, что у системных сегментов user=None
        AnalyzedSegment.objects.filter(article=article, user__isnull=True).delete()

        xml_string_no_ns = re.sub(r'\sxmlns="[^"]+"', '', xml_string, count=1)
        root = ET.fromstring(xml_string_no_ns)
        body_node = root.find('.//body')

        segments_created = 0
        if body_node is not None:
            # Итерация по секциям для определения section_key
            for sec_node in body_node.findall('.//sec'): # Ищем все секции рекурсивно
                sec_title_el = sec_node.find('./title')
                section_key = "".join(sec_title_el.itertext()).strip() if sec_title_el is not None else "Unnamed Section"

                for p_node in sec_node.findall('./p'): # Итерация по абзацам внутри секции
                    segment_text = "".join(p_node.itertext()).strip()
                    if not segment_text or len(segment_text) < 50: # Пропускаем очень короткие параграфы
                        continue

                    inline_xrefs = p_node.findall('.//xref[@ref-type="bibr"]')
                    cited_ref_links_for_segment = set() # Используем set для автоматического исключения дублей
                    inline_markers_in_segment = []

                    if inline_xrefs:
                        for xref in inline_xrefs:
                            # Атрибут `rid` может содержать несколько ID, разделенных пробелами (например, "CR1 CR5")
                            rids = xref.get('rid', '').split()
                            for rid in rids:
                                if rid in ref_map:
                                    cited_ref_links_for_segment.add(ref_map[rid])
                            if xref.text:
                                inline_markers_in_segment.append(xref.text.strip())

                    # Создаем сегмент. Он создается всегда, даже если в нем нет ссылок, так как он является логической частью текста.
                    segment = AnalyzedSegment.objects.create(
                        article=article,
                        section_key=section_key,
                        segment_text=segment_text,
                        inline_citation_markers=list(set(inline_markers_in_segment)) or None, # Сохраняем уникальные маркеры
                        user=None # Системное создание
                    )

                    # Устанавливаем M2M связь
                    if cited_ref_links_for_segment:
                        segment.cited_references.set(list(cited_ref_links_for_segment))

                    segments_created += 1

        send_user_notification(user_id, task_id, display_identifier, 'SUCCESS', f'Автоматическое связывание завершено. Создано {segments_created} сегментов.', progress_percent=100, source_api=current_api_name)
        return {'status': 'success', 'message': f'Created {segments_created} segments.'}

    except Article.DoesNotExist:
        send_user_notification(user_id, task_id, display_identifier, 'FAILURE', 'Статья для анализа не найдена.', source_api=current_api_name)
        return {'status': 'error', 'message': 'Article not found.'}
    except Exception as e:
        error_message_for_user = f'Ошибка при автоматическом связывании: {type(e).__name__} - {str(e)}'
        send_user_notification(user_id, task_id, display_identifier, 'FAILURE', error_message_for_user, source_api=current_api_name)
        self.update_state(state='FAILURE', meta={'identifier': display_identifier, 'error': error_message_for_user, 'traceback': self.request.exc_info if hasattr(self.request, 'exc_info') else str(e)})
        return {'status': 'error', 'message': error_message_for_user}