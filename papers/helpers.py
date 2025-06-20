from asgiref.sync import sync_to_async, async_to_sync
from channels.layers import get_channel_layer
import xml.etree.ElementTree as ET # Для парсинга XML
import re
import requests
import tempfile
import logging
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError

from .models import Article, Author, ArticleContent, ArticleAuthorOrder, ReferenceLink


logger = logging.getLogger(__name__)

# Пространства имен для arXiv Atom XML
ARXIV_NS = {'atom': 'http://www.w3.org/2005/Atom', 'arxiv': 'http://arxiv.org/schemas/atom'}


def send_user_notification(user_id, task_id, identifier_value, status, message, progress_percent=None, article_id=None,
    created=None, source_api=None, originating_reference_link_id=None, analysis_data=None):

    if not user_id:
        return
    channel_layer = get_channel_layer()
    group_name = f"user_{user_id}_notifications"

    payload = {
        'task_id': task_id,
        'identifier': str(identifier_value), # Убедимся, что это строка
        'status': status,
        'message': message,
        'source_api': source_api or 'N/A'
    }
    # Добавляем опциональные поля в payload, только если они переданы
    if progress_percent is not None:
        payload['progress_percent'] = progress_percent
    if article_id is not None:
        payload['article_id'] = article_id
    if created is not None:
        payload['created'] = created
    if originating_reference_link_id is not None:
        payload['originating_reference_link_id'] = originating_reference_link_id
    if analysis_data is not None:
        payload['analysis_data'] = analysis_data

    # channel_layer.group_send(group_name, {"type": "send.notification", "payload": payload})
    async_to_sync(channel_layer.group_send)(group_name, {"type": "send.notification", "payload": payload})


# # Синхронная версия send_user_notification для использования в Celery tasks
# def sync_send_user_notification(user_id, task_id, identifier_value, status, message, progress_percent=None, article_id=None,
#     created=None, source_api=None, originating_reference_link_id=None, analysis_data=None):

#     if not user_id:
#         return
#     channel_layer = get_channel_layer()
#     group_name = f"user_{user_id}_notifications"

#     payload = {
#         'task_id': task_id,
#         'identifier': str(identifier_value), # Убедимся, что это строка
#         'status': status,
#         'message': message,
#         'source_api': source_api or 'N/A'
#     }
#     # Добавляем опциональные поля в payload, только если они переданы
#     if progress_percent is not None:
#         payload['progress_percent'] = progress_percent
#     if article_id is not None:
#         payload['article_id'] = article_id
#     if created is not None:
#         payload['created'] = created
#     if originating_reference_link_id is not None:
#         payload['originating_reference_link_id'] = originating_reference_link_id
#     if analysis_data is not None:
#         payload['analysis_data'] = analysis_data

#     channel_layer.group_send(group_name, {"type": "send.notification", "payload": payload})


def parse_crossref_authors(authors_data):
    parsed_authors = []
    if not authors_data:
        return parsed_authors
    for author_info in authors_data:
        name_parts = []
        if author_info.get('given'):
            name_parts.append(author_info['given'])
        if author_info.get('family'):
            name_parts.append(author_info['family'])
        full_name = " ".join(name_parts).strip()
        if full_name:
            author, _ = Author.objects.get_or_create(full_name=full_name)
            parsed_authors.append({'author_obj': author, 'sequence': author_info.get('sequence', 'first')})
    parsed_authors.sort(key=lambda x: 0 if x['sequence'] == 'first' else 1)
    return [item['author_obj'] for item in parsed_authors]


def parse_europepmc_authors(authors_data_list):
    parsed_authors = []
    if not authors_data_list or not isinstance(authors_data_list, list):
        return parsed_authors
    for author_info_wrapper in authors_data_list:
        if isinstance(author_info_wrapper, dict) and 'author' in author_info_wrapper:
            for author_entry in author_info_wrapper['author']:
                if isinstance(author_entry, dict) and 'fullName' in author_entry:
                    full_name = author_entry['fullName'].strip()
                    if full_name:
                        author, _ = Author.objects.get_or_create(full_name=full_name)
                        parsed_authors.append(author)
    return parsed_authors


def parse_s2_authors(authors_data_list):
    parsed_authors = []
    if not authors_data_list or not isinstance(authors_data_list, list):
        return parsed_authors
    for author_info in authors_data_list:
        if isinstance(author_info, dict) and author_info.get('name'):
            full_name = author_info['name'].strip()
            if full_name:
                author, _ = Author.objects.get_or_create(full_name=full_name)
                parsed_authors.append(author)
    return parsed_authors


def parse_arxiv_authors(entry_element):
    parsed_authors = []
    for author_el in entry_element.findall('atom:author', ARXIV_NS):
        name_el = author_el.find('atom:name', ARXIV_NS)
        if name_el is not None and name_el.text:
            full_name = name_el.text.strip()
            if full_name:
                author, _ = Author.objects.get_or_create(full_name=full_name)
                parsed_authors.append(author)
    return parsed_authors


def parse_pubmed_authors(author_list_node):
    parsed_authors = []
    if author_list_node is None:
        return parsed_authors
    for author_node in author_list_node.findall('./Author'):
        last_name_el = author_node.find('./LastName')
        fore_name_el = author_node.find('./ForeName') or author_node.find('./Forename')
        name_parts = []
        if fore_name_el is not None and fore_name_el.text: name_parts.append(fore_name_el.text.strip())
        if last_name_el is not None and last_name_el.text: name_parts.append(last_name_el.text.strip())
        full_name = " ".join(name_parts).strip()
        if full_name:
            author, _ = Author.objects.get_or_create(full_name=full_name)
            parsed_authors.append(author)
    return parsed_authors


def parse_rxiv_authors(authors_data_list):
    parsed_authors = []
    if not authors_data_list or not isinstance(authors_data_list, list):
        return parsed_authors
    for author_info in authors_data_list:
        full_name = author_info.get('author_name','').strip()
        if full_name:
            if ',' in full_name:
                parts = [p.strip() for p in full_name.split(',', 1)]
                if len(parts) == 2:
                    full_name = f"{parts[1]} {parts[0]}"
            author, _ = Author.objects.get_or_create(full_name=full_name)
            parsed_authors.append(author)
    return parsed_authors


# def extract_text_from_jats_xml(xml_string: str, include_abstract: bool = False) -> str:
#     """
#     Улучшенное извлечение текста из JATS XML с сохранением структуры секций.
#     :param xml_string: Строка, содержащая JATS XML.
#     :param include_abstract_in_body: Следует ли включать текст из <abstract>
#                                      (если он есть внутри <article-meta>),
#                                      если основной <body> не найден или пуст.
#     :return: Извлеченный и отформатированный текст.
#     """
#     if not xml_string:
#         return ""

#     all_text_parts = []
#     try:
#         root = ET.fromstring(xml_string)

#         # Стандартные пространства имен в JATS (могут понадобиться, если теги с префиксами)
#         # ns = {'mml': 'http://www.w3.org/1998/Math/MathML',
#         #       'xlink': 'http://www.w3.org/1999/xlink',
#         #       # Добавить другие, если необходимо
#         #      }
#         # findall('.//atom:entry', ARXIV_NS) -> findall('.//mml:math', ns)

#         # 1. Заголовок статьи
#         article_title_el = root.find('.//front//article-meta//title-group//article-title')
#         if article_title_el is not None:
#             title_text = "".join(article_title_el.itertext()).strip()
#             if title_text:
#                 all_text_parts.append(f"--- TITLE ---\n{title_text}\n")

#         # 2. Абстракт
#         # Абстракты могут быть структурированы (с <sec> и <title>) или быть простым текстом.
#         # Мы будем собирать весь текст из тега <abstract>.
#         abstract_node = root.find('.//front//article-meta//abstract')
#         if abstract_node is not None:
#             abstract_parts = []
#             # Проверяем, есть ли у абстракта свой заголовок (некоторые JATS его имеют)
#             abstract_title_el = abstract_node.find('./title') # Прямой дочерний title
#             if abstract_title_el is not None and abstract_title_el.text:
#                 abstract_parts.append(f"{abstract_title_el.text.strip().upper()}:")

#             # Собираем текст из всех <p> и <sec> внутри абстракта
#             for child_p_or_sec in abstract_node.findall('./*'): # Ищем <p> или <sec>
#                 if child_p_or_sec.tag == 'p' or child_p_or_sec.tag == 'sec':
#                     section_text_content = "".join(child_p_or_sec.itertext()).strip()
#                     if section_text_content:
#                         abstract_parts.append(section_text_content)

#             if abstract_parts:
#                  all_text_parts.append(f"\n--- ABSTRACT ---\n" + "\n".join(abstract_parts) + "\n")
#             elif not abstract_parts and "".join(abstract_node.itertext()).strip(): # Если нет <p> или <sec>, но есть текст
#                  all_text_parts.append(f"\n--- ABSTRACT ---\n" + "".join(abstract_node.itertext()).strip() + "\n")

#         # 3. Основной текст из <body>
#         body_node = root.find('.//body')
#         if body_node is not None:
#             # Исключаемые типы секций (можно расширить)
#             excluded_sec_types = ['author-contribution', 'author-contributions', 'funding', 'conflicts-of-interest', 'ethics',
#                                   'acknowledgement', 'acknowledgments', 'supplementary-material', 'data-availability']

#             def process_section_node(section_node, level=1):
#                 sec_text_parts = []
#                 # Заголовок секции
#                 sec_title_el = section_node.find('./title')
#                 sec_type = section_node.get('sec-type', '').lower()

#                 if sec_type in excluded_sec_types:
#                     return "" # Пропускаем всю секцию

#                 if sec_title_el is not None and sec_title_el.text:
#                     title_str = "".join(sec_title_el.itertext()).strip()
#                     if title_str:
#                         sec_text_parts.append(f"\n--- {title_str.upper()} (SECTION LEVEL {level}) ---\n")

#                 # Текст из параграфов <p> и других текстовых блоков напрямую в секции
#                 # Пропускаем таблицы (table-wrap) и рисунки (fig) на этом этапе,
#                 # но можно добавить извлечение их заголовков (caption), если нужно.
#                 for child in section_node:
#                     if child.tag == 'p':
#                         paragraph_text = "".join(child.itertext()).strip()
#                         if paragraph_text:
#                             sec_text_parts.append(paragraph_text + "\n")
#                     elif child.tag == 'sec': # Рекурсивная обработка вложенных секций
#                         sec_text_parts.append(process_section_node(child, level + 1))
#                     # Можно добавить обработку других тегов, например, списков <ul> <ol> <li>
#                     # elif child.tag == 'table-wrap': ...
#                     # elif child.tag == 'fig': ...
#                 return "".join(sec_text_parts)

#             for section in body_node.findall('./sec'): # Секции первого уровня
#                 all_text_parts.append(process_section_node(section))

#             # Если в body нет <sec>, но есть <p> напрямую (менее структурированный JATS)
#             if not body_node.findall('./sec') and body_node.findall('./p'):
#                  all_text_parts.append(f"\n--- BODY CONTENT ---\n")
#                  for p_node in body_node.findall('./p'):
#                     paragraph_text = "".join(p_node.itertext()).strip()
#                     if paragraph_text:
#                         all_text_parts.append(paragraph_text + "\n")

#         # Если тело пустое, но есть абстракт и разрешено его использовать
#         elif not all_text_parts and include_abstract and abstract_node is not None:
#             # Эта логика уже покрыта выше, но если бы мы хотели использовать абстракт как body
#             pass

#         final_text = "".join(all_text_parts)
#         # Очистка
#         lines = [line.strip() for line in final_text.splitlines() if line.strip()]
#         # Удаляем дублирующиеся пустые строки, которые могли образоваться
#         cleaned_lines = []
#         for i, line in enumerate(lines):
#             if line.startswith("---") and i > 0 and lines[i-1].startswith("---") and not lines[i-1].strip().endswith("\n"):
#                 # Избегаем двух заголовков секций подряд без текста между ними
#                 if lines[i-1].strip() == line.strip(): continue # Полностью одинаковые заголовки

#             # Удаляем строки, состоящие только из декоративных элементов или коротких неинформативных фраз
#             if len(line) < 5 and not line.startswith("---"): # Пример: очень короткие строки, не заголовки
#                  if not any(c.isalnum() for c in line) : continue # если нет букв или цифр

#             cleaned_lines.append(line)

#         return "\n".join(cleaned_lines).strip()

#     except ET.ParseError as e:
#         print(f"JATS XML Parse Error: {e}")
#         try: # Fallback к простому извлечению текста
#             if xml_string: # Убедимся, что xml_string не None
#                 root_fallback = ET.fromstring(xml_string) # root может быть не определен, если ошибка была до него
#                 fallback_text_parts = [text.strip() for text in root_fallback.itertext() if text and text.strip() and len(text.strip()) > 1] # Игнорируем очень короткие текстовые ноды
#                 return "\n".join(fallback_text_parts)
#         except: return ""
#         return ""
#     except Exception as e_gen:
#         print(f"Generic error in JATS parsing: {e_gen}")
#         return ""


def extract_structured_text_from_jats(xml_string: str) -> dict:
    """
    Извлекает текст из JATS XML, структурируя его по основным научным секциям.
    Возвращает словарь: {'title': "...", 'abstract': "...", 'introduction': "...", ...}
    """
    if not xml_string:
        return {}

    sections = {
        "title": None,
        "abstract": None,
        "introduction": None,
        "methods": None,
        "results": None,
        "discussion": None,
        "conclusion": None,
        "other_sections": [], # Для нераспознанных, но помеченных как <sec>
        "full_body_fallback": None # Если секции не найдены, но есть <body>
    }

    try:
        root = ET.fromstring(xml_string)

        # Заголовок статьи
        article_title_el = root.find('.//front//article-meta//title-group//article-title')
        if article_title_el is not None:
            sections['title'] = "".join(article_title_el.itertext()).strip().replace('\n', ' ')

        # Абстракт
        abstract_node = root.find('.//front//article-meta//abstract')
        if abstract_node is not None:
            abstract_text_parts = []
            # Пропускаем заголовки типа "Abstract" внутри самого абстракта
            for child in abstract_node:
                if child.tag.lower() not in ['label', 'title'] or len("".join(child.itertext()).strip().split()) >= 5 :
                     abstract_text_parts.append("".join(child.itertext()).strip())
            sections['abstract'] = "\n\n".join(filter(None, abstract_text_parts))
            if not sections['abstract']: # Если нет дочерних тегов, берем весь текст абстракта
                sections['abstract'] = "".join(abstract_node.itertext()).strip().replace('\n', ' ')

        # Основной текст из <body>
        body_node = root.find('.//body')
        if body_node is not None:
            body_texts = [] # Собираем весь текст body для fallback

            for sec_node in body_node.findall('./sec'): # Ищем секции первого уровня
                sec_title_el = sec_node.find('./title')
                sec_title_text_raw = "".join(sec_title_el.itertext()).strip().lower() if sec_title_el is not None else ""

                current_sec_content_parts = []
                for p_node in sec_node.findall('.//p'): # Все параграфы внутри секции, включая вложенные <sec><p>
                    paragraph_text = "".join(p_node.itertext()).strip()
                    if paragraph_text:
                        current_sec_content_parts.append(paragraph_text)

                section_content = "\n\n".join(current_sec_content_parts)
                if not section_content:
                    continue # Пропускаем секции без текстового контента в <p>

                # Сопоставление с ключами IMRAD (можно улучшить регулярными выражениями или более сложной логикой)
                if 'introduction' in sec_title_text_raw or sec_node.get('sec-type') == 'intro':
                    sections['introduction'] = (sections['introduction'] + "\n\n" if sections['introduction'] else "") + section_content
                elif 'method' in sec_title_text_raw or 'material' in sec_title_text_raw or sec_node.get('sec-type') == 'methods':
                    sections['methods'] = (sections['methods'] + "\n\n" if sections['methods'] else "") + section_content
                elif 'result' in sec_title_text_raw or sec_node.get('sec-type') == 'results':
                    sections['results'] = (sections['results'] + "\n\n" if sections['results'] else "") + section_content
                elif 'discuss' in sec_title_text_raw or sec_node.get('sec-type') == 'discussion':
                    sections['discussion'] = (sections['discussion'] + "\n\n" if sections['discussion'] else "") + section_content
                elif 'conclu' in sec_title_text_raw or sec_node.get('sec-type') == 'conclusion': # concl, conclusion, conclusions
                    sections['conclusion'] = (sections['conclusion'] + "\n\n" if sections['conclusion'] else "") + section_content
                else:
                    sections['other_sections'].append({
                        'title': "".join(sec_title_el.itertext()).strip() if sec_title_el is not None else "Unnamed Section",
                        'text': section_content
                    })
                body_texts.append(section_content) # Добавляем в общий текст body

            if not sections['introduction'] and not sections['methods'] and not sections['results'] and body_texts: # Если не удалось распознать секции IMRAD
                sections['full_body_fallback'] = "\n\n".join(body_texts)

        # Очистка None значений
        return {k: v for k, v in sections.items() if v}

    except ET.ParseError as e:
        print(f"JATS XML Parse Error (structured): {e}")
    except Exception as e_gen:
        print(f"Generic error in structured JATS parsing: {e_gen}")
    return {} # Возвращаем пустой словарь в случае ошибки


# def extract_structured_text_from_bioc(bioc_data: list) -> dict:
#     """
#     Извлекает текст из BioC JSON, структурируя его по основным научным секциям.
#     Возвращает словарь: {'title': "...", 'abstract': "...", 'introduction': "...", ...}
#     """
#     if not bioc_data or not isinstance(bioc_data, list) or \
#        not bioc_data[0].get('documents') or not bioc_data[0]['documents'][0].get('passages'):
#         return {}

#     sections = {
#         "title": None, "abstract": None, "introduction": None, "methods": None,
#         "results": None, "discussion": None, "conclusion": None,
#         "other_sections": [], # Для нераспознанных, но имеющих заголовок секций
#         "full_body_fallback": None # Если секции IMRAD не найдены, но есть какой-то текст
#     }
#     document = bioc_data[0]['documents'][0]

#     # Переменные для сборки текста текущей секции
#     current_section_key_in_dict = None # Ключ в словаре `sections` (напр., 'introduction')
#     current_section_accumulated_texts = [] # Список строк текста для текущей секции

#     def flush_accumulated_texts_to_section():
#         nonlocal current_section_key_in_dict, current_section_accumulated_texts, sections
#         if current_section_accumulated_texts:
#             text_block = "\n\n".join(current_section_accumulated_texts).strip()
#             if text_block:
#                 if current_section_key_in_dict:
#                     if sections.get(current_section_key_in_dict): # Если уже есть текст в этой секции (например, из другого passage)
#                         sections[current_section_key_in_dict] += "\n\n" + text_block
#                     else:
#                         sections[current_section_key_in_dict] = text_block
#                 elif sections.get('full_body_fallback'): # Если секция не определена, но есть текст
#                      sections['full_body_fallback'] += "\n\n" + text_block
#                 else: # Если это первый текст без определенной секции
#                      sections['full_body_fallback'] = text_block

#         current_section_accumulated_texts = [] # Очищаем для следующей секции
#         # current_section_key_in_dict НЕ сбрасываем здесь, он меняется при нахождении нового заголовка

#     # Сначала извлечем основной заголовок и абстракт, т.к. они обычно идут первыми
#     for passage in document.get('passages', []):
#         passage_text = passage.get('text', "").strip()
#         if not passage_text: continue

#         infons = passage.get('infons', {})
#         passage_section_type = infons.get('section_type', '').upper()

#         if passage_section_type == 'TITLE' and not sections['title']: # Берем первый TITLE как заголовок статьи
#             sections['title'] = passage_text
#             # Не добавляем в current_section_accumulated_texts, т.к. это отдельное поле
#         elif passage_section_type == 'ABSTRACT':
#             if sections['abstract'] is None: sections['abstract'] = "" # Инициализируем, если это первый текст для абстракта
#             # Пропускаем заголовки типа "Abstract" внутри самого абстракта
#             if not (infons.get('type', '').lower().endswith('title') and len(passage_text.split()) < 3 and passage_text.lower() == 'abstract'):
#                 sections['abstract'] += ("\n\n" if sections['abstract'] else "") + passage_text

#     # Обработка остальных секций
#     # Карта типов секций BioC на наши ключи (можно расширить)
#     section_type_map = {
#         'INTRO': 'introduction', 'INTRODUCTION': 'introduction',
#         'METHODS': 'methods', 'MATERIAL AND METHODS': 'methods', 'METHODOLOGY': 'methods',
#         'RESULTS': 'results',
#         'DISCUSS': 'discussion', 'DISCUSSION AND CONCLUSION': 'discussion',
#         'CONCL': 'conclusion', 'CONCLUSIONS': 'conclusion',
#         'CASE': 'other_sections', # или 'case_report' если нужно отдельно
#         'BACKGROUND': 'introduction', # Часто фон это часть введения
#         'OBJECTIVE': 'introduction', # Цели тоже часто во введении
#         # 'SECTION': 'other_sections' # Общий тип, если не подошло другое
#     }
#     excluded_bioc_section_types = ['REF', 'FIG', 'TABLE', 'APPENDIX', 'COMP_INT', 'AUTH_CONT', 'ACK_FUND', 'FOOTNOTE', 'TITLE', 'ABSTRACT']

#     for passage in document.get('passages', []):
#         passage_text = passage.get('text', "").strip()
#         if not passage_text: continue

#         infons = passage.get('infons', {})
#         passage_section_type_raw = infons.get('section_type', '').upper() # Например, INTRO, METHODS
#         passage_content_type = infons.get('type', '').lower() # Например, title_1, paragraph

#         if passage_section_type_raw in excluded_bioc_section_types:
#             continue

#         is_title_passage = 'title' in passage_content_type and len(passage_text.split()) < 15 # Эвристика для заголовка

#         # Определяем, к какой нашей секции относится этот passage
#         target_section_key = section_type_map.get(passage_section_type_raw)

#         if is_title_passage: # Если это заголовок
#             flush_accumulated_texts_to_section() # Сохраняем накопленный текст предыдущей секции
#             current_section_key_in_dict = target_section_key # Устанавливаем ключ для следующего текста
#             if current_section_key_in_dict: # Если это известный нам тип секции IMRAD
#                 # Добавляем заголовок как часть текста секции, если он еще не был добавлен
#                 # (например, если для секции уже есть текст, а это подзаголовок)
#                 if sections.get(current_section_key_in_dict):
#                     current_section_accumulated_texts.append(f"\n--- {passage_text.upper()} ---")
#                 else: # Первый заголовок для этой секции
#                     current_section_accumulated_texts.append(f"--- {passage_text.upper()} ---")
#             else: # Неизвестный тип секции, но есть заголовок -> other_sections
#                 sections['other_sections'].append({'title': passage_text, 'text': ""}) # Запоминаем, текст добавится ниже
#                 current_section_key_in_dict = 'other_sections_last_entry' # Временный ключ

#         elif passage_text: # Если это обычный текст (параграф)
#             if current_section_key_in_dict == 'other_sections_last_entry':
#                 # Добавляем текст к последней добавленной "другой секции"
#                 if sections['other_sections']:
#                     # Убедимся, что текст не пустой перед добавлением \n\n
#                     if sections['other_sections'][-1]['text']:
#                          sections['other_sections'][-1]['text'] += "\n\n" + passage_text
#                     else:
#                          sections['other_sections'][-1]['text'] = passage_text
#             elif current_section_key_in_dict: # Добавляем к текущей известной секции
#                 current_section_accumulated_texts.append(passage_text)
#             else: # Текст без явно определенной секции (например, в начале body)
#                 if sections.get('full_body_fallback'):
#                     sections['full_body_fallback'] += "\n\n" + passage_text
#                 else:
#                     sections['full_body_fallback'] = passage_text

#     flush_accumulated_texts_to_section() # Сохраняем текст последней обрабатываемой секции

#     # Очистка от пустых строчек и None значений
#     final_sections = {}
#     for key, value in sections.items():
#         if isinstance(value, str) and value.strip():
#             final_sections[key] = "\n".join([line for line in value.splitlines() if line.strip()])
#         elif isinstance(value, list) and value: # для other_sections
#             cleaned_other_sections = []
#             for sec_item in value:
#                 if isinstance(sec_item, dict) and sec_item.get('text', '').strip():
#                     cleaned_text = "\n".join([line for line in sec_item['text'].splitlines() if line.strip()])
#                     if cleaned_text:
#                         cleaned_other_sections.append({'title': sec_item.get('title'), 'text': cleaned_text})
#             if cleaned_other_sections:
#                 final_sections[key] = cleaned_other_sections
#         elif value : # на случай если что-то не строка и не список, но не пустое
#             final_sections[key] = value

#     return final_sections


def reconstruct_abstract_from_inverted_index(inverted_index: dict, abstract_length: int) -> str | None:
    """
    Восстанавливает текст аннотации из инвертированного индекса OpenAlex.
    :param inverted_index: Словарь инвертированного индекса.
    :param abstract_length: Ожидаемая длина аннотации в словах (из поля abstract_length).
    :return: Восстановленная строка аннотации или None.
    """
    if not inverted_index or not isinstance(inverted_index, dict) or abstract_length == 0:
        return None

    # Создаем список слов нужной длины
    abstract_words = [""] * abstract_length
    found_words = 0
    for word, positions in inverted_index.items():
        for pos in positions:
            if 0 <= pos < abstract_length:
                abstract_words[pos] = word
                found_words +=1

    # Если мы не смогли восстановить значительную часть слов, возможно, что-то не так
    # (например, abstract_length было неверным или индекс неполный)
    # В этом случае лучше вернуть None, чем неполный текст.
    # Простая эвристика: если заполнено менее 70% слов, считаем неудачей.
    if found_words < abstract_length * 0.7 and abstract_length > 10 : # Пропускаем проверку для очень коротких "абстрактов"
        # print(f"Warning: Reconstructed abstract seems incomplete. Expected {abstract_length} words, got {found_words} words from index.")
        # Можно вернуть ' '.join(filter(None, abstract_words)).strip() если хотим частичный результат
        return None

    return ' '.join(filter(None, abstract_words)).strip() # filter(None,...) убирает пустые строки, если не все позиции были заполнены


def parse_openalex_authors(authorships_data: list) -> list:
    """Парсинг авторов из поля 'authorships' ответа OpenAlex API."""
    parsed_authors = []
    if not authorships_data or not isinstance(authorships_data, list):
        return parsed_authors

    # authorships обычно уже отсортированы по author_position
    for authorship in authorships_data:
        if isinstance(authorship, dict):
            author_info = authorship.get('author')
            if isinstance(author_info, dict) and author_info.get('display_name'):
                full_name = author_info['display_name'].strip()
                if full_name:
                    author, _ = Author.objects.get_or_create(full_name=full_name)
                    parsed_authors.append(author)
    return parsed_authors


# def sanitize_for_json_serialization(data):
#     """
#     Рекурсивно очищает данные от объектов Ellipsis для безопасной JSON-сериализации.
#     Заменяет Ellipsis на None.
#     """
#     if isinstance(data, list):
#         return [sanitize_for_json_serialization(item) for item in data]
#     elif isinstance(data, dict):
#         return {k: sanitize_for_json_serialization(v) for k, v in data.items()}
#     elif data is Ellipsis: # Проверяем, является ли объект именно Ellipsis
#         return None  # Заменяем на None (JSON null) или можно на "..." (строку)
#     return data


def parse_references_from_jats(xml_string: str) -> list:
    """
    Извлекает и парсит список литературы из строки JATS XML.
    Возвращает список словарей, где каждый словарь - одна ссылка с ее метаданными
    и, что важно, с ее внутренним JATS ID (атрибут 'id' тега <ref>).
    """
    references = []
    if not xml_string:
        return references

    try:
        # Убираем default namespace для упрощения поиска через findall
        xml_string = re.sub(r'\sxmlns="[^"]+"', '', xml_string, count=1)
        root = ET.fromstring(xml_string)
        ref_list_node = root.find('.//ref-list')

        if ref_list_node is None:
            return references

        for ref_node in ref_list_node.findall('./ref'):
            # Извлекаем внутренний ID ссылки - ключ к сопоставлению
            jats_ref_id = ref_node.get('id')
            if not jats_ref_id:
                continue # Пропускаем ссылки без ID, их невозможно будет сопоставить с текстом

            ref_data = {
                'jats_ref_id': jats_ref_id,
                'doi': None,
                'title': None,
                'year': None,
                'raw_text': None,
                'authors_str': None,
                'journal_title': None,
            }

            citation_node = ref_node.find('./element-citation')

            if citation_node is None:
                citation_node = ref_node.find('./mixed-citation')

            if citation_node is None:
                citation_node = ref_node.find('./citation')

            if citation_node is not None:
                # Собираем полный текст цитаты из <mixed-citation> или формируем из <element-citation>
                ref_data['raw_text'] = " ".join(citation_node.itertext()).strip().replace('\n', ' ').replace('  ', ' ')

                # Извлекаем DOI
                doi_el = citation_node.find(".pub-id[@pub-id-type='doi']")
                if doi_el is not None and doi_el.text:
                    ref_data['doi'] = doi_el.text.strip().lower()

                # Извлекаем другие метаданные...
                title_el = citation_node.find('./article-title')
                if title_el is None:
                    title_el = citation_node.find('./chapter-title')
                if title_el is not None and title_el.text:
                    ref_data['title'] = "".join(title_el.itertext()).strip()

                year_el = citation_node.find('./year')
                if year_el is not None and year_el.text:
                    ref_data['year'] = year_el.text.strip()

                source_el = citation_node.find('./source')
                if source_el is not None:
                    ref_data['journal_title'] = source_el.text.strip()

                author_els = citation_node.findall('./string-name')
                if author_els is None:
                    author_els = citation_node.find('./person-group').findall('./name')
                if author_els is not None:
                    author_list = []
                    for author in author_els:
                        surname = None
                        given_names = None
                        full_name = ''
                        surname = author.find('./surname')
                        if surname is not None:
                            full_name += f'{surname.text.strip()}'
                        given_names = author.find('./given-names')
                        if given_names is not None:
                            full_name += f' {given_names.text.strip()}'
                        if full_name:
                            author_list.append(full_name)
                    if author_list:
                        ref_data['authors_str'] = ", ".join(author_list)

            references.append(ref_data)

    except Exception as e:
        print(f"Error during JATS reference parsing: {e}")
    return references


def download_pdf(pdf_url: str, identifier_value: str) -> str | None:
    file_content = None
    content_type = None
    response = None
    logger.info(f"*** Start Download PDF from URL: {pdf_url} for identifier: {identifier_value}")

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
            "Accept": "application/pdf,text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "DNT": "1",
            "Upgrade-Insecure-Requests": "1"
        }
        response = requests.get(pdf_url, timeout=60, headers=headers, stream=True, allow_redirects=True) # разрешены редиректы
        print(f'****** response: {response}')
        if response.status_code == 200:
            content_type = response.headers.get('content-type', '').lower()
            print(f'****** content_type: {content_type}')
            if 'application/pdf' in content_type:
                file_content = response.content
                logger.info(f"*** (REQUESTS) Download PDF from URL: {pdf_url} for identifier: {identifier_value}")
                # print(f'****** (REQUESTS) file_content: {file_content}')
    except Exception as e:
        logger.error(f"*** (REQUESTS) Error download PDF from URL: {pdf_url} for identifier: {identifier_value} \nErro msg: {e}")

    if not file_content:
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    # user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:100.0) Gecko/20100101 Firefox/100.0', # Firefox User-Agent
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36', # Chrome User-Agent
                    java_script_enabled=True,
                    viewport={'width': 1920, 'height': 1080},
                    locale='en-US',
                    # color_scheme='light', # Можно попробовать 'dark' или 'light'
                    # timezone_id='America/New_York', # Для большей маскировки
                    permissions=['geolocation'], # Явно запрещаем геолокацию, если не нужна
                    # bypass_csp=True, # Использовать с ОСТОРОЖНОСТЬЮ, может нарушить работу сайта или быть обнаруженным
                    accept_downloads=True,
                )
                # Дополнительные заголовки, которые могут помочь
                context.set_extra_http_headers({
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none", # или "cross-site", если переход с Unpaywall
                    "Sec-Fetch-User": "?1",
                    "Upgrade-Insecure-Requests": "1",
                    "DNT": "1", # Do Not Track
                })

                page = context.new_page()

                # Ждём загрузку после перехода
                with page.expect_download(timeout=60000) as download_info:
                    # page.goto(pubmed_pdf_url, wait_until='commit') # 'domcontentloaded'
                    page.goto(pdf_url, wait_until='load')
                    # page.wait_for_timeout(5000)
                download = download_info.value

                # file_content = download.read()
                with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                    download.save_as(tmp_file.name)
                    tmp_file.seek(0)
                    file_content = tmp_file.read()

                # page.wait_for_timeout(2000)
                logger.info(f"*** (PLAYWRIGHT) Download PDF from URL: {pdf_url} for identifier: {identifier_value}")
                browser.close()
        except Exception as e:
            logger.error(f"*** (PLAYWRIGHT) Error download PDF from URL: {pdf_url} for identifier: {identifier_value} \nErro msg: {e}")

    return file_content