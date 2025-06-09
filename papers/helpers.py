from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
import xml.etree.ElementTree as ET # Для парсинга XML
import re
from .models import Article, Author, ArticleContent, ArticleAuthorOrder, ReferenceLink

# Пространства имен для arXiv Atom XML
ARXIV_NS = {'atom': 'http://www.w3.org/2005/Atom', 'arxiv': 'http://arxiv.org/schemas/atom'}


# --- Хелпер-функции ---

# def send_user_notification(user_id, task_id, identifier_value, status, message, progress_percent=None, article_id=None, created=None, source_api=None, originating_reference_link_id=None):
#     if not user_id: return
#     channel_layer = get_channel_layer()
#     group_name = f"user_{user_id}_notifications"
#     payload = {
#         'task_id': task_id,
#         'identifier': str(identifier_value),
#         'status': status,
#         'message': message,
#         'source_api': source_api or 'N/A'
#     }
#     if progress_percent is not None: payload['progress_percent'] = progress_percent
#     if article_id is not None: payload['article_id'] = article_id
#     if created is not None: payload['created'] = created
#     if originating_reference_link_id is not None:
#         payload['originating_reference_link_id'] = originating_reference_link_id
#     async_to_sync(channel_layer.group_send)(group_name, {"type": "send.notification", "payload": payload})


def send_user_notification(user_id, task_id, identifier_value, status, message,
                           progress_percent=None, article_id=None, created=None,
                           source_api=None, originating_reference_link_id=None,
                           analysis_data=None):
    if not user_id: return
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
    if progress_percent is not None: payload['progress_percent'] = progress_percent
    if article_id is not None: payload['article_id'] = article_id
    if created is not None: payload['created'] = created
    if originating_reference_link_id is not None:
        payload['originating_reference_link_id'] = originating_reference_link_id
    if analysis_data is not None: # <--- ДОБАВЛЕНА ЛОГИКА ДЛЯ НОВОГО ПАРАМЕТРА
        payload['analysis_data'] = analysis_data

    async_to_sync(channel_layer.group_send)(group_name, {"type": "send.notification", "payload": payload})


def parse_crossref_authors(authors_data):
    parsed_authors = []
    if not authors_data: return parsed_authors
    for author_info in authors_data:
        name_parts = []
        if author_info.get('given'): name_parts.append(author_info['given'])
        if author_info.get('family'): name_parts.append(author_info['family'])
        full_name = " ".join(name_parts).strip()
        if full_name:
            author, _ = Author.objects.get_or_create(full_name=full_name)
            parsed_authors.append({'author_obj': author, 'sequence': author_info.get('sequence', 'first')})
    parsed_authors.sort(key=lambda x: 0 if x['sequence'] == 'first' else 1)
    return [item['author_obj'] for item in parsed_authors]

def parse_europepmc_authors(authors_data_list):
    parsed_authors = []
    if not authors_data_list or not isinstance(authors_data_list, list): return parsed_authors
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
    if not authors_data_list or not isinstance(authors_data_list, list): return parsed_authors
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
    if author_list_node is None: return parsed_authors
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
    if not authors_data_list or not isinstance(authors_data_list, list): return parsed_authors
    for author_info in authors_data_list:
        full_name = author_info.get('author_name','').strip()
        if full_name:
            if ',' in full_name:
                parts = [p.strip() for p in full_name.split(',', 1)]
                if len(parts) == 2: full_name = f"{parts[1]} {parts[0]}"
            author, _ = Author.objects.get_or_create(full_name=full_name)
            parsed_authors.append(author)
    return parsed_authors


# def extract_text_from_jats_xml(xml_string: str, include_abstract_in_body: bool = False) -> str:
def extract_text_from_jats_xml(xml_string: str, include_abstract: bool = False) -> str:
    """
    Улучшенное извлечение текста из JATS XML с сохранением структуры секций.
    :param xml_string: Строка, содержащая JATS XML.
    :param include_abstract_in_body: Следует ли включать текст из <abstract>
                                     (если он есть внутри <article-meta>),
                                     если основной <body> не найден или пуст.
    :return: Извлеченный и отформатированный текст.
    """
    if not xml_string:
        return ""

    all_text_parts = []
    try:
        root = ET.fromstring(xml_string)

        # Стандартные пространства имен в JATS (могут понадобиться, если теги с префиксами)
        # ns = {'mml': 'http://www.w3.org/1998/Math/MathML',
        #       'xlink': 'http://www.w3.org/1999/xlink',
        #       # Добавить другие, если необходимо
        #      }
        # findall('.//atom:entry', ARXIV_NS) -> findall('.//mml:math', ns)

        # 1. Заголовок статьи
        article_title_el = root.find('.//front//article-meta//title-group//article-title')
        if article_title_el is not None:
            title_text = "".join(article_title_el.itertext()).strip()
            if title_text:
                all_text_parts.append(f"--- TITLE ---\n{title_text}\n")

        # 2. Абстракт
        # Абстракты могут быть структурированы (с <sec> и <title>) или быть простым текстом.
        # Мы будем собирать весь текст из тега <abstract>.
        abstract_node = root.find('.//front//article-meta//abstract')
        if abstract_node is not None:
            abstract_parts = []
            # Проверяем, есть ли у абстракта свой заголовок (некоторые JATS его имеют)
            abstract_title_el = abstract_node.find('./title') # Прямой дочерний title
            if abstract_title_el is not None and abstract_title_el.text:
                abstract_parts.append(f"{abstract_title_el.text.strip().upper()}:")

            # Собираем текст из всех <p> и <sec> внутри абстракта
            for child_p_or_sec in abstract_node.findall('./*'): # Ищем <p> или <sec>
                if child_p_or_sec.tag == 'p' or child_p_or_sec.tag == 'sec':
                    section_text_content = "".join(child_p_or_sec.itertext()).strip()
                    if section_text_content:
                        abstract_parts.append(section_text_content)

            if abstract_parts:
                 all_text_parts.append(f"\n--- ABSTRACT ---\n" + "\n".join(abstract_parts) + "\n")
            elif not abstract_parts and "".join(abstract_node.itertext()).strip(): # Если нет <p> или <sec>, но есть текст
                 all_text_parts.append(f"\n--- ABSTRACT ---\n" + "".join(abstract_node.itertext()).strip() + "\n")

        # 3. Основной текст из <body>
        body_node = root.find('.//body')
        if body_node is not None:
            # Исключаемые типы секций (можно расширить)
            excluded_sec_types = ['author-contribution', 'author-contributions', 'funding', 'conflicts-of-interest', 'ethics',
                                  'acknowledgement', 'acknowledgments', 'supplementary-material', 'data-availability']

            def process_section_node(section_node, level=1):
                sec_text_parts = []
                # Заголовок секции
                sec_title_el = section_node.find('./title')
                sec_type = section_node.get('sec-type', '').lower()

                if sec_type in excluded_sec_types:
                    return "" # Пропускаем всю секцию

                if sec_title_el is not None and sec_title_el.text:
                    title_str = "".join(sec_title_el.itertext()).strip()
                    if title_str:
                        sec_text_parts.append(f"\n--- {title_str.upper()} (SECTION LEVEL {level}) ---\n")

                # Текст из параграфов <p> и других текстовых блоков напрямую в секции
                # Пропускаем таблицы (table-wrap) и рисунки (fig) на этом этапе,
                # но можно добавить извлечение их заголовков (caption), если нужно.
                for child in section_node:
                    if child.tag == 'p':
                        paragraph_text = "".join(child.itertext()).strip()
                        if paragraph_text:
                            sec_text_parts.append(paragraph_text + "\n")
                    elif child.tag == 'sec': # Рекурсивная обработка вложенных секций
                        sec_text_parts.append(process_section_node(child, level + 1))
                    # Можно добавить обработку других тегов, например, списков <ul> <ol> <li>
                    # elif child.tag == 'table-wrap': ...
                    # elif child.tag == 'fig': ...
                return "".join(sec_text_parts)

            for section in body_node.findall('./sec'): # Секции первого уровня
                all_text_parts.append(process_section_node(section))

            # Если в body нет <sec>, но есть <p> напрямую (менее структурированный JATS)
            if not body_node.findall('./sec') and body_node.findall('./p'):
                 all_text_parts.append(f"\n--- BODY CONTENT ---\n")
                 for p_node in body_node.findall('./p'):
                    paragraph_text = "".join(p_node.itertext()).strip()
                    if paragraph_text:
                        all_text_parts.append(paragraph_text + "\n")

        # Если тело пустое, но есть абстракт и разрешено его использовать
        elif not all_text_parts and include_abstract and abstract_node is not None:
            # Эта логика уже покрыта выше, но если бы мы хотели использовать абстракт как body
            pass

        final_text = "".join(all_text_parts)
        # Очистка
        lines = [line.strip() for line in final_text.splitlines() if line.strip()]
        # Удаляем дублирующиеся пустые строки, которые могли образоваться
        cleaned_lines = []
        for i, line in enumerate(lines):
            if line.startswith("---") and i > 0 and lines[i-1].startswith("---") and not lines[i-1].strip().endswith("\n"):
                # Избегаем двух заголовков секций подряд без текста между ними
                if lines[i-1].strip() == line.strip(): continue # Полностью одинаковые заголовки

            # Удаляем строки, состоящие только из декоративных элементов или коротких неинформативных фраз
            if len(line) < 5 and not line.startswith("---"): # Пример: очень короткие строки, не заголовки
                 if not any(c.isalnum() for c in line) : continue # если нет букв или цифр

            cleaned_lines.append(line)

        return "\n".join(cleaned_lines).strip()

    except ET.ParseError as e:
        print(f"JATS XML Parse Error: {e}")
        try: # Fallback к простому извлечению текста
            if xml_string: # Убедимся, что xml_string не None
                root_fallback = ET.fromstring(xml_string) # root может быть не определен, если ошибка была до него
                fallback_text_parts = [text.strip() for text in root_fallback.itertext() if text and text.strip() and len(text.strip()) > 1] # Игнорируем очень короткие текстовые ноды
                return "\n".join(fallback_text_parts)
        except: return ""
        return ""
    except Exception as e_gen:
        print(f"Generic error in JATS parsing: {e_gen}")
        return ""


def extract_structured_text_from_jats(xml_string: str) -> dict:
    """
    Извлекает текст из JATS XML, структурируя его по основным научным секциям.
    Возвращает словарь: {'title': "...", 'abstract': "...", 'introduction': "...", ...}
    """
    if not xml_string:
        return {}

    sections = {
        "title": None, "abstract": None, "introduction": None, "methods": None,
        "results": None, "discussion": None, "conclusion": None,
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
                if not section_content: continue # Пропускаем секции без текстового контента в <p>

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


def extract_text_from_bioc_json(bioc_data) -> str:
    if not bioc_data or not isinstance(bioc_data, list) or not bioc_data[0].get('documents'):
        return ""

    all_text_parts = []
    document = bioc_data[0]['documents'][0]

    # Заголовок статьи (обычно первый TITLE)
    title_found = False
    for passage in document.get('passages', []):
        passage_infons = passage.get('infons', {})
        passage_text = passage.get('text', "").strip()
        if passage_infons.get('section_type', '').upper() == 'TITLE' and passage_text and not title_found:
            all_text_parts.append(f"--- TITLE ---\n{passage_text}\n")
            title_found = True # Берем только первый основной заголовок
            break
            # Или, если есть 'type': 'title', можно его использовать

    # Абстракт
    abstract_texts = []
    for passage in document.get('passages', []):
        passage_infons = passage.get('infons', {})
        passage_text = passage.get('text', "").strip()
        passage_section_type = passage_infons.get('section_type', '').upper()

        if passage_section_type == 'ABSTRACT' and passage_text:
            # Избегаем печати самого слова "Abstract" если оно идет как отдельный passage с типом заголовка
            if not (passage_infons.get('type', '').lower().endswith('title') and len(passage_text.split()) < 3 and passage_text.lower() == 'abstract'):
                abstract_texts.append(passage_text)

    if abstract_texts:
        all_text_parts.append(f"\n--- ABSTRACT ---\n" + "\n\n".join(abstract_texts) + "\n") # Разделяем параграфы абстракта двойным переводом строки

    # Основные секции
    # Список желаемых секций и их возможные представления в BioC `section_type`
    # (может потребоваться адаптация на основе реальных данных)
    section_map = {
        'INTRO': 'INTRODUCTION',
        'METHODS': 'METHODS', # или 'MATERIAL AND METHODS', 'METHODOLOGY'
        'RESULTS': 'RESULTS',
        'DISCUSS': 'DISCUSSION', # или 'DISCUSSION AND CONCLUSION'
        'CONCL': 'CONCLUSION',   # или 'CONCLUSIONS'
        'CASE': 'CASE REPORT', # или 'CASE PRESENTATION'
        # Другие возможные секции
        'BACKGROUND': 'BACKGROUND',
        'OBJECTIVE': 'OBJECTIVE',
        'SECTION': 'SECTION' # Общий тип секции, если другие не подошли
    }
    # Секции, которые мы обычно хотим пропустить
    excluded_section_types_bioc = ['REF', 'FIG', 'TABLE', 'APPENDIX', 'COMP_INT', 'AUTH_CONT', 'ACK_FUND']


    current_section_name_for_texts = None
    temp_section_texts = []

    for passage in document.get('passages', []):
        passage_infons = passage.get('infons', {})
        passage_text = passage.get('text', "").strip()
        passage_section_type_raw = passage_infons.get('section_type', '').upper()
        passage_content_type = passage_infons.get('type', '').lower()

        if not passage_text or passage_section_type_raw in excluded_section_types_bioc:
            continue # Пропускаем пустые или ненужные секции

        # Определяем, является ли текущий passage заголовком секции
        is_title = 'title' in passage_content_type and len(passage_text.split()) < 15 # Эвристика

        # Если это заголовок новой релевантной секции
        if is_title and passage_section_type_raw in section_map:
            if temp_section_texts: # Сохраняем текст предыдущей секции
                all_text_parts.append("\n".join(temp_section_texts) + "\n")
                temp_section_texts = []

            current_section_name_for_texts = section_map[passage_section_type_raw]
            all_text_parts.append(f"\n--- {passage_text.upper()} ---\n") # Используем текст из заголовка
        elif passage_section_type_raw in section_map or \
             (current_section_name_for_texts and passage_section_type_raw == 'UNKNOWN'): # UNKNOWN может быть параграфом в текущей секции
            # Если это параграф текущей или новой релевантной секции
            if not current_section_name_for_texts and passage_section_type_raw in section_map : # Первый параграф новой секции без явного заголовка
                 current_section_name_for_texts = section_map[passage_section_type_raw]
                 all_text_parts.append(f"\n--- {current_section_name_for_texts} ---\n")
            temp_section_texts.append(passage_text)
        elif not is_title and passage_section_type_raw not in ['TITLE', 'ABSTRACT']: # Текст без явного типа секции, но не заголовок/абстракт
             # Если предыдущая секция была определена, добавляем к ней
             if current_section_name_for_texts:
                 temp_section_texts.append(passage_text)
             else: # Иначе добавляем как есть, возможно, это часть введения или другого блока
                 all_text_parts.append(passage_text + "\n")


    if temp_section_texts: # Добавляем остатки текста из последней обработанной секции
        all_text_parts.append("\n".join(temp_section_texts) + "\n")


    final_text = "".join(all_text_parts)
    final_text = "\n".join([line.strip() for line in final_text.splitlines() if line.strip()])
    return final_text.strip()


# def extract_structured_text_from_bioc(bioc_data: list) -> dict:
#     """
#     Извлекает текст из BioC JSON, структурируя его по основным научным секциям.
#     Возвращает словарь: {'title': "...", 'abstract': "...", 'introduction': "...", ...}
#     """
#     if not bioc_data or not isinstance(bioc_data, list) or not bioc_data[0].get('documents'):
#         return {}

#     sections = {
#         "title": None, "abstract": None, "introduction": None, "methods": None,
#         "results": None, "discussion": None, "conclusion": None,
#         "other_sections": [],
#         "full_body_fallback": None
#     }
#     document = bioc_data[0]['documents'][0]

#     current_section_key = None
#     current_section_texts = []

#     def flush_current_section_texts():
#         nonlocal current_section_key, current_section_texts
#         if current_section_key and temp_texts:
#             existing_text = sections.get(current_section_key)
#             new_text_block = "\n\n".join(temp_texts)
#             sections[current_section_key] = (existing_text + "\n\n" + new_text_block if existing_text else new_text_block).strip()
#         elif temp_texts: # Если секция не определена, но есть текст
#             sections['other_sections'].append({'title': 'Unknown Section Block', 'text': "\n\n".join(temp_texts)})
#         current_section_texts = []
#         # current_section_key не сбрасываем, если следующий параграф относится к той же секции

#     for passage in document.get('passages', []):
#         passage_text = passage.get('text', "").strip()
#         if not passage_text: continue

#         infons = passage.get('infons', {})
#         section_type_raw = infons.get('section_type', '').upper()
#         content_type = infons.get('type', '').lower()

#         # Исключаемые типы секций
#         excluded_bioc_types = ['REF', 'FIG', 'TABLE', 'APPENDIX', 'COMP_INT', 'AUTH_CONT', 'ACK_FUND', 'FOOTNOTE']
#         if section_type_raw in excluded_bioc_types:
#             continue

#         new_section_key = None
#         is_title_passage = 'title' in content_type and len(passage_text.split()) < 15

#         if section_type_raw == 'TITLE': new_section_key = 'title'
#         elif section_type_raw == 'ABSTRACT': new_section_key = 'abstract'
#         elif section_type_raw == 'INTRO' or 'introduction' in passage_text.lower() and is_title_passage : new_section_key = 'introduction'
#         elif section_type_raw == 'METHODS' or ('method' in passage_text.lower() or 'material' in passage_text.lower()) and is_title_passage: new_section_key = 'methods'
#         elif section_type_raw == 'RESULTS' or 'result' in passage_text.lower() and is_title_passage: new_section_key = 'results'
#         elif section_type_raw == 'DISCUSS' or 'discussion' in passage_text.lower() and is_title_passage: new_section_key = 'discussion'
#         elif section_type_raw == 'CONCL' or 'conclu' in passage_text.lower() and is_title_passage: new_section_key = 'conclusion'
#         elif section_type_raw == 'SECTION' and is_title_passage: # Общая секция с заголовком
#              # Пытаемся угадать по заголовку
#             pt_lower = passage_text.lower()
#             if 'introduction' in pt_lower: new_section_key = 'introduction'
#             elif 'method' in pt_lower or 'material' in pt_lower: new_section_key = 'methods'
#             elif 'result' in pt_lower: new_section_key = 'results'
#             elif 'discuss' in pt_lower: new_section_key = 'discussion'
#             elif 'conclu' in pt_lower: new_section_key = 'conclusion'


#         if new_section_key:
#             if new_section_key == current_section_key and is_title_passage : # Это может быть подзаголовок внутри той же секции
#                  if temp_section_texts: temp_section_texts.append("") # Пустая строка для разделения
#                  temp_section_texts.append(f"--- {passage_text} ---") # Добавляем как подзаголовок
#             elif new_section_key != current_section_key :
#                 flush_current_section_texts() # Сохраняем предыдущую секцию
#                 current_section_key = new_section_key
#                 if not is_title_passage and new_section_key not in ['title', 'abstract']: # Если это не заголовок, а первый параграф новой секции
#                      temp_section_texts.append(passage_text)
#                 elif is_title_passage and new_section_key not in ['title', 'abstract']: # Если это заголовок секции (кроме title/abstract)
#                      pass # Заголовок уже будет обработан и добавлен в all_text_parts, здесь просто меняем current_section_key
#         elif passage_text: # Это не заголовок, а обычный текст
#             temp_section_texts.append(passage_text)

#     flush_current_section_texts() # Сохраняем текст последней секции

#     # Формируем full_body_fallback из всех текстовых частей, если основные секции не найдены
#     imrad_found = any(sections.get(key) for key in ['introduction', 'methods', 'results', 'discussion', 'conclusion'])
#     if not imrad_found:
#         all_body_passages_text = []
#         for passage in document.get('passages', []):
#             passage_text = passage.get('text', "").strip()
#             section_type_raw = passage.get('infons', {}).get('section_type', '').upper()
#             if passage_text and section_type_raw not in ['TITLE', 'ABSTRACT'] + excluded_bioc_types:
#                 all_body_passages_text.append(passage_text)
#         if all_body_passages_text:
#             sections['full_body_fallback'] = "\n\n".join(all_body_passages_text)

#     return {k: v for k, v in sections.items() if v} # Очистка None значений

# papers/tasks.py
# ... (все существующие импорты, константы, хелперы, другие задачи) ...


def extract_structured_text_from_bioc(bioc_data: list) -> dict:
    """
    Извлекает текст из BioC JSON, структурируя его по основным научным секциям.
    Возвращает словарь: {'title': "...", 'abstract': "...", 'introduction': "...", ...}
    """
    if not bioc_data or not isinstance(bioc_data, list) or \
       not bioc_data[0].get('documents') or not bioc_data[0]['documents'][0].get('passages'):
        return {}

    sections = {
        "title": None, "abstract": None, "introduction": None, "methods": None,
        "results": None, "discussion": None, "conclusion": None,
        "other_sections": [], # Для нераспознанных, но имеющих заголовок секций
        "full_body_fallback": None # Если секции IMRAD не найдены, но есть какой-то текст
    }
    document = bioc_data[0]['documents'][0]

    # Переменные для сборки текста текущей секции
    current_section_key_in_dict = None # Ключ в словаре `sections` (напр., 'introduction')
    current_section_accumulated_texts = [] # Список строк текста для текущей секции

    def flush_accumulated_texts_to_section():
        nonlocal current_section_key_in_dict, current_section_accumulated_texts, sections
        if current_section_accumulated_texts:
            text_block = "\n\n".join(current_section_accumulated_texts).strip()
            if text_block:
                if current_section_key_in_dict:
                    if sections.get(current_section_key_in_dict): # Если уже есть текст в этой секции (например, из другого passage)
                        sections[current_section_key_in_dict] += "\n\n" + text_block
                    else:
                        sections[current_section_key_in_dict] = text_block
                elif sections.get('full_body_fallback'): # Если секция не определена, но есть текст
                     sections['full_body_fallback'] += "\n\n" + text_block
                else: # Если это первый текст без определенной секции
                     sections['full_body_fallback'] = text_block

        current_section_accumulated_texts = [] # Очищаем для следующей секции
        # current_section_key_in_dict НЕ сбрасываем здесь, он меняется при нахождении нового заголовка

    # Сначала извлечем основной заголовок и абстракт, т.к. они обычно идут первыми
    for passage in document.get('passages', []):
        passage_text = passage.get('text', "").strip()
        if not passage_text: continue

        infons = passage.get('infons', {})
        passage_section_type = infons.get('section_type', '').upper()

        if passage_section_type == 'TITLE' and not sections['title']: # Берем первый TITLE как заголовок статьи
            sections['title'] = passage_text
            # Не добавляем в current_section_accumulated_texts, т.к. это отдельное поле
        elif passage_section_type == 'ABSTRACT':
            if sections['abstract'] is None: sections['abstract'] = "" # Инициализируем, если это первый текст для абстракта
            # Пропускаем заголовки типа "Abstract" внутри самого абстракта
            if not (infons.get('type', '').lower().endswith('title') and len(passage_text.split()) < 3 and passage_text.lower() == 'abstract'):
                sections['abstract'] += ("\n\n" if sections['abstract'] else "") + passage_text

    # Обработка остальных секций
    # Карта типов секций BioC на наши ключи (можно расширить)
    section_type_map = {
        'INTRO': 'introduction', 'INTRODUCTION': 'introduction',
        'METHODS': 'methods', 'MATERIAL AND METHODS': 'methods', 'METHODOLOGY': 'methods',
        'RESULTS': 'results',
        'DISCUSS': 'discussion', 'DISCUSSION AND CONCLUSION': 'discussion',
        'CONCL': 'conclusion', 'CONCLUSIONS': 'conclusion',
        'CASE': 'other_sections', # или 'case_report' если нужно отдельно
        'BACKGROUND': 'introduction', # Часто фон это часть введения
        'OBJECTIVE': 'introduction', # Цели тоже часто во введении
        # 'SECTION': 'other_sections' # Общий тип, если не подошло другое
    }
    excluded_bioc_section_types = ['REF', 'FIG', 'TABLE', 'APPENDIX', 'COMP_INT', 'AUTH_CONT', 'ACK_FUND', 'FOOTNOTE', 'TITLE', 'ABSTRACT']

    for passage in document.get('passages', []):
        passage_text = passage.get('text', "").strip()
        if not passage_text: continue

        infons = passage.get('infons', {})
        passage_section_type_raw = infons.get('section_type', '').upper() # Например, INTRO, METHODS
        passage_content_type = infons.get('type', '').lower() # Например, title_1, paragraph

        if passage_section_type_raw in excluded_bioc_section_types:
            continue

        is_title_passage = 'title' in passage_content_type and len(passage_text.split()) < 15 # Эвристика для заголовка

        # Определяем, к какой нашей секции относится этот passage
        target_section_key = section_type_map.get(passage_section_type_raw)

        if is_title_passage: # Если это заголовок
            flush_accumulated_texts_to_section() # Сохраняем накопленный текст предыдущей секции
            current_section_key_in_dict = target_section_key # Устанавливаем ключ для следующего текста
            if current_section_key_in_dict: # Если это известный нам тип секции IMRAD
                # Добавляем заголовок как часть текста секции, если он еще не был добавлен
                # (например, если для секции уже есть текст, а это подзаголовок)
                if sections.get(current_section_key_in_dict):
                    current_section_accumulated_texts.append(f"\n--- {passage_text.upper()} ---")
                else: # Первый заголовок для этой секции
                    current_section_accumulated_texts.append(f"--- {passage_text.upper()} ---")
            else: # Неизвестный тип секции, но есть заголовок -> other_sections
                sections['other_sections'].append({'title': passage_text, 'text': ""}) # Запоминаем, текст добавится ниже
                current_section_key_in_dict = 'other_sections_last_entry' # Временный ключ

        elif passage_text: # Если это обычный текст (параграф)
            if current_section_key_in_dict == 'other_sections_last_entry':
                # Добавляем текст к последней добавленной "другой секции"
                if sections['other_sections']:
                    # Убедимся, что текст не пустой перед добавлением \n\n
                    if sections['other_sections'][-1]['text']:
                         sections['other_sections'][-1]['text'] += "\n\n" + passage_text
                    else:
                         sections['other_sections'][-1]['text'] = passage_text
            elif current_section_key_in_dict: # Добавляем к текущей известной секции
                current_section_accumulated_texts.append(passage_text)
            else: # Текст без явно определенной секции (например, в начале body)
                if sections.get('full_body_fallback'):
                    sections['full_body_fallback'] += "\n\n" + passage_text
                else:
                    sections['full_body_fallback'] = passage_text

    flush_accumulated_texts_to_section() # Сохраняем текст последней обрабатываемой секции

    # Очистка от пустых строчек и None значений
    final_sections = {}
    for key, value in sections.items():
        if isinstance(value, str) and value.strip():
            final_sections[key] = "\n".join([line for line in value.splitlines() if line.strip()])
        elif isinstance(value, list) and value: # для other_sections
            cleaned_other_sections = []
            for sec_item in value:
                if isinstance(sec_item, dict) and sec_item.get('text', '').strip():
                    cleaned_text = "\n".join([line for line in sec_item['text'].splitlines() if line.strip()])
                    if cleaned_text:
                        cleaned_other_sections.append({'title': sec_item.get('title'), 'text': cleaned_text})
            if cleaned_other_sections:
                final_sections[key] = cleaned_other_sections
        elif value : # на случай если что-то не строка и не список, но не пустое
            final_sections[key] = value

    return final_sections


# def parse_references_from_jats(xml_string: str) -> list:
#     """
#     Извлекает список литературы из строки JATS XML.
#     Возвращает список словарей, где каждый словарь - одна ссылка.
#     """
#     references = []
#     if not xml_string:
#         return references

#     try:
#         root = ET.fromstring(xml_string)
#         # Ищем <ref-list> в <back> или просто в <article>
#         ref_list_node = root.find('.//back//ref-list') or root.find('.//ref-list')

#         if ref_list_node is None:
#             return references

#         for ref_node in ref_list_node.findall('./ref'):
#             ref_data = {'raw_text': None, 'doi': None, 'title': None, 'year': None, 'authors_str': None, 'journal_title': None}

#             # Пытаемся извлечь DOI
#             doi_el = ref_node.find(".//*pub-id[@pub-id-type='doi']") # Ищем в любых вложенных элементах
#             if doi_el is not None and doi_el.text:
#                 ref_data['doi'] = doi_el.text.strip().lower()

#             # Пытаемся извлечь структурированную цитату <element-citation> или <mixed-citation>
#             citation_node = ref_node.find('./element-citation') or ref_node.find('./mixed-citation')

#             if citation_node is not None:
#                 # Собираем весь текст из <mixed-citation> как raw_text, если это возможно
#                 if citation_node.tag == 'mixed-citation':
#                     ref_data['raw_text'] = "".join(citation_node.itertext()).strip().replace('\n', ' ').replace('  ', ' ')

#                 # Название статьи
#                 title_el = citation_node.find('./article-title') or citation_node.find('./chapter-title') or citation_node.find('./source') # <source> может быть названием книги или журнала
#                 if title_el is not None and title_el.text:
#                     ref_data['title'] = "".join(title_el.itertext()).strip()

#                 # Год
#                 year_el = citation_node.find('./year')
#                 if year_el is not None and year_el.text:
#                     ref_data['year'] = year_el.text.strip()

#                 # Название журнала (если не было в title_el как <source>)
#                 if not ref_data['journal_title'] and citation_node.tag == 'element-citation': # Для element-citation <source> часто журнал
#                     source_el = citation_node.find('./source')
#                     if source_el is not None and source_el.text:
#                         ref_data['journal_title'] = "".join(source_el.itertext()).strip()

#                 # Авторы (очень упрощенный сбор)
#                 authors = []
#                 person_group_node = citation_node.find('./person-group[@person-group-type="author"]')
#                 if person_group_node is not None:
#                     for name_node in person_group_node.findall('./name'):
#                         surname_el = name_node.find('./surname')
#                         given_names_el = name_node.find('./given-names')
#                         author_name_parts = []
#                         if given_names_el is not None and given_names_el.text: author_name_parts.append(given_names_el.text.strip())
#                         if surname_el is not None and surname_el.text: author_name_parts.append(surname_el.text.strip())
#                         if author_name_parts: authors.append(" ".join(author_name_parts))
#                     # Если нет <name>, но есть <string-name> (иногда в <collab>)
#                     for string_name_el in person_group_node.findall('./string-name') + person_group_node.findall('./collab'):
#                         if string_name_el.text: authors.append(string_name_el.text.strip())
#                 if authors:
#                     ref_data['authors_str'] = "; ".join(authors)

#                 # Если raw_text не был взят из mixed-citation, формируем его из того, что есть
#                 if not ref_data['raw_text']:
#                     raw_parts = []
#                     if ref_data['authors_str']: raw_parts.append(ref_data['authors_str'])
#                     if ref_data['year']: raw_parts.append(f"({ref_data['year']})")
#                     if ref_data['title']: raw_parts.append(ref_data['title'])
#                     if ref_data['journal_title']: raw_parts.append(f"In: {ref_data['journal_title']}")
#                     ref_data['raw_text'] = ". ".join(filter(None, raw_parts))

#             if ref_data['doi'] or ref_data['raw_text'] or ref_data['title']: # Добавляем, если есть хоть что-то
#                 references.append(ref_data)

#     except ET.ParseError as e:
#         print(f"JATS XML Parse Error during reference extraction: {e}")
#     except Exception as e_gen:
#         print(f"Generic error during JATS reference extraction: {e_gen}")
#     return references


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


def sanitize_for_json_serialization(data):
    """
    Рекурсивно очищает данные от объектов Ellipsis для безопасной JSON-сериализации.
    Заменяет Ellipsis на None.
    """
    if isinstance(data, list):
        return [sanitize_for_json_serialization(item) for item in data]
    elif isinstance(data, dict):
        return {k: sanitize_for_json_serialization(v) for k, v in data.items()}
    elif data is Ellipsis: # Проверяем, является ли объект именно Ellipsis
        return None  # Заменяем на None (JSON null) или можно на "..." (строку)
    return data


# def parse_references_from_jats(xml_string: str) -> list:
#     """
#     Извлекает и парсит список литературы из строки JATS XML.
#     Возвращает список словарей, где каждый словарь - одна ссылка с ее метаданными
#     и, что важно, с ее внутренним JATS ID (атрибут 'id' тега <ref>).
#     """
#     references = []
#     if not xml_string:
#         return references

#     try:
#         # Убираем default namespace для упрощения поиска через findall
#         # Это частая проблема при парсинге XML с пространствами имен
#         xml_string = re.sub(r'\sxmlns="[^"]+"', '', xml_string, count=1)
#         root = ET.fromstring(xml_string)
#         ref_list_node = root.find('.//ref-list')

#         if ref_list_node is None:
#             return references

#         for ref_node in ref_list_node.findall('./ref'):
#             # Извлекаем внутренний ID ссылки - ключ к сопоставлению
#             jats_ref_id = ref_node.get('id')
#             if not jats_ref_id:
#                 continue # Пропускаем ссылки без ID, так как мы не сможем их связать с текстом

#             ref_data = {'jats_ref_id': jats_ref_id, 'doi': None, 'title': None, 'year': None, 'raw_text': None}

#             citation_node = ref_node.find('./element-citation') or ref_node.find('./mixed-citation')
#             if citation_node is not None:
#                 # Извлекаем DOI
#                 doi_el = citation_node.find(".pub-id[@pub-id-type='doi']")
#                 if doi_el is not None and doi_el.text:
#                     ref_data['doi'] = doi_el.text.strip().lower()

#                 # Извлекаем Title, Year и другие метаданные, как в предыдущей версии функции
#                 title_el = citation_node.find('./article-title') or citation_node.find('./chapter-title') or citation_node.find('./source')
#                 if title_el is not None: ref_data['title'] = "".join(title_el.itertext()).strip()

#                 year_el = citation_node.find('./year')
#                 if year_el is not None and year_el.text: ref_data['year'] = year_el.text.strip()

#                 # Собираем полный текст цитаты
#                 ref_data['raw_text'] = "".join(citation_node.itertext()).strip().replace('\n', ' ').replace('  ', ' ')

#             references.append(ref_data)

#     except ET.ParseError as e:
#         print(f"JATS XML Parse Error during reference extraction: {e}")
#     except Exception as e_gen:
#         print(f"Generic error during JATS reference extraction: {e_gen}")
#     return references


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

            ref_data = {'jats_ref_id': jats_ref_id, 'doi': None, 'title': None, 'year': None, 'raw_text': None}

            citation_node = ref_node.find('./element-citation') or ref_node.find('./mixed-citation')

            if citation_node is not None:
                # Собираем полный текст цитаты из <mixed-citation> или формируем из <element-citation>
                ref_data['raw_text'] = " ".join(citation_node.itertext()).strip().replace('\n', ' ').replace('  ', ' ')

                # Извлекаем DOI
                doi_el = citation_node.find(".pub-id[@pub-id-type='doi']")
                if doi_el is not None and doi_el.text:
                    ref_data['doi'] = doi_el.text.strip().lower()

                # Извлекаем другие метаданные...
                title_el = citation_node.find('./article-title') or citation_node.find('./chapter-title')
                if title_el is not None and title_el.text:
                    ref_data['title'] = "".join(title_el.itertext()).strip()

                year_el = citation_node.find('./year')
                if year_el is not None and year_el.text:
                    ref_data['year'] = year_el.text.strip()

            references.append(ref_data)

    except Exception as e:
        print(f"Error during JATS reference parsing: {e}")
    return references