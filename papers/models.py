from django.db import models
from django.contrib.auth.models import User
from django.utils.translation import gettext_lazy as _


class Author(models.Model):
    """Модель для хранения информации об авторах статей."""
    full_name = models.CharField(_("Полное имя"), max_length=255, unique=True)
    # Можно добавить другие поля, например, ORCID, affiliations и т.д.

    def __str__(self):
        return self.full_name

    class Meta:
        verbose_name = _("Автор")
        verbose_name_plural = _("Авторы")
        ordering = ['full_name']


class Article(models.Model):
    """Основная модель для хранения научной статьи и ее метаданных."""
    user = models.ForeignKey(
        User,
        on_delete=models.CASCADE,
        verbose_name=_("Пользователь"),
        help_text=_("Пользователь, добавивший статью в систему")
    )
    title = models.TextField(_("Название статьи")) # TextField для очень длинных названий
    authors = models.ManyToManyField(
        Author,
        through='ArticleAuthorOrder', # Промежуточная модель для указания порядка авторов
        related_name='articles',
        verbose_name=_("Авторы")
    )
    abstract = models.TextField(_("Аннотация"), null=True, blank=True)

    # --- Идентификаторы ---
    doi = models.CharField(
        _("DOI"),
        max_length=255,
        unique=True, # DOI должен быть уникальным
        null=True, # Может отсутствовать на начальном этапе
        blank=True,
        db_index=True # Индексируем для быстрого поиска
    )
    pubmed_id = models.CharField(_("PubMed ID"), max_length=50, unique=True, null=True, blank=True, db_index=True)
    pmc_id = models.CharField(_("PubMed Central (PMC) ID"), max_length=50, unique=True, null=True, blank=True, db_index=True)
    arxiv_id = models.CharField(_("arXiv ID"), max_length=50, unique=True, null=True, blank=True, db_index=True)
    # Можно добавить другие идентификаторы по мере необходимости (PМCID, etc.)

    # --- Данные для LLM и ручного ввода ---
    cleaned_text_for_llm = models.TextField(
        _("Очищенный текст для LLM"),
        null=True,
        blank=True,
        help_text=_("Полный текст статьи, очищенный и подготовленный для анализа LLM. Может быть добавлен вручную.")
    )
    is_manually_added_full_text = models.BooleanField(
        _("Полный текст добавлен вручную"),
        default=False,
        help_text=_("Указывает, был ли полный текст статьи добавлен пользователем вручную.")
    )

    pdf_file = models.FileField(
        upload_to='articles_pdf/',
        verbose_name=_("PDF файл"),
        max_length=500,
        blank=True,
        null=True,
    )
    pdf_text = models.TextField(
        verbose_name=_("Текст из PDF"),
        help_text=_("Текстовое содержимое pdf файла полученное из MarkItDown, MarkerPDF или др."),
        null=True,
        blank=True,
    )

    # --- Источник основной записи ---
    primary_source_api = models.CharField(
        _("API основного источника"),
        max_length=100,
        null=True,
        blank=True,
        help_text=_("API, из которого были взяты основные метаданные для этой записи (title, abstract).")
    )

    # --- Метаданные публикации ---
    publication_date = models.DateField(_("Дата публикации"), null=True, blank=True)
    journal_name = models.CharField(_("Название журнала/источника"), max_length=512, null=True, blank=True)
    # Можно добавить volume, issue, pages etc.
    # --- Unpaywall OA Info ---
    oa_status = models.CharField(
        _("Статус Open Access"),
        max_length=50, null=True, blank=True,
        help_text=_("Статус Open Access от Unpaywall (e.g., gold, green, bronze, closed)")
    )
    best_oa_url = models.URLField(
        _("URL лучшей OA версии"),
        max_length=2048, null=True, blank=True, # URL могут быть длинными
        help_text=_("Ссылка на лучшую OA версию (HTML/лендинг) от Unpaywall")
    )
    best_oa_pdf_url = models.URLField(
        _("URL PDF лучшей OA версии"),
        max_length=2048, null=True, blank=True,
        help_text=_("Ссылка на PDF лучшей OA версии от Unpaywall")
    )
    oa_license = models.CharField(
        _("Лицензия OA версии"),
        max_length=100, null=True, blank=True,
        help_text=_("Лицензия OA версии от Unpaywall (e.g., cc-by, cc-by-nc)")
    )
    is_user_initiated = models.BooleanField(
        _("Добавлено пользователем напрямую"),
        default=False, # По умолчанию False. Будет True, только если пользователь сам инициировал добавление.
        db_index=True, # Индексируем для быстрого поиска основных статей пользователя
        help_text=_("True, если эта статья была добавлена пользователем напрямую, а не как связанная ссылка.")
    )
    # structured_content = models.JSONField(
    #     _("Структурированное содержимое"),
    #     null=True,
    #     blank=True,
    #     help_text=_("Содержимое статьи, разбитое по секциям (например, abstract, introduction, methods, results, discussion, conclusion)")
    # )
    structured_content = models.JSONField(
        _("Структурированное содержимое"),
        null=True,
        blank=True,
        default=dict, # default=dict, чтобы можно было сразу добавлять ключи
        help_text=_("Содержимое статьи, разбитое по секциям (например, abstract, introduction, methods, results, discussion, conclusion)")
    )
    # --- Временные метки ---
    created_at = models.DateTimeField(_("Дата создания"), auto_now_add=True)
    updated_at = models.DateTimeField(_("Дата обновления"), auto_now=True)

    class Meta:
        verbose_name = _("Научная статья")
        verbose_name_plural = _("Научные статьи")
        ordering = ['-updated_at', '-created_at']

    def regenerate_cleaned_text_from_structured(self):
        """
        Формирует cleaned_text_for_llm из structured_content.
        Секции добавляются в предопределенном порядке.
        """
        if not self.structured_content or not isinstance(self.structured_content, dict):
            # Если нет структурированного контента, используем абстракт (если есть) или оставляем пустым
            # self.cleaned_text_for_llm = self.abstract if self.abstract else ""
            return None

        # Если только 'title' и/или 'abstract' то не продолжаем
        structured_content_keys = list(self.structured_content.keys())
        for key in ['title', 'abstract']:
            if key in structured_content_keys:
                structured_content_keys.remove(key)
        if not structured_content_keys:
            return None

        ordered_keys = ['title', 'abstract', 'introduction', 'methods', 'results', 'discussion', 'conclusion']
        text_parts = []

        processed_keys = set()

        for key in ordered_keys:
            if self.structured_content.get(key):
                section_text = str(self.structured_content[key])
                if key == 'title':
                    title_marker = f"--- {key.upper()} ---"
                else:
                    title_marker = f"\n--- {key.upper()} ---"
                # Убираем дублирование заголовка, если он уже есть в тексте секции
                # if not section_text.strip().upper().startswith(title_marker):
                #    text_parts.append(title_marker)
                text_parts.append(title_marker)
                text_parts.append(section_text)
                processed_keys.add(key)

        # Добавляем "other_sections"
        other_sections_data = self.structured_content.get('other_sections')
        if isinstance(other_sections_data, list):
            for sec_item in other_sections_data:
                if isinstance(sec_item, dict):
                    title = sec_item.get('title', 'OTHER SECTION').upper()
                    text = sec_item.get('text', '')
                    if text:
                        text_parts.append(f"\n--- {title} ---")
                        text_parts.append(text)

        # Добавляем любые другие ключи из structured_content, которые не были обработаны
        # (кроме 'full_body_fallback', который мы используем ниже, если ничего другого нет)
        for key, value in self.structured_content.items():
            if key not in processed_keys and key not in ['other_sections', 'full_body_fallback'] and value:
                text_parts.append(f"\n--- {key.upper()} (CUSTOM) ---")
                text_parts.append(str(value))
                processed_keys.add(key)

        # if not text_parts and self.structured_content.get('full_body_fallback'):
        #     text_parts.append(self.structured_content['full_body_fallback'])

        self.cleaned_text_for_llm = "\n\n".join(filter(None, [tp.strip() for tp in text_parts])).strip()
        # if not self.cleaned_text_for_llm and self.abstract: # Если после всего текста нет, а абстракт есть
            # self.cleaned_text_for_llm = self.abstract

    def save(self, *args, **kwargs):
        # Автоматически регенерируем cleaned_text_for_llm, если structured_content изменился
        # или если cleaned_text_for_llm пуст, а structured_content есть.
        # Это можно сделать более избирательно, если отслеживать изменения structured_content.
        # Для простоты, пока будем делать это при каждом save, если structured_content есть.
        if self.structured_content:
            # Проверяем, изменилось ли structured_content (если объект уже в БД)
            if self.pk:
                try:
                    old_version = Article.objects.get(pk=self.pk)
                    if old_version.structured_content != self.structured_content or \
                        (not self.cleaned_text_for_llm and self.structured_content):
                            self.regenerate_cleaned_text_from_structured()
                except Article.DoesNotExist:
                    self.regenerate_cleaned_text_from_structured() # Для нового объекта
            else: # Новый объект
                self.regenerate_cleaned_text_from_structured()
        super().save(*args, **kwargs)

    def __str__(self):
        return self.title[:100] # Возвращаем первые 100 символов названия


class ArticleAuthorOrder(models.Model):
    """Промежуточная модель для связи Article и Author с указанием порядка."""
    article = models.ForeignKey(Article, on_delete=models.CASCADE)
    author = models.ForeignKey(Author, on_delete=models.CASCADE)
    order = models.PositiveIntegerField(_("Порядок"), default=0)

    class Meta:
        verbose_name = _("Порядок автора статьи")
        verbose_name_plural = _("Порядки авторов статей")
        ordering = ['order']
        unique_together = ('article', 'author') # Автор не может быть дважды в одной статье
        constraints = [
            models.UniqueConstraint(fields=['article', 'order'], name='unique_author_order_per_article')
        ]


class ArticleContent(models.Model):
    """Хранение "сырого" контента статьи из различных API и в различных форматах."""
    article = models.ForeignKey(
        Article,
        related_name='contents',
        on_delete=models.CASCADE,
        verbose_name=_("Статья")
    )
    source_api_name = models.CharField(
        _("Название API источника"),
        max_length=100,
        help_text=_("Например, 'pubmed', 'crossref_api', 'arxiv_api'")
    )
    format_type = models.CharField(
        _("Тип формата"),
        max_length=50,
        help_text=_("Например, 'json_metadata', 'xml_fulltext_jats', 'abstract_text', 'references_list_json'")
    )
    content = models.TextField(_("Содержимое")) # Используем TextField; для JSON можно использовать JSONField, если СУБД поддерживает
    retrieved_at = models.DateTimeField(_("Дата и время загрузки"), auto_now_add=True)

    def __str__(self):
        return f"{self.article.title[:30]}... - {self.source_api_name} ({self.format_type})"

    class Meta:
        verbose_name = _("Контент статьи из источника")
        verbose_name_plural = _("Контент статей из источников")
        unique_together = ('article', 'source_api_name', 'format_type') # Для одной статьи, один тип контента от одного API


class ReferenceLink(models.Model):
    """Модель для хранения ссылок (references) внутри статьи и их связи с другими статьями в БД."""

    class StatusChoices(models.TextChoices):
        PENDING_DOI_INPUT = 'pending_doi_input', _('Ожидает ввода/поиска DOI')
        DOI_LOOKUP_IN_PROGRESS = 'doi_lookup_in_progress', _('Идет поиск DOI для ссылки')
        DOI_PROVIDED_NEEDS_LOOKUP = 'doi_provided_needs_lookup', _('DOI найден, ожидает загрузки статьи')
        ARTICLE_FETCH_IN_PROGRESS = 'article_fetch_in_progress', _('Идет загрузка статьи по DOI')
        ARTICLE_LINKED = 'article_linked', _('Статья найдена и связана')
        ARTICLE_NOT_FOUND = 'article_not_found', _('Статья не найдена по DOI')
        MANUAL_ENTRY = 'manual_entry', _('Данные введены вручную')
        MANUAL_METADATA_ONLY = 'manual_metadata_only', _('Метаданные введены вручную (без связи)')
        ERROR_DOI_LOOKUP = 'error_doi_lookup', _('Ошибка при поиске DOI')
        ERROR_ARTICLE_FETCH = 'error_article_fetch', _('Ошибка при загрузке статьи')
        ERROR_PROCESSING = 'error_processing', _('Ошибка при обработке')

    source_article = models.ForeignKey(
        Article,
        related_name='references_made',
        on_delete=models.CASCADE,
        verbose_name=_("Исходная статья")
    )
    raw_reference_text = models.TextField(
        _("Исходный текст ссылки"),
        null=True,
        blank=True,
        help_text=_("Текст ссылки, как он представлен в исходной статье.")
    )
    target_article_doi = models.CharField(
        _("DOI цитируемой статьи"),
        max_length=255,
        null=True,
        blank=True,
        db_index=True,
        help_text=_("DOI статьи, на которую ссылаются. Может быть введен/отредактирован пользователем.")
    )
    resolved_article = models.ForeignKey(
        Article,
        related_name='cited_by_references',
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        verbose_name=_("Связанная статья в БД")
    )
    manual_data_json = models.JSONField(
        _("Данные, введенные вручную"),
        null=True,
        blank=True,
        help_text=_("JSON с метаданными цитируемой статьи, если она добавляется вручную (title, authors, year, etc.).")
    )
    status = models.CharField(
        _("Статус ссылки"),
        max_length=50, # Убедитесь, что длина достаточна для новых значений
        choices=StatusChoices.choices,
        default=StatusChoices.PENDING_DOI_INPUT
    )
    log_messages = models.TextField(
        _("Логи обработки"),
        null=True,
        blank=True,
        help_text=_("Сообщения о процессе поиска, загрузки, ошибках.")
    )
    created_at = models.DateTimeField(_("Дата создания"), auto_now_add=True)
    updated_at = models.DateTimeField(_("Дата обновления"), auto_now=True)

    order = models.PositiveIntegerField(
        default=0,
        blank=False,
        null=False,
    )

    class Meta:
        ordering = ['order'] # ordering = ['-created_at']
        verbose_name = _("Библиографическая ссылка")
        verbose_name_plural = _("Библиографические ссылки")

    def __str__(self):
        if self.resolved_article:
            return f"Ссылка из '{self.source_article.title[:20]}...' на '{self.resolved_article.title[:20]}...'"
        elif self.target_article_doi:
            return f"Из '{self.source_article.title[:20]}...' на DOI: {self.target_article_doi}"
        return f"Ссылка ID {self.id} из '{self.source_article.title[:20]}...'"


class AnalyzedSegment(models.Model):
    article = models.ForeignKey(
        Article,
        on_delete=models.CASCADE,
        related_name='analyzed_segments',
        verbose_name=_("Целевая статья")
    )
    # Ключ секции, из которой взят сегмент (из article.structured_content или заголовок из other_sections)
    section_key = models.CharField(
        _("Ключ/заголовок секции"),
        max_length=255,
        blank=True, null=True,
        help_text=_("Например, introduction, methods, или заголовок пользовательской секции.")
    )
    segment_text = models.TextField(
        _("Текст сегмента"),
        help_text=_("Выделенный пользователем или автоматически извлеченный фрагмент текста.")
    )
    # Ссылки, которые подтверждают/относятся к этому сегменту текста
    cited_references = models.ManyToManyField(
        ReferenceLink,
        related_name='supporting_segments',
        blank=True, # Сегмент может быть и без прямых ссылок
        verbose_name=_("Цитируемые/связанные ссылки")
    )
    # Текстовое представление инлайн-цитат, найденных в segment_text (опционально, для справки)
    inline_citation_markers = models.JSONField(
        _("Маркеры инлайн-цитат"),
        null=True, blank=True, default=list,
        help_text=_("Список текстовых маркеров цитирования, найденных в сегменте, например, ['[1]', '(Smith 2023)']")
    )
    # Поля для будущего анализа LLM
    llm_analysis_notes = models.TextField(_("Заметки анализа LLM"), null=True, blank=True)
    llm_veracity_score = models.FloatField(_("Оценка достоверности LLM"), null=True, blank=True)
    llm_model_name =  models.CharField(
        _("Название LLM модели"),
        max_length=100,
        help_text=_("Например: 'gpt-4o-mini'"),
        null=True,
        blank=True,
    )

    prompt_used = models.TextField(_("Использованный промпт"), null=True, blank=True)

    user = models.ForeignKey(
        User,
        on_delete=models.SET_NULL, # Если пользователя удалят, сегмент останется
        null=True, blank=True,     # Может быть создан системой или пользователем
        verbose_name=_("Пользователь, создавший/изменивший сегмент")
    )
    created_at = models.DateTimeField(_("Дата создания"), auto_now_add=True)
    updated_at = models.DateTimeField(_("Дата обновления"), auto_now=True)

    class Meta:
        verbose_name = _("Анализируемый сегмент текста")
        verbose_name_plural = _("Анализируемые сегменты текста")
        ordering = ['article', 'created_at']
        # Можно добавить unique_together, если, например, текст сегмента в рамках одной статьи и секции должен быть уникальным
        # unique_together = ('article', 'section_key', 'segment_text_hash') # (потребует поля хеша)

    def __str__(self):
        section_info = f"Секция: {self.section_key}" if self.section_key else "Общий текст"
        return f"Сегмент из '{self.article.title[:30]}...' ({section_info}): '{self.segment_text[:50]}...'"
