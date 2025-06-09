from rest_framework import serializers
from .models import Author, Article, ArticleAuthorOrder, ArticleContent, ReferenceLink, User, AnalyzedSegment


class UserSerializer(serializers.ModelSerializer):
    """Сериализатор для модели User (только для чтения)."""
    class Meta:
        model = User
        fields = ['id', 'username', 'first_name', 'last_name']


class AuthorSerializer(serializers.ModelSerializer):
    """Сеrialsizer для модели Author."""
    class Meta:
        model = Author
        fields = ['id', 'full_name']


class ArticleContentSerializer(serializers.ModelSerializer):
    """Сериализатор для модели ArticleContent."""
    class Meta:
        model = ArticleContent
        fields = ['id', 'article', 'source_api_name', 'format_type', 'content', 'retrieved_at']
        read_only_fields = ('retrieved_at',)


# class ReferenceLinkSerializer(serializers.ModelSerializer):
#     source_article_title = serializers.StringRelatedField(source='source_article.title', read_only=True) # Убрал source_article как ID
#     resolved_article_title = serializers.StringRelatedField(source='resolved_article.title', read_only=True)

#     class Meta:
#         model = ReferenceLink
#         fields = [
#             'id', 'source_article', 'source_article_title', 'raw_reference_text',
#             'target_article_doi',
#             'resolved_article', # resolved_article здесь read_only, т.к. связывается через Celery
#             'resolved_article_title',
#             'manual_data_json', 'status', 'log_messages', 'created_at', 'updated_at'
#         ]
#         read_only_fields = (
#             'created_at', 'updated_at',
#             'source_article_title', 'resolved_article_title',
#             'source_article', # Не даем менять исходную статью
#             'resolved_article', # resolved_article обновляется только системой
#             'log_messages'
#         )
#         # Поля, которые можно редактировать: raw_reference_text, target_article_doi, manual_data_json, status (если нужно)

#     def update(self, instance, validated_data):
#         # Проверяем, изменился ли target_article_doi
#         old_doi = instance.target_article_doi
#         new_doi = validated_data.get('target_article_doi', old_doi)

#         # Приводим DOI к нижнему регистру или None, если пустая строка
#         if new_doi is not None:
#             new_doi = new_doi.strip().lower()
#             if not new_doi: # Если после strip() осталась пустая строка
#                 new_doi = None

#         validated_data['target_article_doi'] = new_doi # Обновляем в validated_data для сохранения

#         if old_doi != new_doi: # Если DOI изменился (или был добавлен/удален)
#             instance.resolved_article = None # Сбрасываем связь со статьей
#             if new_doi: # Если новый DOI есть
#                 instance.status = ReferenceLink.StatusChoices.DOI_PROVIDED_NEEDS_LOOKUP
#             else: # Если DOI был удален (стал None)
#                 instance.status = ReferenceLink.StatusChoices.PENDING_DOI_INPUT

#         # Обновляем остальные поля стандартным образом
#         return super().update(instance, validated_data)

class ReferenceLinkSerializer(serializers.ModelSerializer):
    source_article_title = serializers.StringRelatedField(source='source_article.title', read_only=True)
    resolved_article_title = serializers.StringRelatedField(source='resolved_article.title', read_only=True)

    # --- ИЗМЕНЕНИЕ ---
    # Делаем source_article доступным для записи, чтобы создавать новые ссылки
    source_article = serializers.PrimaryKeyRelatedField(queryset=Article.objects.all())

    class Meta:
        model = ReferenceLink
        fields = [
            'id', 'source_article', 'source_article_title', 'raw_reference_text',
            'target_article_doi', 'resolved_article', 'resolved_article_title',
            'manual_data_json', 'status', 'log_messages', 'created_at', 'updated_at'
        ]
        read_only_fields = (
            'created_at', 'updated_at',
            'source_article_title', 'resolved_article_title',
            'resolved_article', # resolved_article по-прежнему обновляется только системой
            'log_messages'
        )

    def update(self, instance, validated_data):
        # Проверяем, изменился ли target_article_doi
        old_doi = instance.target_article_doi
        new_doi = validated_data.get('target_article_doi', old_doi)

        # Приводим DOI к нижнему регистру или None, если пустая строка
        if new_doi is not None:
            new_doi = new_doi.strip().lower()
            if not new_doi: # Если после strip() осталась пустая строка
                new_doi = None

        validated_data['target_article_doi'] = new_doi # Обновляем в validated_data для сохранения

        if old_doi != new_doi: # Если DOI изменился (или был добавлен/удален)
            instance.resolved_article = None # Сбрасываем связь со статьей
            if new_doi: # Если новый DOI есть
                instance.status = ReferenceLink.StatusChoices.DOI_PROVIDED_NEEDS_LOOKUP
            else: # Если DOI был удален (стал None)
                instance.status = ReferenceLink.StatusChoices.PENDING_DOI_INPUT

        # Обновляем остальные поля стандартным образом
        return super().update(instance, validated_data)


class ArticleAuthorOrderSerializer(serializers.ModelSerializer):
    """Сериализатор для отображения авторов с порядком в статье."""
    # author = AuthorSerializer(read_only=True) # Если хотим полную инфу об авторе
    author_id = serializers.PrimaryKeyRelatedField(queryset=Author.objects.all(), source='author')
    author_name = serializers.StringRelatedField(source='author.full_name', read_only=True)

    class Meta:
        model = ArticleAuthorOrder
        fields = ['author_id', 'author_name', 'order']


class ArticleSerializer(serializers.ModelSerializer):
    user = UserSerializer(read_only=True)
    user_id = serializers.PrimaryKeyRelatedField(queryset=User.objects.all(), source='user', write_only=True, required=False)
    article_authors = ArticleAuthorOrderSerializer(source='articleauthororder_set', many=True, required=False)
    contents = ArticleContentSerializer(many=True, read_only=True)
    references_made = ReferenceLinkSerializer(many=True, read_only=True)

    # structured_content по умолчанию будет доступен для записи, т.к. это JSONField
    # cleaned_text_for_llm будет только для чтения, т.к. генерируется на бэкенде

    class Meta:
        model = Article
        fields = [
            'id', 'user', 'user_id', 'title', 'abstract', 'doi', 'pubmed_id', 'arxiv_id',
            'cleaned_text_for_llm', 'is_manually_added_full_text',
            'primary_source_api', 'publication_date', 'journal_name',
            'oa_status', 'best_oa_url', 'best_oa_pdf_url', 'oa_license', # Поля Unpaywall
            'is_user_initiated', # Наше новое поле
            'structured_content', # Поле для структурированного текста
            'article_authors',
            'contents', 'references_made',
            'created_at', 'updated_at'
        ]
        read_only_fields = ('created_at', 'updated_at', 'user', 'cleaned_text_for_llm')

    def update(self, instance, validated_data):
        # Стандартное обновление полей, включая structured_content, если оно есть в validated_data
        # authors_data нужно обработать до super().update, если он не обрабатывает вложенные M2M по умолчанию
        authors_data = validated_data.pop('articleauthororder_set', None)

        # Обновляем поля, которые не являются M2M или специальными
        # structured_content будет обновлен через super().update()
        instance = super().update(instance, validated_data)

        # Обработка авторов (если были переданы)
        if authors_data is not None:
            instance.articleauthororder_set.all().delete()
            for author_data in authors_data:
                # Убедимся, что author_data['author'] - это объект Author
                author_instance = author_data.get('author')
                if isinstance(author_instance, Author):
                     ArticleAuthorOrder.objects.create(article=instance, author=author_instance, order=author_data.get('order',0))
                elif isinstance(author_instance, int): # Если передан ID
                     try:
                         author_obj = Author.objects.get(pk=author_instance)
                         ArticleAuthorOrder.objects.create(article=instance, author=author_obj, order=author_data.get('order',0))
                     except Author.DoesNotExist:
                         pass # Логировать ошибку или пропустить


        # Если structured_content был обновлен, модель сама вызовет regenerate_cleaned_text_from_structured() при save()
        # Однако, если save() в модели не переопределен или делает это не так, как нам нужно,
        # можно вызвать это явно здесь и сохранить еще раз только нужные поля.
        # Так как мы переопределили save() в модели, этот блок здесь может быть избыточен,
        # но для явности и если метод save() в модели не будет учитывать все случаи:

        # if 'structured_content' in validated_data:
        #     instance.regenerate_cleaned_text_from_structured()
        #     # Сохраняем только обновленный cleaned_text_for_llm и updated_at
        #     # instance.save(update_fields=['cleaned_text_for_llm', 'updated_at']) # Django сам обновит updated_at
        #     # Если regenerate_cleaned_text_from_structured не вызывает save, то нужен save здесь.
        #     # Но т.к. наш regenerate_cleaned_text_from_structured НЕ вызывает save, а только меняет поле,
        #     # и мы вызываем super().update() который делает save(), то этого достаточно, если
        #     # regenerate_cleaned_text_from_structured вызывается ПЕРЕД super().save().
        #     # Либо, как сейчас в модели, regenerate вызывается внутри save() модели.

        # Если метод save в модели Article корректно обрабатывает regenerate_cleaned_text_from_structured,
        # то здесь дополнительный save не нужен после super().update().

        return instance

    # def create(self, validated_data):
    #    # ... (существующий create) ...
    #    # При создании, is_user_initiated устанавливается в диспетчере,
    #    # а structured_content и cleaned_text_for_llm будут заполняться задачами.
    #    # Если нужно, чтобы cleaned_text_for_llm сразу генерировался из structured_content при создании через API:
    #    instance = super().create(validated_data)
    #    if instance.structured_content:
    #        instance.regenerate_cleaned_text_from_structured()
    #        instance.save(update_fields=['cleaned_text_for_llm'])
    #    return instance


class AnalyzedSegmentSerializer(serializers.ModelSerializer):
    user = UserSerializer(read_only=True) # Отображаем пользователя, но устанавливаем его в perform_create
    # Для cited_references мы хотим принимать список ID при создании/обновлении
    cited_references = serializers.PrimaryKeyRelatedField(
        queryset=ReferenceLink.objects.all(),
        many=True,
        required=False # Сегмент может не иметь ссылок
    )
    # article также должен быть PrimaryKeyRelatedField при создании/обновлении, если не вложенный URL
    article_id = serializers.PrimaryKeyRelatedField(
        queryset=Article.objects.all(),
        source='article', # Указываем, что это поле 'article' в модели
        write_only=True # Только для записи, для чтения будет вложенная информация или ничего
    )

    class Meta:
        model = AnalyzedSegment
        fields = [
            'id', 'article', 'article_id', 'section_key', 'segment_text',
            'cited_references', 'inline_citation_markers',
            'llm_analysis_notes', 'llm_veracity_score',
            'user', 'created_at', 'updated_at'
        ]
        read_only_fields = ('user', 'created_at', 'updated_at')
        # Делаем article read_only, так как он будет устанавливаться в perform_create или через вложенный URL
        # или если мы принимаем article_id, то 'article' можно сделать read_only=True
        # и использовать article_id для записи.
        extra_kwargs = {
            'article': {'read_only': True} # Будем устанавливать в perform_create
        }


    def create(self, validated_data):
        # user устанавливается во ViewSet.perform_create
        # article также должен быть установлен во ViewSet.perform_create, если используется вложенный роутинг,
        # или если article_id передается в validated_data, как мы сделали с source='article'.
        # Если 'article' (объект) не был добавлен в validated_data сериализатором (из-за article_id),
        # то его нужно будет добавить перед super().create или в самом super().create.
        # В нашем случае ArticleSerializer имеет article_id для записи, так что это должно сработать.
        # Но для AnalyzedSegmentViewSet мы будем передавать article_id в URL или в теле,
        # и устанавливать article в perform_create во ViewSet.

        # Извлекаем cited_references, чтобы правильно их обработать после создания объекта
        cited_references_data = validated_data.pop('cited_references', [])
        segment = AnalyzedSegment.objects.create(**validated_data)
        if cited_references_data:
            segment.cited_references.set(cited_references_data)
        return segment

    def update(self, instance, validated_data):
        cited_references_data = validated_data.pop('cited_references', None)
        instance = super().update(instance, validated_data)
        if cited_references_data is not None: # Если передали пустой список - очистим, если передали данные - установим
            instance.cited_references.set(cited_references_data)
        return instance
