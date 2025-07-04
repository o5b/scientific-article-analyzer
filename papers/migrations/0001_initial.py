# Generated by Django 5.2.1 on 2025-06-01 14:04

import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='Author',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('full_name', models.CharField(max_length=255, unique=True, verbose_name='Полное имя')),
            ],
            options={
                'verbose_name': 'Автор',
                'verbose_name_plural': 'Авторы',
                'ordering': ['full_name'],
            },
        ),
        migrations.CreateModel(
            name='Article',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('title', models.TextField(verbose_name='Название статьи')),
                ('abstract', models.TextField(blank=True, null=True, verbose_name='Аннотация')),
                ('doi', models.CharField(blank=True, db_index=True, max_length=255, null=True, unique=True, verbose_name='DOI')),
                ('pubmed_id', models.CharField(blank=True, db_index=True, max_length=50, null=True, unique=True, verbose_name='PubMed ID')),
                ('arxiv_id', models.CharField(blank=True, db_index=True, max_length=50, null=True, unique=True, verbose_name='arXiv ID')),
                ('cleaned_text_for_llm', models.TextField(blank=True, help_text='Полный текст статьи, очищенный и подготовленный для анализа LLM. Может быть добавлен вручную.', null=True, verbose_name='Очищенный текст для LLM')),
                ('is_manually_added_full_text', models.BooleanField(default=False, help_text='Указывает, был ли полный текст статьи добавлен пользователем вручную.', verbose_name='Полный текст добавлен вручную')),
                ('primary_source_api', models.CharField(blank=True, help_text='API, из которого были взяты основные метаданные для этой записи (title, abstract).', max_length=100, null=True, verbose_name='API основного источника')),
                ('publication_date', models.DateField(blank=True, null=True, verbose_name='Дата публикации')),
                ('journal_name', models.CharField(blank=True, max_length=512, null=True, verbose_name='Название журнала/источника')),
                ('oa_status', models.CharField(blank=True, help_text='Статус Open Access от Unpaywall (e.g., gold, green, bronze, closed)', max_length=50, null=True, verbose_name='Статус Open Access')),
                ('best_oa_url', models.URLField(blank=True, help_text='Ссылка на лучшую OA версию (HTML/лендинг) от Unpaywall', max_length=2048, null=True, verbose_name='URL лучшей OA версии')),
                ('best_oa_pdf_url', models.URLField(blank=True, help_text='Ссылка на PDF лучшей OA версии от Unpaywall', max_length=2048, null=True, verbose_name='URL PDF лучшей OA версии')),
                ('oa_license', models.CharField(blank=True, help_text='Лицензия OA версии от Unpaywall (e.g., cc-by, cc-by-nc)', max_length=100, null=True, verbose_name='Лицензия OA версии')),
                ('created_at', models.DateTimeField(auto_now_add=True, verbose_name='Дата создания')),
                ('updated_at', models.DateTimeField(auto_now=True, verbose_name='Дата обновления')),
                ('user', models.ForeignKey(help_text='Пользователь, добавивший статью в систему', on_delete=django.db.models.deletion.CASCADE, to=settings.AUTH_USER_MODEL, verbose_name='Пользователь')),
            ],
            options={
                'verbose_name': 'Научная статья',
                'verbose_name_plural': 'Научные статьи',
                'ordering': ['-updated_at', '-created_at'],
            },
        ),
        migrations.CreateModel(
            name='ArticleAuthorOrder',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('order', models.PositiveIntegerField(default=0, verbose_name='Порядок')),
                ('article', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='papers.article')),
                ('author', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='papers.author')),
            ],
            options={
                'verbose_name': 'Порядок автора статьи',
                'verbose_name_plural': 'Порядки авторов статей',
                'ordering': ['order'],
            },
        ),
        migrations.AddField(
            model_name='article',
            name='authors',
            field=models.ManyToManyField(related_name='articles', through='papers.ArticleAuthorOrder', to='papers.author', verbose_name='Авторы'),
        ),
        migrations.CreateModel(
            name='ReferenceLink',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('raw_reference_text', models.TextField(blank=True, help_text='Текст ссылки, как он представлен в исходной статье.', null=True, verbose_name='Исходный текст ссылки')),
                ('target_article_doi', models.CharField(blank=True, db_index=True, help_text='DOI статьи, на которую ссылаются. Может быть введен/отредактирован пользователем.', max_length=255, null=True, verbose_name='DOI цитируемой статьи')),
                ('manual_data_json', models.JSONField(blank=True, help_text='JSON с метаданными цитируемой статьи, если она добавляется вручную (title, authors, year, etc.).', null=True, verbose_name='Данные, введенные вручную')),
                ('status', models.CharField(choices=[('pending_doi_input', 'Ожидает ввода/поиска DOI'), ('doi_lookup_in_progress', 'Идет поиск DOI для ссылки'), ('doi_provided_needs_lookup', 'DOI найден, ожидает загрузки статьи'), ('article_fetch_in_progress', 'Идет загрузка статьи по DOI'), ('article_linked', 'Статья найдена и связана'), ('article_not_found', 'Статья не найдена по DOI'), ('manual_entry', 'Данные введены вручную'), ('manual_metadata_only', 'Метаданные введены вручную (без связи)'), ('error_doi_lookup', 'Ошибка при поиске DOI'), ('error_article_fetch', 'Ошибка при загрузке статьи'), ('error_processing', 'Ошибка при обработке')], default='pending_doi_input', max_length=50, verbose_name='Статус ссылки')),
                ('log_messages', models.TextField(blank=True, help_text='Сообщения о процессе поиска, загрузки, ошибках.', null=True, verbose_name='Логи обработки')),
                ('created_at', models.DateTimeField(auto_now_add=True, verbose_name='Дата создания')),
                ('updated_at', models.DateTimeField(auto_now=True, verbose_name='Дата обновления')),
                ('resolved_article', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='cited_by_references', to='papers.article', verbose_name='Связанная статья в БД')),
                ('source_article', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='references_made', to='papers.article', verbose_name='Исходная статья')),
            ],
            options={
                'verbose_name': 'Библиографическая ссылка',
                'verbose_name_plural': 'Библиографические ссылки',
                'ordering': ['-created_at'],
            },
        ),
        migrations.CreateModel(
            name='ArticleContent',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('source_api_name', models.CharField(help_text="Например, 'pubmed', 'crossref_api', 'arxiv_api'", max_length=100, verbose_name='Название API источника')),
                ('format_type', models.CharField(help_text="Например, 'json_metadata', 'xml_fulltext_jats', 'abstract_text', 'references_list_json'", max_length=50, verbose_name='Тип формата')),
                ('content', models.TextField(verbose_name='Содержимое')),
                ('retrieved_at', models.DateTimeField(auto_now_add=True, verbose_name='Дата и время загрузки')),
                ('article', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='contents', to='papers.article', verbose_name='Статья')),
            ],
            options={
                'verbose_name': 'Контент статьи из источника',
                'verbose_name_plural': 'Контент статей из источников',
                'unique_together': {('article', 'source_api_name', 'format_type')},
            },
        ),
        migrations.AddConstraint(
            model_name='articleauthororder',
            constraint=models.UniqueConstraint(fields=('article', 'order'), name='unique_author_order_per_article'),
        ),
        migrations.AlterUniqueTogether(
            name='articleauthororder',
            unique_together={('article', 'author')},
        ),
    ]
