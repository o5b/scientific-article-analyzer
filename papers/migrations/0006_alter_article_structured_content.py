# Generated by Django 5.2.1 on 2025-06-05 12:13

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('papers', '0005_article_structured_content'),
    ]

    operations = [
        migrations.AlterField(
            model_name='article',
            name='structured_content',
            field=models.JSONField(blank=True, default=dict, help_text='Содержимое статьи, разбитое по секциям (например, abstract, introduction, methods, results, discussion, conclusion)', null=True, verbose_name='Структурированное содержимое'),
        ),
    ]
