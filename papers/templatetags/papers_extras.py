import json
from django import template
from django.utils.safestring import mark_safe


register = template.Library()


@register.filter(name='jsonify')
def jsonify(data):
    """
    Безопасно преобразует Python объект в JSON-строку.
    Использование: {{ my_variable|jsonify }}
    """
    if data is None:
        return mark_safe('null') # JSON null

    # json.dumps по умолчанию экранирует символы, такие как <, >, &
    # что делает его безопасным для вставки в <script> теги.
    # mark_safe говорит Django, что эта строка уже безопасна и не требует доп. HTML-экранирования.
    return mark_safe(json.dumps(data))


@register.filter(name='get_item') # Ваш существующий фильтр, если вы его используете
def get_item(dictionary, key):
    if isinstance(dictionary, dict):
        return dictionary.get(key)
    return None


@register.filter(name='split')
def split(value, arg):
    return value.split(arg)