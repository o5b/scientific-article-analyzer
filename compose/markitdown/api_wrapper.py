# /app/api_wrapper.py (внутри Docker-контейнера markitdown)
from fastapi import FastAPI, File, UploadFile, HTTPException
import tempfile
import os
import shutil
import logging


# Импортируем markitdown как библиотеку
try:
    from markitdown import MarkItDown
    MARKITDOWN_LIBRARY_AVAILABLE = True
    # Можно создать один экземпляр, если он потокобезопасен и это эффективно
    # md_converter_instance = MarkItDown()
    # Либо создавать новый экземпляр для каждого запроса для большей безопасности
except ImportError as e:
    MARKITDOWN_LIBRARY_AVAILABLE = False
    logging.critical(f"Критическая ошибка: Не удалось импортировать библиотеку MarkItDown: {e}. "
                     f"Убедитесь, что она корректно установлена в Docker-образе, как указано в Dockerfile.")
    # В продакшене можно было бы завершить работу сервиса, если библиотека недоступна
    # raise RuntimeError(f"MarkItDown library could not be imported: {e}") from e


app = FastAPI(
    title="MarkItDown Conversion Service",
    description="API для конвертации файлов в Markdown с использованием библиотеки MarkItDown.",
    version="1.0.0"
)


logger = logging.getLogger("markitdown-api-wrapper")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


# Проверка, что MarkItDown загрузилась при старте приложения (опционально)
if not MARKITDOWN_LIBRARY_AVAILABLE:
    logger.error("Сервис запускается, но библиотека MarkItDown недоступна. Эндпоинты не будут работать.")


async def convert_file_content_with_markitdown(file_content: bytes, original_filename: str) -> str:
    """
    Конвертирует содержимое файла (байты) с использованием библиотеки markitdown.
    MarkItDown().convert() ожидает путь к файлу, поэтому мы сохраняем содержимое во временный файл.
    """
    if not MARKITDOWN_LIBRARY_AVAILABLE:
        logger.error("Попытка вызова конвертации, но библиотека MarkItDown недоступна.")
        raise RuntimeError("Библиотека MarkItDown недоступна в этом окружении.")

    # Используем расширение оригинального файла, если оно есть, или .tmp
    # Важно: Убедитесь, что MarkItDown().convert() может обрабатывать PDF,
    # если вы передаете PDF. Пример md.convert("test.xlsx") использует .xlsx.
    # Если для PDF нужен другой метод или формат, это нужно учесть.
    # Для этого API мы предполагаем, что это PDF.
    file_suffix = os.path.splitext(original_filename)[1] if original_filename else ".pdf"
    if not file_suffix: # Если имя файла без расширения, например "myfile"
        file_suffix = ".pdf" # По умолчанию для этого эндпоинта

    temp_file_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=file_suffix) as tmp_file:
            tmp_file.write(file_content)
            temp_file_path = tmp_file.name
        
        logger.info(f"Временный файл '{temp_file_path}' создан для '{original_filename}'. Начинается конвертация.")
        
        # Создаем экземпляр MarkItDown. Если он тяжелый, можно оптимизировать.
        md_converter = MarkItDown()
        result = md_converter.convert(temp_file_path) # Передаем путь к временному файлу

        if hasattr(result, 'text_content'):
            markdown_output = result.text_content
            logger.info(f"Конвертация файла '{original_filename}' через библиотеку MarkItDown успешна. "
                        f"Длина Markdown: {len(markdown_output)}.")
            return markdown_output
        else:
            logger.error(f"Результат конвертации MarkItDown для '{original_filename}' не имеет атрибута 'text_content'. "
                         f"Тип результата: {type(result)}. Содержимое (если небольшое): {str(result)[:200]}")
            raise ValueError("Формат результата конвертации от MarkItDown не соответствует ожидаемому.")
            
    except Exception as e:
        logger.error(f"Ошибка во время конвертации файла '{original_filename}' библиотекой MarkItDown: {e}", exc_info=True)
        # Перебрасываем исключение, чтобы FastAPI обработал его и вернул корректный HTTP-ответ
        raise
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
            logger.debug(f"Временный файл '{temp_file_path}' удален.")


@app.post("/convert-document/")
async def convert_document_endpoint(file: UploadFile = File(...)):
    """
    Эндпоинт для конвертации загруженного файла (предположительно PDF, XLSX, DOCX и т.д.,
    в зависимости от того, что поддерживает MarkItDown().convert()) в Markdown.
    """
    original_filename = file.filename if file.filename else "unknown_file"
    logger.info(f"Получен файл: '{original_filename}' для конвертации через MarkItDown API.")
    
    if not MARKITDOWN_LIBRARY_AVAILABLE:
        logger.error("MarkItDown library is not available for processing the request.")
        raise HTTPException(status_code=503, detail="Сервис временно недоступен: внутренняя библиотека MarkItDown не загружена.")

    try:
        file_content = await file.read()
        if not file_content:
            logger.warning(f"Получен пустой файл: '{original_filename}'.")
            raise HTTPException(status_code=400, detail="Получен пустой файл.")

        markdown_result = await convert_file_content_with_markitdown(file_content, original_filename)
            
        return {"markdown_text": markdown_result, "source_tool": "markitdown_python_library"}
    except ValueError as e: # Наша ошибка, если результат конвертации неожиданный
        logger.error(f"Ошибка значения при конвертации '{original_filename}': {e}")
        raise HTTPException(status_code=500, detail=str(e))
    except HTTPException: # Если HTTPException уже была поднята (например, 400)
        raise
    except Exception as e: # Другие ошибки от MarkItDown или общие
        logger.error(f"Неожиданная ошибка при конвертации '{original_filename}': {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Произошла внутренняя ошибка сервера при обработке файла: {str(e)}")
    finally:
        await file.close() # Закрываем файл в любом случае
        logger.debug(f"Файл '{original_filename}' закрыт после обработки.")


@app.get("/health", summary="Проверка состояния сервиса")
async def health_check():
    """Проверяет доступность сервиса и базовую работоспособность библиотеки MarkItDown."""
    status_report = {"service_status": "ok", "markitdown_library_available": MARKITDOWN_LIBRARY_AVAILABLE}
    if MARKITDOWN_LIBRARY_AVAILABLE:
        try:
            _ = MarkItDown() # Попытка инициализации
            status_report["markitdown_library_initialization"] = "success"
        except Exception as e:
            status_report["markitdown_library_initialization"] = f"failed: {str(e)}"
            status_report["service_status"] = "degraded" # Сервис работает, но основная функция может быть нарушена
            logger.warning(f"Health check: MarkItDown library initialization failed: {e}")
    else:
        status_report["service_status"] = "error" # Критическая ошибка, библиотека не импортирована

    if status_report["service_status"] == "error":
         raise HTTPException(status_code=503, detail=status_report)
    return status_report