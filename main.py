import os
import logging
from pathlib import Path
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
import uuid
from fastapi import BackgroundTasks
import aiofiles
import asyncio
import uvicorn
app = FastAPI()


"""Очень переменчивое окружение"""
#Путь до Ghostscript
GS_PATH = Path(os.getenv("GS_PATH", "C:/Program Files/gs/gs10.05.1/bin/gswin64c.exe")) #"/usr/bin/gs"
#Ограничение кол-ва параллельных потоков(не влезающие стоят в очереди)
max_parallel_requests = int(os.getenv('MPR', '3'))


#Логи в файл
log_file_path = "app.log"

#Обработчик файла
file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
file_handler.setLevel(logging.INFO)

#Формат логов
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

#Получение логера и обрабочик
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)

#Вывод в консоль
logging.basicConfig(level=logging.INFO)   

#Назначаем директории
BASE_DIR = Path(__file__).parent
UPLOAD_DIR = BASE_DIR / "uploads"
COMPRESSED_DIR = BASE_DIR / "compressed"

#Создаём папки
UPLOAD_DIR.mkdir(exist_ok=True)
COMPRESSED_DIR.mkdir(exist_ok=True)


#Кэш странички html
index_html_content = None

async def load_index_html():
    global index_html_content
    if index_html_content is None:
        async with aiofiles.open(BASE_DIR / "templates/index.html", "r", encoding="utf-8") as f:
            index_html_content = await f.read()


#Очистка файлов (параллельно - тк всем файлам даётся уникальное имя)
async def cleanup_files(files):
    tasks = []
    for filename in files:
        tasks.append(asyncio.create_task(delete_file(filename)))
    await asyncio.gather(*tasks)

async def delete_file(filepath):
    try:
        await asyncio.to_thread(os.remove, filepath)
        logger.info(f"Удалён файл: {filepath}")
    except Exception as e:
        logger.error(f"Ошибка при удалении файла {filepath}: {e}")


#Функция сжатия
async def compress_pdf_with_ghostscript(input_path: Path, output_path: Path, quality: str = "ebook", dpi: int = 150):
    
    if not GS_PATH.exists():
        raise FileNotFoundError(f"Ghostscript не найден по пути: {GS_PATH}")

    #Набор параметров гостскрипт для сжатия(сам процесс)
    cmd = [
        str(GS_PATH),
        "-sDEVICE=pdfwrite",
        "-dCompatibilityLevel=1.4",#Версия PDF
        "-dPDFSETTINGS=/" + quality,#В quality передаём метод сжатия(ebook)
        "-dDownsampleColorImages=true",#Разрешение менять dpi (у разных изображений - цвет, чб)
        "-dColorImageDownsampleThreshold=1.0",
        "-dColorImageResolution=" + str(dpi),#Изменяем dpi
        "-dDownsampleGrayImages=true",
        "-dGrayImageDownsampleThreshold=1.0",
        "-dGrayImageResolution=" + str(dpi),
        "-dDownsampleMonoImages=true",
        "-dMonoImageDownsampleThreshold=1.0",
        "-dMonoImageResolution=" + str(dpi),
        "-dNOPAUSE",
        "-dBATCH",
        "-dQUIET",
        f"-sOutputFile={output_path}",
        input_path
    ]

    #Запускаем
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    #Личный таймаут
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=300)#таймаут 5 минут                            #Вроде хорошая идея
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()#Чтобы дождаться фактического завершения и не запечатывать зависший процесс
        raise RuntimeError("Процесс Ghostscript превысил таймаут")
    
    #Проверка ошибок
    if process.returncode != 0:                                                                                               #Могут ли быть другие ошибки?
        error_msg = stderr.decode()
        raise RuntimeError(f"Ghostscript error: {error_msg}")


#Семафор для ограничения кол-ва потоков
semaphore = asyncio.Semaphore(max_parallel_requests)


#Запрос для получения html страницы(делаем один раз)
@app.get("/", response_class=HTMLResponse)
async def read_index():
    await load_index_html()
    return index_html_content


@app.post("/compress")
async def compress_pdf(
    #Для удаления полсле отправки файла  
    background_tasks: BackgroundTasks,
    #Получаем файл
    file: UploadFile = File(...),
    #Получаем уровень сжатия(по умолчанию средний)
    level: str = Form("medium")
):
    #Оборачиваем всю обработку в семафор
    async with semaphore:

        #Читаем файл
        contents = await file.read()

        #РандомИмя
        filename_uuid = f"{uuid.uuid4()}.pdf"
        input_path = UPLOAD_DIR / filename_uuid
        output_path = COMPRESSED_DIR / filename_uuid
        #Записываем так чтобы потом можно было вернуть с исходным именем

        #Сохраняем исходный файл
        async with aiofiles.open(input_path, "wb") as f:
            await f.write(contents)

        try:
            #!!Уровень качества (поменять после тестов) - возможно оставить только один алгоритм и изменять только dpi!!
            quality_map = {
                "low": "ebook",
                "medium": "ebook",
                "high": "ebook"
            }
            #Дополнительный выбор dpi(подобрать оптимальные значения)
            dpi_map = {
                "low": 100,
                "medium": 150,
                "high": 200   #220
            }

            #Записываем полученное значение урвня сжатия(на всякий если не задано то средний)
            quality_setting = quality_map.get(level, "ebook")
            dpi_setting = dpi_map.get(level, 150)

            #Вызываем фунцию сжатия
            await compress_pdf_with_ghostscript(input_path, output_path, quality=quality_setting, dpi=dpi_setting)


            #Размер исходного файла
            initial_size_kb = os.path.getsize(input_path) // 1024
        
            #Размер сжатого файла
            final_size_kb = os.path.getsize(output_path) // 1024

            #Пустой лог(чтобы глазу приятно)
            logger.info(" ")
            #Лог успешного сжатия
            logger.info(f"Файл: {file.filename} сжат и сохранен как {output_path}")
            #Имя файла | уровень сжатия | алгоритм сжатия | dpi | начальный вес | конечный вес
            logger.info(f"Файл: {file.filename} | Уровень: {level} | Алгоритм: {quality_setting} | DPI: {dpi_setting} | Исходный вес: {initial_size_kb} KB | Итоговый вес: {final_size_kb} KB")


            #Вызываем функцию удаления файлов в фоне после завершения функции  -  асинхронное удаление может привести к затираню файла который в этот момент использует другой пользователь, если у него такое же имя(!НЕ УБИРАТЬ РандомИмя!)
            background_tasks.add_task(cleanup_files, [input_path, output_path])
        
            #Возвращаем файл(с исходным названием)
            return FileResponse(str(output_path), filename=f"СЖАТЫЙ_{level}_{file.filename}")


        #Ошибки (ВСЕ критические)
        except Exception as e:
            #Пустой лог(чтобы глазу приятно)
            logger.info(" ")
            #Записываем ошибку в логи
            logger.error(f"Файл: {file.filename} Ошибка при обработке: {e}")
            #Чистим файлы
            await cleanup_files([input_path, output_path])
            #Кидаем ошибку на страницу
            raise HTTPException(status_code=500, detail="Ошибка при обработке файла")

#Чтобы запускать сервер при запуске файла(тестить без консоли)
if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000)


#'screen', 'ebook', 'printer', 'prepress'
# логи в файл:
#Имя файла _ уровень сжатия _ алгоритм сжатия _ dpi _ начальный вес _ конечный вес

#Тяжеловесность алгоритмов: screen/ebook -> printer -> prepress(быстрее принтера и в целом не замедляется на высоких dpi)
# prepress - на низком dpi текст размытый но нет ряби мешающей чтению(60 = 90 ebook - по весу)

"""ИТОГ:  
Используем ebook
Текст при ЛЮБОМ dpi сжимается одинаково"""
# ebook и screen не отличимы глазу и равны по весу при равном dpi(на текстовых файлах чутка отличаютя по весу в пользу ebook = 1%)
# printer можно попробовать рассмотреть для высокого качества ( на текстовых файлах весит чутка больше = 6%)

