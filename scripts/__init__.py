import os
import sys
import logging

_log_format: str = "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
_dateftm: str = "%d/%B/%Y %H:%M:%S"

# os.environ['XL_IDP_PATH_REFERENCE'] = 'json'
# os.environ['XL_IDP_ROOT_AUTO_UPDATE_SCRIPTS'] = '.'


def get_logger(name: str) -> logging.getLogger:
    # Создаем объект логгера
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Создаем обработчик для вывода в стандартный поток вывода
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)

    # Создаем обработчик для записи в файл
    log_dir_name: str = f"{os.environ.get('XL_IDP_ROOT_AUTO_UPDATE_SCRIPTS')}/logging"
    if not os.path.exists(log_dir_name):
        os.mkdir(log_dir_name)
    file_handler = logging.FileHandler(f"{log_dir_name}/{name}.log")
    file_handler.setLevel(logging.INFO)

    # Создаем обработчик для вывода в стандартный поток ошибок
    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setLevel(logging.ERROR)

    # Создаем форматер для сообщений
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stdout_handler.setFormatter(formatter)
    stderr_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Добавляем обработчики к логгеру
    logger.addHandler(stdout_handler)
    logger.addHandler(stderr_handler)

    return logger


def get_my_env_var(var_name: str) -> str:
    try:
        return os.environ[var_name]
    except KeyError as e:
        raise MissingEnvironmentVariable(f"{var_name} does not exist") from e


class MissingEnvironmentVariable(Exception):
    pass
