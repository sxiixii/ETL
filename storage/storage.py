import abc
import json
from pathlib import Path
from typing import Any

from etl_logger import log


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        pass


class JsonFileStorage(BaseStorage):
    """
    Файлы состояний хранятся в папке 'storage'. Класс принимает имя файла,
    если такой файл не существует, то он будет создан.
    """

    def __init__(self, file_name: str):
        self.file_name = file_name

    def get_file_path(self):
        current_path = Path.cwd()
        parent = current_path.parent
        return Path(parent, 'state', self.file_name + '.json')

    def get_file(self):
        file_path = self.get_file_path()
        file_exist = Path.exists(file_path)
        if not file_exist:
            with open(file_path, 'w'):
                pass
        return file_path

    def save_state(self, state: dict) -> None:
        json_state = json.dumps(state)
        file = self.get_file()
        with open(file, 'w') as f:
            f.write(json_state)
            log.info(f'Successfully saved state: "{state}"')

    def retrieve_state(self) -> dict:
        file = self.get_file()
        with open(file, 'r') as f:
            data = f.read()
            if data:
                return json.loads(data)
            log.info(f'Trying to read an empty file "{self.file_name}"')
            return {}


class State:
    def __init__(self, storage: BaseStorage):
        self.storage = storage

    def set_state(self, key: str, value: Any) -> None:
        data = {key: value}
        self.storage.save_state(data)

    def get_state(self, key: str) -> Any:
        data = self.storage.retrieve_state()
        value = data.get(key, None)
        return value
