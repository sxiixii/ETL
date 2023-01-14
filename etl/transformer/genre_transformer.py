from uuid import UUID

from pydantic import BaseModel


class Genre(BaseModel):
    id: str
    name: str
    description: str
    film_ids: list[UUID]


class GenreTransformer:
    def __init__(self, data: list[list]) -> None:
        self.data = data

    def transform_data(self) -> list[dict]:
        if self.data:
            for genres in self.data:
                genres_batch = []
                for genre in genres:
                    validated_data = self.validate_data(genre)
                    genres_batch.append(validated_data)
                yield genres_batch

    def validate_data(self, genre: list) -> dict:
        film_ids_list = genre[3].strip('{}').split(',')

        valid_genre = Genre(
            id=genre[0],
            name=genre[1],
            description=genre[2],
            film_ids=film_ids_list,
        )
        return valid_genre.dict()
