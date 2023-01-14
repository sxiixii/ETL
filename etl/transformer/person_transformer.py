from uuid import UUID

from pydantic import BaseModel


class Person(BaseModel):
    id: str
    full_name: str
    role: str
    film_ids: list[UUID]


class PersonTransformer:
    def __init__(self, data: list[list]) -> None:
        self.data = data

    def transform_data(self) -> list[dict]:
        if self.data:
            for persons in self.data:
                persons_batch = []
                for person in persons:
                    validated_data = self.validate_data(person)
                    persons_batch.append(validated_data)
                yield persons_batch

    def validate_data(self, person: list) -> dict:
        film_ids_list = person[3].strip('{}').split(',')
        person_id = person[0] + '-' + person[2]
        valid_person = Person(
            id=person_id,
            full_name=person[1],
            role=person[2],
            film_ids=film_ids_list,
        )
        return valid_person.dict()
