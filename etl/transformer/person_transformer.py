from typing import List
from uuid import UUID

from pydantic import BaseModel


class Films(BaseModel):
    role: str
    film_ids: List[UUID]


class Person(BaseModel):
    id: UUID
    name: str
    roles: list


class PersonTransformer:
    def __init__(self, data: list[list]) -> None:
        self.data = data

    def transform_data(self) -> list[dict]:
        if self.data:
            for persons in self.data:
                persons_batch = []
                for person in persons:
                    grouped_data = self.group_data(person)
                    validated_data = self.validate_data(grouped_data)
                    persons_batch.append(validated_data)
                yield persons_batch

    def group_data(self, person: list):
        roles = person[2]
        grouped_roles = {}
        for role in roles:
            check_key = grouped_roles.get(role['role'], None)
            if check_key is None:
                value = list()
                value.append(role['film_id'])
                grouped_roles[role['role']] = value
            else:
                grouped_roles[role['role']].append(role['film_id'])
        roles = [Films(role=key, film_ids=value) for key, value in grouped_roles.items()]
        print(roles)
        return {
            'id': person[0],
            'name': person[1],
            'roles': roles
        }

    def validate_data(self, person: dict) -> dict:
        valid_person = Person(
            id=person['id'],
            name=person['name'],
            roles=person['roles']
        )
        return valid_person.dict()
