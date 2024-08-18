from typing import List

class TransformToElastic:
    def __init__(self, data):
        self.data = data

    def transform(self) -> List:
        list = []

        if self.data == []:
            return []

        for movie in self.data:
            transformed_movie = {
                "id": str(movie["id"]),
                "imdb_rating": movie["rating"],
                "genres": movie["genres"],
                "title": movie["title"],
                "description": movie["description"],
                "directors_names": " ".join(
                    [
                        person["person_name"]
                        for person in movie["persons"]
                        if person["person_role"] == "director"
                    ]
                ),
                "actors_names": ", ".join(
                    [
                        person["person_name"]
                        for person in movie["persons"]
                        if person["person_role"] == "actor"
                    ]
                ),
                "writers_names": ", ".join(
                    [
                        person["person_name"]
                        for person in movie["persons"]
                        if person["person_role"] == "writer"
                    ]
                ),
                "directors": [
                    {"id": person["person_id"], "name": person["person_name"]}
                    for person in movie["persons"]
                    if person["person_role"] == "director"
                ],
                "actors": [
                    {"id": person["person_id"], "name": person["person_name"]}
                    for person in movie["persons"]
                    if person["person_role"] == "actor"
                ],
                "writers": [
                    {"id": person["person_id"], "name": person["person_name"]}
                    for person in movie["persons"]
                    if person["person_role"] == "writer"
                ],
            }
            list.append(transformed_movie)

        return list
