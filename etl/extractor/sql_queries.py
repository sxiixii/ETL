from os import environ

from dotenv import load_dotenv

load_dotenv()

FILM_ES_INDEX = environ.get('FILM_ES_INDEX')
PERSON_ES_INDEX = environ.get('PERSON_ES_INDEX')
GENRE_ES_INDEX = environ.get('GENRE_ES_INDEX')

BASE_SELECT = '''SELECT
                   fw.id,
                   fw.title,
                   fw.description,
                   fw.rating,
                   fw.type,
                   fw.created,
                   fw.modified,
                   COALESCE (
                       json_agg(
                           DISTINCT jsonb_build_object(
                               'person_role', pfw.role,
                               'person_id', p.id,
                               'person_name', p.full_name
                           )
                       ) FILTER (WHERE p.id is not null),
                       '[]'
                   ) as persons,
                   array_agg(DISTINCT g.name) as genres\n'''

INDEX_AND_TABLES = {
    FILM_ES_INDEX: {
        'persons': BASE_SELECT + '''FROM content.person p
                                                         LEFT JOIN content.person_film_work pfw ON pfw.person_id = p.id
                                                         LEFT JOIN content.film_work fw ON fw.id = pfw.film_work_id
                                                         LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                                                         LEFT JOIN content.genre g ON g.id = gfw.genre_id
                                                         WHERE p.modified %s
                                                         GROUP BY fw.id
                                                         ORDER BY fw.modified;''',
        'films': BASE_SELECT + '''FROM content.film_work fw
                                                       LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                                                       LEFT JOIN content.person p ON p.id = pfw.person_id
                                                       LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                                                       LEFT JOIN content.genre g ON g.id = gfw.genre_id
                                                       WHERE fw.modified %s
                                                       GROUP BY fw.id
                                                       ORDER BY fw.modified;''',
        'genres': BASE_SELECT + '''FROM content.genre g
                                                        LEFT JOIN content.genre_film_work gfw ON gfw.genre_id = g.id
                                                        LEFT JOIN content.film_work fw ON fw.id = gfw.film_work_id
                                                        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                                                        LEFT JOIN content.person p ON p.id = pfw.person_id
                                                        WHERE g.modified %s
                                                        GROUP BY fw.id
                                                        ORDER BY fw.modified;''',
    },

    PERSON_ES_INDEX: {
        'persons': '''SELECT
                        p.id as person_id,
                        p.full_name,
                        COALESCE (json_agg(
                                           DISTINCT jsonb_build_object(
                                               'role', pfw.role,
                                               'film_id', pfw.film_work_id
                                           )
                                           ) FILTER (WHERE pfw.id is not null),
                                           '[]'
                                       ) as films
                    FROM content.person p
                    LEFT JOIN content.person_film_work pfw ON p.id = pfw.person_id
                    WHERE p.modified %s
                    GROUP BY p.id, p.full_name
                    ORDER BY p.id;'''
    },

    GENRE_ES_INDEX: {
        'genres': '''SELECT
                            g.id as genre_id,
                            g.name,
                            g.description,
                            array_agg(DISTINCT gfw.film_work_id) AS film_ids
                        FROM content.genre g
                        LEFT JOIN content.genre_film_work gfw ON g.id = gfw.genre_id
                        WHERE g.modified %s
                        GROUP BY g.id, g.name
                        ORDER BY g.id;'''
    },

}
