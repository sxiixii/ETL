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

tables = {
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
}
