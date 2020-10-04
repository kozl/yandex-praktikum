WITH x AS (
	SELECT m.id, group_concat(a.name) as actors_names, group_concat(a.id) as actors_ids
	FROM movies m
		JOIN movie_actors ma ON ma.movie_id = m.id
		JOIN actors a ON a.id = ma.actor_id
	GROUP BY ma.movie_id
)

SELECT 
	m.id,
	title,
	plot as description,
	director,
	genre, 
	imdb_rating,
	x.actors_names,
	x.actors_ids,
	CASE
		WHEN m.writers = '' THEN '[{"id": "' || m.writer || '"}]'
		ELSE m.writers
	END AS writers
	FROM movies m
	JOIN x ON x.id = m.id