with artists as (
	SELECT id, to_json(json_build_object('id',id,'name',name,'type',type)) as data
	FROM spotify.interpret
	GROUP BY 1
),
track2artists as (
	SELECT tracks_id, json_agg(a.data) as artists
	FROM spotify.tracks2interpret t2i
	LEFT JOIN artists as a on t2i.interpret_id = a.id
	GROUP BY 1
),
album2artists as (
	SELECT alben_id, json_agg(a.data) as artists
	FROM spotify.alben2interpret t2i
	LEFT JOIN artists as a on t2i.interpret_id = a.id
	GROUP BY 1
),
track as (
	SELECT tra.*,
		to_json(json_build_object(
			'id',alb.id,
			'album_name', alb.album_name,
			'album_type', alb.album_type,
			'release_date', alb.release_date,
			'total_tracks', alb.total_tracks,
			'artists', a2a.artists
		)) as album,
		t2a.artists
	FROM spotify.tracks tra
	LEFT JOIN track2artists t2a on t2a.tracks_id = tra.id
	LEFT JOIN spotify.alben alb on alb.id = tra.alben_id
	LEFT JOIN album2artists a2a on alb.id = a2a.alben_id
),
aggregated as (
	SELECT
		to_json(json_build_object(
			'id',usr.id,
			'type', usr.type
		)) as added_by,
		to_json(json_build_object(
			'id',tra.id,
			'name', tra.name,
			'popularity', tra.popularity,
			'track_number', tra.track_number,
			'type', tra.type,
			'album',tra.album,
			'artists',tra.artists
		)) as track
	FROM track tra
	LEFT JOIN spotify.users usr on tra.users_id = usr.id and tra.users_type = usr.type
)
SELECT
	to_json(json_build_object(
		'added_by',agg.added_by,
		'track', agg.track
	)) as document
FROM aggregated agg