SELECT
    *,
    3959 * acos(
        cos(radians(40.683850 )) * cos(radians(latitude)) *
        cos(radians(longitude) - radians(-73.973270)) +
        sin(radians(40.683850 )) * sin(radians(latitude))
    ) AS distance
FROM gmaps_details
WHERE
    distance <= 15 and
	(company like '%pharmacy%' or category like '%pharmacy%')
GROUP BY phone