SELECT * FROM gmaps_details 
LEFT JOIN gmaps_links on gmaps_details.gmaps_url=gmaps_links.gmaps_url 
LEFT JOIN osm_reverse_geocode_parsed on gmaps_links.id=osm_reverse_geocode_parsed.id where gmaps_links.id in (SELECT id from osm_reverse_geocode_parsed WHERE country='United States') and query_parameter like '%insurance%' GROUP BY company