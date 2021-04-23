#!/bin/sh

set -euf -o pipefail

case "${1-}" in

tmdb_movie_id_missing)
	set -x
	python tmdb_movie_id.py missing | tee output.csv
	python quickstatements.py --batchname TMDbIDs <output.csv
	;;

tmdb_movie_id_report)
	set -x
	python tmdb_movie_id.py report | tee report.txt
	python pwb.py login
	python page.py edit --title "User:$WIKIDATA_USERNAME/Maintenance_reports/P4947" <report.txt
	;;

*)
	echo "unknown command"
	exit 1
	;;
esac
