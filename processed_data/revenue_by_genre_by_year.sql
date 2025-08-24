
            SELECT
                EXTRACT(YEAR FROM M.release_date) AS release_year,
                G.name AS genre_name,
                SUM(M.revenue) AS total_revenue
            FROM
                movies M
            JOIN
                movie_genres MG ON M.movie_id = MG.movie_id
            JOIN
                genres G ON MG.genre_id = G.genre_id
            GROUP BY
                release_year, genre_name
            ORDER BY
                release_year, total_revenue DESC;
            
