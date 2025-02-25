WITH daily_change AS( 
SELECT date,
       ticker,
       close,
       LAG(close) OVER (PARTITION BY ticker ORDER BY date) AS d1
  FROM assets
) 
SELECT date,   
        ticker,
        (d1-close)/close AS pct_change
        FROM daily_change