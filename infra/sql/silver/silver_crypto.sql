select crypto, 
	ROUND(usd, 5) as usd_price,
	ROUND(usd_market_cap, 5) as market_cap,
	usd_24h_vol, 
	ROUND(usd_24h_change, 2) as 24_hr_change,
	last_updated_at
from crypto_quotes 
where usd_price IS NOT NULL
