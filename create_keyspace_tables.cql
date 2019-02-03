CREATE KEYSPACE tapsell WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 };
CREATE TABLE tapsell.clicks (request_id text PRIMARY KEY, click_time bigint);
CREATE TABLE tapsell.impressions (request_id text PRIMARY KEY, ad_id text, ad_title text, advertiser_cost double, app_id text, app_title text, impression_time bigint);