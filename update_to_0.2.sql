-- updates sban v0.1 database to v0.2
PRAGMA foreign_keys = OFF;

-- create and populate ban_history table
CREATE TABLE IF NOT EXISTS ban_history (id INTEGER, name VARCHAR(50),
source VARCHAR(50), created INTEGER, reason VARCHAR(300), expires INTEGER,
u_source VARCHAR(50), u_reason VARCHAR(300), u_date INTEGER, last_pos VARCHAR(50));
INSERT INTO ban_history SELECT id, name, source, created, reason, expires,
u_source, u_reason, u_date, last_pos FROM bans WHERE active != 'true';
DELETE FROM bans where active != 'true';

-- drop the active field from bans
CREATE TABLE sban_temp AS SELECT bans.id AS 'id', bans.name AS 'name',
bans.source AS 'source', bans.created AS 'created', bans.reason AS 'reason',
bans.expires AS 'expires', bans.u_source AS 'u_source', bans.u_reason AS 'u_reason',
bans.u_date AS 'u_date', bans.last_pos AS 'last_pos' FROM bans;
DROP TABLE bans;
CREATE TABLE bans AS SELECT * FROM sban_temp;
DROP TABLE sban_temp;

-- set db_version
UPDATE version SET rev = '0.2';

-- cleanup db
VACUUM;

PRAGMA foreign_keys = ON;
