--[[
sban mod for minetest voxel game
designed and coded by shivajiva101@hotmail.com

request an insecure enviroment to load the db handler
and access files in the world folder. This requires
access via secure.trusted in the minetest.conf file.
i.e. secure.trusted = sban
for it to work!
]]

local ie = minetest.request_insecure_environment()

-- success?
if not ie then
	error("insecure environment inaccessible"..
	" - make sure this mod has been added to minetest.conf!")
end

local _sql = ie.require("lsqlite3")

-- secure the global sqlite3
if sqlite3 then sqlite3 = nil end

-- register privelege
minetest.register_privilege("ban_admin", {
	description = "ban administrator",
	give_to_singleplayer = false,
})

local WP = minetest.get_worldpath()
local WL -- whitelist cache
local ESC = minetest.formspec_escape
local bans = {}
local hotlist = {}
local DB = WP.."/sban.sqlite"
local db_version = "0.2"
local db = _sql.open(DB) -- connection
local expiry, owner, owner_id, def_duration, display_max, names_per_id, importer
local t_units = {
	s = 1, m = 60, h = 3600,
	d = 86400, w = 604800, M = 2592000, y = 31104000,
	D = 86400, W = 604800, Y = 31104000,
	[""] = 1,
}

sban = {}

--[[
################
### Settings ###
################
]]

if minetest.settings then
	expiry = minetest.settings:get("sban.ban_max")
	owner = minetest.settings:get("name")
	def_duration = minetest.settings:get("sban.fs_duration") or "1w"
	display_max = tonumber(minetest.settings:get("sban.display_max")) or 10
	names_per_id = tonumber(minetest.settings:get("sban.accounts_per_id"))
	importer = minetest.settings:get("sban.import_enabled") or true
else
	expiry = minetest.setting_get("sban.ban_max")
	owner = minetest.setting_get("name")
	def_duration = minetest.setting_get("sban.fs_duration") or "1w"
	display_max = tonumber(minetest.setting_get("sban.display_max")) or 10
	names_per_id = tonumber(minetest.setting_get("sban.accounts_per_id"))
	importer = minetest.setting_get("sban.import_enable") or true
end

--[[
###################
###  Functions  ###
###################
]]

-- db:exec wrapper for error reporting
local function db_exec(stmt)
	if db:exec(stmt) ~= _sql.OK then
		minetest.log("error", "Sqlite ERROR:  "..db:errmsg())
	end
	db:close_vm()
end

-- convert value to seconds, copied from xban2 mod and modified
local function parse_time(t)
	local s = 0
	for n, u in t:gmatch("(%d+)([smhdwyDMY]?)") do
		s = s + (tonumber(n) * (t_units[u] or 1))
	end
	return s
end

-- human readable date format - converts UTC
local function hrdf(t)
	if type(t) == "number" then
		return (t and os.date("%c", t))
	end
end

-- handle ip4 & ip6 types
local function ip_checker(str)
	if str:find(":") or str:find("%.") then
		return true
	end
end

-- reqd before db insertion
local function parse_reason(str)
	return string.gsub(str, "'", "''")
end

-- get ban from table in memory
local function ban_fetch(id)
	for _,entry in ipairs(bans) do
		if entry.id == id then
			return entry
		end
	end
end

-- remove ban from table
local function ban_remove(id)
	for i,entry in ipairs(bans) do
		if entry.id == id then
			table.remove(bans, i)
			return true
		end
	end
end

--[[
#################################
###  Database: Create Tables  ###
#################################
]]

createDb = "CREATE TABLE IF NOT EXISTS bans (id INTEGER, "
.."name VARCHAR(50), source VARCHAR(50), created INTEGER, "
.."reason VARCHAR(300), expires INTEGER, u_source VARCHAR(50), "
.."u_reason VARCHAR(300), u_date INTEGER, last_pos VARCHAR(50));\n"
.."CREATE TABLE IF NOT EXISTS ban_history (id INTEGER, "
.."name VARCHAR(50), source VARCHAR(50), created INTEGER, "
.."reason VARCHAR(300), expires INTEGER, u_source VARCHAR(50), "
.."u_reason VARCHAR(300), u_date INTEGER, last_pos VARCHAR(50));\n"
.."CREATE TABLE IF NOT EXISTS playerdata (id INTEGER, "
.."name VARCHAR(50), ip VARCHAR(50), created INTEGER, last_login INTEGER);\n"
.."CREATE TABLE IF NOT EXISTS players ("
.."id INTEGER PRIMARY KEY AUTOINCREMENT, ban BOOLEAN);\n"
.."CREATE TABLE IF NOT EXISTS whitelist (name VARCHAR(50), "
.."source VARCHAR(50), created INTEGER);\n"
.."CREATE TABLE IF NOT EXISTS version (rev VARCHAR(20));\n"
db_exec(createDb)

--[[
###########################
###  Database: Queries  ###
###########################
]]

-- returns an id for name or ip
local function get_id(name_or_ip)
	local q
	local ip_chk = ip_checker(name_or_ip)
	if ip_chk then
		q = ([[
			SELECT id
			FROM playerdata
			WHERE ip = '%s' LIMIT 1;]]
		):format(name_or_ip)
	else
		q = ([[
			SELECT id
			FROM playerdata
			WHERE name = '%s' LIMIT 1;]]
		):format(name_or_ip)
	end
	local it, state = db:nrows(q)
	local row = it(state)
	if row then
		return row.id
	end
end

-- returns the next available id
local function next_id()
	local q = [[SELECT seq FROM sqlite_sequence WHERE name= "players"]]
	-- returns an integer for last id
	local it, state = db:nrows(q)
	local row = it(state)
	if row then
		return row.seq + 1 -- next id
	end
end

-- returns true if db entry is correct
local function check_ban(id)
	local q = ([[
		SELECT  players.id
		FROM    players
		INNER JOIN
			bans ON players.id = bans.id
		WHERE   players.ban = 'true' AND
			bans.id = '%i' LIMIT 1;
	]]):format(id)
	-- fill return table
	local it, state = db:nrows(q)
	local row = it(state)
	return row ~= nil
end

-- returns table of bans from ban & ban_history tables for the id
local function list_ban(id)
	local r = {}
	local q = ban_fetch(id)
	if q then r[1] = q end
	-- check archive
	local q = ([[
	SELECT * FROM ban_history WHERE id = '%i'
	]]):format(id)
	-- fill return table
	for row in db:nrows(q) do
		r[#r + 1] = row
	end
	return r
end

-- returns a table of records from playerdata for an id
local function find_records_by_id(id)
	local r, q
	q = ([[
		SELECT * FROM playerdata
		WHERE id = '%i' ORDER BY last_login DESC;
		]]):format(id)
	for row in db:nrows(q) do
		if not r then r = {} end
		r[#r + 1] = row
	end
	return r
end

-- returns a table of active bans from the db
local function get_active_bans()
	local r = {}
	local q = [[SELECT * FROM bans;]]
 	for row in db:nrows(q) do
		r[#r+1] = row
	end
	return r
end

-- returns a pairs table of whitelisted names
local function get_whitelist()
	local r = {}
	local query = "SELECT * FROM whitelist"
	for row in db:nrows(query) do
		r[row.name] = true
	end
	return r
end

-- returns the db version as a string
local function get_version()
	local query = "SELECT rev FROM version;"
	local it, state = db:nrows(query)
	local row = it(state)
	if row then
		return row.rev
	end
end

-- returns a table of distinct ip's for an id
local function account_ips(id)
	local r,q = {}
	-- construct query
	q = ([[
		SELECT DISTINCT ip
		FROM 	playerdata
		WHERE 	id = '%i';
	]]):format(id)
	-- fill return table
	for row in db:nrows(q) do
		r[#r + 1] = row
	end
	return r
end

-- returns a table of distinct names for an id
local function account_names(id)
	local r,q = {}
	-- construct query
	q = ([[
		SELECT DISTINCT name
		FROM playerdata
		WHERE id = '%i';
	]]):format(id)
	-- fill return table
	for row in db:nrows(q) do
		r[#r + 1] = row
	end
	return r
end

-- console based display of player data
local function display_record(name, p_name)

	local id = get_id(p_name)
	local r = find_records_by_id(id)

	if not r then
		minetest.chat_send_player(name, "No records for "..p_name)
		return
	end

	-- Show names
	do
		local names_hash = {}
		local names = {}
		for i = 1, #r do
			local record_name = r[i].name
			if not names_hash[record_name] then
				names_hash[record_name] = true
				names[#names + 1] = record_name
			end
		end
		minetest.chat_send_player(name, "Names: " .. table.concat(names, ", "))
	end

	local privs = minetest.get_player_privs(name)
	-- records loaded, display
	local idx = 1
	if #r > display_max then
		idx = #r - display_max
		minetest.chat_send_player(name,
		"Player records: "..#r.." (showing last "..display_max.." records)")
	else
		minetest.chat_send_player(name,
		"Player records: "..#r)
	end
	if privs.ban_admin == true then
		for i = idx, #r do
			-- format utc values
			local d1 = hrdf(r[i].created)
			local d2 = hrdf(r[i].last_login)
			minetest.chat_send_player(name,
				("[%s] Name: %s IP: %s Created: %s Last login: %s"
			):format(i, r[i].name, r[i].ip, d1, d2))
		end
	else
		for i = idx, #r do
			local d1 = hrdf(r[i].created)
			local d2 = hrdf(r[i].last_login)
			minetest.chat_send_player(name,
				("[%s] Name: %s Created: %s Last login: %s"
			):format(i, r[i].name, d1, d2))
		end
	end

	local t = list_ban(id) or {}

	if #t > 0 then
		minetest.chat_send_player(name, "Ban records: "..#t)
		local ban = t[#t].active
		for i, e in ipairs(t) do
			local d1 = hrdf(e.created)
			local expires
			if type(e.expires) == "number" then
				expires = hrdf(e.expires)
			else
				expires = "never"
			end
			if type(e.u_date) == "number"
			and e.u_date > 0 then
				local d2 = hrdf(e.u_date)
				minetest.chat_send_player(name,
					("[%s] Name: %s Created: %s Banned by: %s Reason: %s Expires: %s"
				):format(i, e.name, d1, e.source, e.reason, expires))
				minetest.chat_send_player(name,
					("[%s] Unbanned by: %s Reason: %s Time: %s"
				):format(i, e.u_source, e.u_reason, d2))
			else
				minetest.chat_send_player(name,
					("[%s] Name: %s Created: %s Banned by: %s Reason: %s Expires: %s"
				):format(i, e.name, d1, e.source, e.reason, expires))
			end
		end
		minetest.chat_send_player(name, "Banned: "..ban)
	else
		minetest.chat_send_player(name, "No Ban records!")
	end
end

-- returns a table of names like 'name'
local function get_names(name)
	local r,t,q = {},{}
	q = "SELECT name FROM playerdata WHERE name LIKE '%"..name.."%';"
	for row in db:nrows(q) do
		-- Simple sort using a temp table to remove duplicates
		if not t[row.name] then
			r[#r+1] = row.name
			t[row.name] = true
		end
	end
	return r
end

--[[
###########################
###  Database: Inserts  ###
###########################
]]

-- create a new record for a new id
local function create_player_record(player_name, ip_address)
	-- players table id is auto incremented
	local ts = os.time()
	local stmt = [[
	INSERT INTO players (ban) VALUES ('false');
	]]
	db_exec(stmt)
	local id = db:last_insert_rowid()
	add_player_record(id, player_name, ip_address)
	return id
end

-- create a record for a new name/ip of a known id
local function add_player_record(id, player_name, ip_address)
	local ts = os.time()
	local stmt = ([[
			INSERT INTO playerdata
			VALUES (%s,'%s','%s',%s,%s)
	]]):format(id, player_name, ip_address, ts, ts)
	db_exec(stmt)
end

-- add a new whitelist record
local function add_whitelist_record(source, name_or_ip)
	local ts = os.time()
	local stmt = ([[
			INSERT INTO whitelist
			VALUES ('%s', '%s', %i)
	]]):format(name_or_ip, source, ts)
	db_exec(stmt)
end

-- create a new ban record
local function create_ban_record(name, source, reason, expires)

	local ts = os.time()
	local id = get_id(name)
	local player = minetest.get_player_by_name(name)
	local p_reason = parse_reason(reason)

	-- initialise last position
	local last_pos = ""
	if player then
		last_pos = minetest.pos_to_string(vector.round(player:getpos()))
	end

	-- add entry to ban table
	bans[#bans+1] = {
		id = id,
		name = name,
		source = source,
		created = ts,
		reason = reason,
		expires = expires,
		u_source = '',
		u_reason = '',
		u_date = '',
		last_pos = last_pos
	}

	-- add a new record & update status of id
	local stmt = ([[
		UPDATE players SET bans = 'true' WHERE id = '%s';
		INSERT INTO bans VALUES ('%s','%s','%s','%s','%s','%s','','','','%s');
	]]):format(id, id, name, source, ts, p_reason, expires, last_pos)
	db_exec(stmt)

	-- create kick & log messages
	local msg_k, msg_l
	if expires ~= "" then
		local date = hrdf(expires)
		msg_k = ("Banned: Expires: %s, Reason: %s"
		):format(date, reason)
		msg_l = ("[sban] %s temp banned by %s reason: %s"
		):format(name, source, reason)
	else
		msg_k = ("Banned: Reason: %s"):format(reason)
		msg_l = ("[sban] %s banned by %s reason: %s"
		):format(name, source, reason)
	end
	minetest.log("action", msg_l)

	-- kick all player names associated with the id
	local records = find_records_by_id(id)
	for i, v in ipairs(records) do
		player = minetest.get_player_by_name(v.name)
		if player then
			player:set_detach() -- in case attached!
			minetest.kick_player(v.name, msg_k)
		end
	end
end

-- initialise the db version
local function init_version(str)
	local stmt = ([[
			INSERT INTO version
			VALUES ('%s')
			]]):format(str)
	db_exec(stmt)
end

--[[
###########################
###  Database: Updates  ###
###########################
]]

local function update_login(player_name)
	local ts = os.time()
	local stmt = ([[
	UPDATE playerdata SET last_login = %i WHERE name = '%s';
	]]):format(ts, player_name)
	db_exec(stmt)
end

local function update_ban_record(id, source, reason, name)
	reason = parse_reason(reason)
	local ts = os.time()
	local stmt = ([[
		UPDATE players SET bans = 'false' WHERE id = '%i';
		INSERT INTO ban_history SELECT * FROM bans WHERE id = '%i';
		UPDATE ban_history SET
			u_source = '%s',
			u_reason = '%s',
			u_date = '%i'
		WHERE id = '%i';
		DELETE FROM bans WHERE id = '%i';
	]]):format(id, id, source, reason, ts, id, id)
	db_exec(stmt)
	ban_remove(id) -- update cache
	-- log event
	minetest.log("action",
	("[sban] %s unbanned by %s reason: %s"):format(name, source, reason))
end

local function reset_orphan_record(id)
	local stmt = ([[
	UPDATE players SET ban = 'false' WHERE id = '%s';
	]]):format(id)
	db_exec(stmt)
end

local function update_version(str)
	local stmt = ([[
	UPDATE version SET rev = '%s';
	]]):format(str)
	db_exec(stmt)
end

--[[
##################################
###  Database: Delete Records  ###
##################################
]]

local function del_ban_record(id)
	local stmt = ([[
		DELETE FROM bans WHERE id = '%i'
	]]):format(id)
	db:close_vm()
end

local function del_whitelist(name_or_ip)
	local stmt = ([[
		DELETE FROM whitelist WHERE name = '%s'
	]]):format(name_or_ip)
	db_exec(stmt)
end

--[[
#######################
###  Export/Import  ###
#######################
]]
if importer then

	local function load_xban(filename)
		local f, e = ie.io.open(WP.."/"..filename, "rt")
		if not f then
			return false, "Unable to load xban2 database:"..e
		end
		local cont = f:read("*a")
		f:close()
		if not cont then
			return false, "Unable to load xban2 database: Read failed"
		end
		local t = minetest.deserialize(cont)
		if not t then
			return false, "xban2 database: Deserialization failed"
		end
		return t
	end

	local function load_ipban()
		local f, e = ie.io.open(WP.."/ipban.txt")
		if not f then
			return false, "Unable to open `ipban.txt': "..e
		end
		local content = f:read("*a")
		f:close()
		return content
	end

	local function save_sql(txt)
		local file = ie.io.open(WP.."/xban.sql", "a")
		if file then
			file:write(txt)
			file:close()
		end
	end

	local function del_sql()
		ie.os.remove(WP.."/xban.sql")
	end

	local function import_xban(name, file_name)

		local t, err = load_xban(file_name)
		-- exit with error message
		if not t then
			return t, err
		end
		local id = next_id()

		minetest.log("action", "processing "..#t.." records")

		-- iterate the xban2 data
		for i, e in ipairs(t) do
			-- only process banned entries
			if e.banned == true then

				local names = {}
				local ip = {}
				local last_seen = e.last_seen
				local last_pos = e.last_pos or ""
				--local id = nil
				local q
				-- each entry in xban db contains a names field, both IP and names
				-- are stored in this field, split into 2 tables
				for k, v in pairs(e.names) do
					if string.find(k, "%.") then
						table.insert(ip, k)
					else
						table.insert(names, k)
					end
				end
				-- check for existing entry by name
				local chk = true
				for _, v in ipairs(names) do
					q = ([[
					SELECT * FROM playerdata
					WHERE name = '%s' LIMIT 1;
					]]):format(v)
					local it, state = db:nrows(q)
					local row = it(state)
					if row then
						chk = false
						break
					end
				end
				if chk then
					-- process the entry
					-- construct INSERT for players table
					q = [[INSERT INTO players (ban) VALUES ('true');]]
					db_exec(q)

					-- If there are more names use the last ip for
					-- the reamining entries. If there are more ip's
					-- use the last name for the remaining entries
					local ts = os.time()
					if table.getn(names) > table.getn(ip) then
						local tbl = table.getn(ip)
						local idx
						for ii, v in ipairs(names) do
							idx = ii
							if idx > tbl then idx = tbl end
							-- id,name,ip,created,last_login
							q = ([[
							INSERT INTO playerdata
							VALUES (%s,'%s','%s',%s,%s)
							]]):format(id, v, ip[idx], ts, last_seen)
							db_exec(q)
						end
					elseif table.getn(ip) > table.getn(names) then
						local tbl = table.getn(names)
						local idx
						for ii, v in ipairs(ip) do
							idx = ii
							if idx > tbl then idx = tbl end
							-- id,name,ip,created,last_login
							q = ([[
							INSERT INTO playerdata
							VALUES (%s,'%s','%s',%s,%s)
							]]):format(id, names[idx], v, ts, last_seen)
							db_exec(q)
						end
					else
						for ii, v in ipairs(names) do
							-- id,name,ip,created,last_login
							q = ([[
							INSERT INTO playerdata
							VALUES (%s,'%s','%s',%s,%s)
							]]):format(id, v, ip[ii], ts, last_seen)
							db_exec(q)
						end
					end
					-- ban fields: id,name,source,created,reason,expires,
					-- u_source,u_reason,u_date,last_pos
					-- store position as string
					if last_pos.y then
						last_pos = minetest.pos_to_string(last_pos)
					end
					for _, v in ipairs(e.record) do
						local expires = v.expires or ""
						local reason = string.gsub(v.reason, "'", "''")
						q = ([[
						INSERT INTO bans
						VALUES ('%s','%s','%s','%s','%s','%s','','','','%s')
						]]):format(id, names[1], v.source, v.time,
						reason, expires, last_pos)
						db_exec(q)
					end
					id = id + 1
				end
			end
		end
	end

	local function import_ipban(source)
		local contents = load_ipban()
		if not contents then
			return false
		end
		local data = string.split(contents, "\n")
		for i, v in ipairs(data) do
			-- each line consists of an ip, separator and name
			local ip, name = v:match("([^|]+)%|(.+)")
			if ip and name then
				-- check for an existing entry by name
				local q = ([[SELECT * FROM
					playerdata WHERE name = '%s' LIMIT 1;]]):format(name)
				local it, state = db:nrows(q)
				local row = it(state)
				local id
				if not row then
					id = create_player_record(name, ip)
				end
				-- check for existing ban
				if not ban_fetch(id) then
					-- create ban entry - name,source,reason,expires
					create_ban_record(name, source, "imported from ipban.txt", '')
				end
			end
		end
	end

	local function sql_string(id, entry)
		local names = {}
		local ip = {}
		local last_seen = entry.last_seen
		local last_pos = entry.last_pos or ""
		local ts = os.time()

		-- names field includes both IP and names data, sort into 2 tables
		for k, v in pairs(entry.names) do
			if string.find(k, "%.") ~= nil then
				table.insert(ip, k)
			else
				table.insert(names, k)
			end
		end

		-- construct INSERT for players table based on ban status
		local q = ("INSERT INTO players VALUES ('%s','%s');\n"
		):format(id, entry.banned)

		-- case: more names than IP's uses the last entry for reamining names
		if #names > #ip then
			local t = #ip
			local idx
			for i, v in ipairs(names) do
				idx = i
				if idx > t then idx = t end
				-- id,name,ip,created,last_login
				q = q..("INSERT INTO playerdata VALUES ('%s','%s','%s','%s','%s');\n"
				):format(id, v, ip[idx], ts, last_seen)
			end
			-- case: more ip's than names uses last entry for remaining ip's
		elseif #ip > #names then
			local t = #names
			local idx
			for i, v in ipairs(ip) do
				idx = i
				if idx > t then idx = t end
				-- id,name,ip,created,last_login
				q = q..("INSERT INTO playerdata VALUES ('%s','%s','%s','%s','%s');\n"
				):format(id, names[idx], v, ts, last_seen)
			end
			-- case: number of ip's and names is equal
		else
			for i, v in ipairs(names) do
				-- id,name,ip,created,last_login
				q = q..("INSERT INTO playerdata VALUES ('%s','%s','%s','%s','%s');\n"
			):format(id, v, ip[i], ts, last_seen)
			end
		end

		if entry.reason then
			-- convert position
			if last_pos.y then
				last_pos = vector.round(last_pos)
				last_pos = minetest.pos_to_string(last_pos)
			end
			-- id,name,source,created,reason,expires,u_source,u_reason,u_date,active,last_pos
			for i, v in ipairs(entry.record) do
				local expires = v.expires or ""
				local reason = string.gsub(v.reason, "'", "''")
				reason = string.gsub(reason, "%:%)", "")
				q = q..("INSERT INTO bans VALUES ('%s','%s','%s','%i','%s','%s','','','','%s','%s');\n"
				):format(id, names[1], v.source, v.time, reason, expires, entry.banned, last_pos)
			end
		end
		return q
	end

	local function export_sql(filename)
		-- load the db, iterate in reverse order and remove each
		-- record to balance the memory use otherwise large files
		-- cause lua OOM error
		local dbi = load_xban(filename)
		local id = next_id()
		-- reverse the contents with #entries/2
		for i = 1, math.floor(#dbi / 2) do
			local tmp = dbi[i]
			dbi[i] = dbi[#dbi - i + 1]
			dbi[#dbi - i + 1] = tmp
		end
		-- add create tables string
		save_sql(createDb)
		-- add single transaction
		save_sql("BEGIN;\n")
		-- process records
		for i = #dbi, 1, - 1 do
			-- contains data?
			if dbi[i] then
				local str = sql_string(id, dbi[i]) -- sql statement
				save_sql(str)
				dbi[i] = nil -- housekeeping
				id = id + 1
			end
		end
		-- close transaction
		save_sql("END;")
	end

	-- Export the database back to xban db format
	local function export_xban()
		-- so long, thanks for trying it :P
		local xport = {}
		local DEF_DB_FILENAME = minetest.get_worldpath().."/xban.db"
		local DB_FILENAME = minetest.setting_get("xban.db_filename")

		if (not DB_FILENAME) or (DB_FILENAME == "") then
			DB_FILENAME = DEF_DB_FILENAME
		end

		-- players
		local q = [[SELECT * FROM players;]]
		for row in db:nrows(q) do
			local b = false
			if row.ban == 'true' then b = true end
			xport[row.id] = {
				banned = b,

			}
		end

		-- playerdata
		for i,v in ipairs(xport) do
			local name, ip = {}, {}
			xport[i].names = {}
			q = ([[SELECT * FROM playerdata
			WHERE id = '%i']]):format(i)
			for row in db:nrows(q) do
				if not name[row.name] then
					name[row.name] = true
				end
				if not ip[row.ip] then
					ip[row.ip] = true
				end
				xport[i].last_seen = row.last_login
			end
			for key,val in pairs(name) do
				xport[i].names[key] = val
			end
			for key,val in pairs(ip) do
				xport[i].names[key] = val
			end
		end

		-- ban
		for i,v in ipairs(xport) do
			if xport[i].banned == true then
				local t = {}
				q = ([[SELECT * FROM bans WHERE id = '%i';]]):format(i)
				for row in db:nrows(q) do
					t[#t+1] = {
						time = row.created,
						source = row.source,
						reason = row.reason
					}
					if row.active == 'true' then
						xport[i].last_pos = minetest.string_to_pos(row.last_pos)
					end
				end
				xport[i].record = t
			end
		end

		local function repr(x)
			if type(x) == "string" then
				return ("%q"):format(x)
			else
				return tostring(x)
			end
		end

		local function my_serialize_2(t, level)
			level = level or 0
			local lines = { }
			local indent = ("\t"):rep(level)
			for k, v in pairs(t) do
				local typ = type(v)
				if typ == "table" then
					table.insert(lines,
					  indent..("[%s] = {\n"):format(repr(k))
					  ..my_serialize_2(v, level + 1).."\n"
					  ..indent.."},")
				else
					table.insert(lines,
					  indent..("[%s] = %s,"):format(repr(k), repr(v)))
				end
			end
			return table.concat(lines, "\n")
		end

		local function this_serialize(t)
			return "return {\n"..my_serialize_2(t, 1).."\n}"
		end

		local f, e = io.open(DB_FILENAME, "wt")
		xport.timestamp = os.time()
		if f then
			local ok, err = f:write(this_serialize(xport))
			if not ok then
				minetest.log("error", "Unable to save database: %s", err)
			end
		else
			minetest.log("error", "Unable to save database: %s", e)
		end
		if f then f:close() end
	end

	minetest.register_chatcommand("ban_dbe", {
		description = "export xban2 db to sql format",
		params = "<filename>",
		privs = {server = true},
		func = function(name, params)
			local filename = params:match("%S+")
			if not filename then
				return false, "Use: /ban_dbe <filename>"
			end
			del_sql()
			export_sql(filename)
			return true, "xban2 dumped to " .. filename
		end
	})

	minetest.register_chatcommand("ban_dbx", {
		description = "export db to xban2 format",
		privs = {server = true},
		func = function(name)
			export_xban()
			return true, "dumped db to xban2 file!"
		end
	})

	minetest.register_chatcommand("ban_dbi", {
		description = "Import bans",
		params = "<filename>",
		privs = {server = true},
		func = function(name, params)
			local filename = params:match("%S+")
			if not filename then
				return false, "Use: /ban_dbi <filename>"
			end
			local msg
			if filename == "ipban.txt" then
				import_ipban(name)
				msg = "ipban.txt imported!"
			else
				local res, err = import_xban(name, filename)
				msg = err
				if res then
					msg = filename.." imported!"
				end
			end
			return true, msg
		end
	})
end

--[[
##############
###  Misc  ###
##############
]]

-- initialise db version
local current_version = get_version()
if not current_version then
	init_version(db_version) -- clean run
elseif current_version == "0.1" then
	error("You must update sban database to "..db_version..
	"\nUse sqlite3 to import update_to_0.2.sql")
end

-- load whitelist and bans
WL = get_whitelist()
bans = get_active_bans()

-- owner may not have joined yet but attempt to initialise
owner_id = get_id(owner)

local function hotlistp(name, remove)
	for i, v in ipairs(hotlist) do
		if v == name then
			if remove then
				table.remove(hotlist, i)
			end
			return
		end
	end
	table.insert(hotlist, name)
	if #hotlist > 10 then
		table.remove(hotlist, 1)
	end
end

--[[
###########
##  GUI  ##
###########
]]

local state = {}
local FORMNAME = "sban:main"

local function get_state(name)

	local s = state[name]
	-- initialise if reqd
	if not s then
		s = {
			list = {},
			hlist = {},
			index = -1,
			info = "Select an entry from the list\n or use search",
			banned = false,
			ban = nil,
			multi = false,
			page = 1,
			flag = false
		}
		state[name] = s
	end
	return s
end

local function create_info(entry)

	if not entry then
		return "something went wrong!\n Please reselct the entry."
	end

	local str = "Banned by: "..entry.source.."\n"
		.."When: "..hrdf(entry.created).."\n"

	if entry.expires ~= '' then
		str = str.."Expires: "..hrdf(entry.expires).."\n"
	end

	str = str .."Reason: "
	-- Word wrap
	local words = entry.reason:split(" ")
	local l,ctr = 40,8 -- initialise limits
	for _,word in ipairs(words) do
		local wl = word:len()
		if ctr + wl < l then
			str = str..word.." "
			ctr = ctr + (wl + 1)
		else
			str = str.."\n"..word.." "
			ctr = wl + 1
		end
	end

	return str
end

local function getformspec(name)

	local fs = state[name]
	local f
	local list = fs.list
	local bgimg = ""
	if default and default.gui_bg_img then
		bgimg = default.gui_bg_img
	end

	f = "size[8,6.6]"
	..bgimg
	.."field[0.3,0.4;4.5,0.5;search;;]"
	.."field_close_on_enter[search;false]"
	.."button[4.5,0.1;1.5,0.5;find;Find]"
	if #fs.list > 0 then
		f = f.."textlist[0,0.9;2.4,3.6;plist;"

		for i,v in ipairs(list) do
			f = f..v..","
		end

		f = f:sub(1, f:len() - 1)
		f = f..";"..fs.index.."]"
	end
	f = f.."field[0.3,6.5;4.5,0.5;reason;Reason:;]"
	.."field_close_on_enter[reason;false]"

	if fs.multi == true then
		f = f.."image_button[6,0.1;0.5,0.5;ui_left_icon.png;left;]"
		.."image_button[7,0.1;0.5,0.5;ui_right_icon.png;right;]"
		if fs.page > 9 then
			f = f.."label[6.50,0.09;"..fs.page.."]"
		else
			f = f.."label[6.55,0.09;"..fs.page.."]"
		end
	end

	f = f.."label[2.6,0.9;"..fs.info.."]"

	if fs.banned then
		f = f.."button[4.5,6.2;1.5,0.5;unban;Unban]"
	else
		f = f
		.."field[0.3,5.5;2.6,0.3;duration;Duration:;"..def_duration.."]"
		.."field_close_on_enter[duration;false]"
		.."button[4.5,6.2;1.5,0.5;ban;Ban]"
		.."button[6,6.2;2,0.5;tban;Temp Ban]"
	end

	return f
end

local function update_state(name, selected)

	local fs = get_state(name)
	local id = get_id(selected)

	fs.ban = list_ban(id)

	local info = "Ban records: "..#fs.ban.."\n"

	fs.banned = ban_fetch(id)
	fs.multi = false

	if #fs.ban == 0 then
		info = info.."Player has no ban records!"
	else
		if not fs.flag then
			fs.page = #fs.ban
			fs.flag = true
		end
		if fs.page > #fs.ban then fs.page = #fs.ban end
		info = info..create_info(fs.ban[fs.page])
	end

	fs.info = info
	if #fs.ban > 1 then
		fs.multi = true
	end
end

minetest.register_on_player_receive_fields(function(player, formname, fields)

	if formname ~= FORMNAME then return end

	local name = player:get_player_name()
	local privs = minetest.get_player_privs(name)
	local fs = get_state(name)

	if not privs.ban then
		minetest.log("warning",
				"[sban] Received fields from unauthorized user: "..name)
		return
	end

	if fields.find then

		if fields.search:len() > 2 then
			fs.list = get_names(ESC(fields.search))
		else
			fs.list = fs.hlist
		end
		local str = "No record found!"
		if #fs.list > 0 then
			str = "Select an entry to see the details..."
		end
		fs.info = str
		fs.index = -1
		minetest.show_formspec(name, FORMNAME, getformspec(name))

	elseif fields.plist then

		local t = minetest.explode_textlist_event(fields.plist)

		if (t.type == "CHG") or (t.type == "DCL") then

			fs.index = t.index
			fs.flag = false -- reset
			update_state(name, fs.list[t.index])
			minetest.show_formspec(name, FORMNAME, getformspec(name))
		end

	elseif fields.left or fields.right then

		if fields.left then
			if fs.page > 1 then fs.page = fs.page - 1 end
		else
			if fs.page < #fs.ban then fs.page = fs.page + 1 end
		end
		update_state(name, fs.list[fs.index])
		minetest.show_formspec(name, FORMNAME, getformspec(name))

	elseif fields.ban or fields.unban or fields.tban then

		local selected = fs.list[fs.index]
		local id = get_id(selected)

		if fields.reason ~= "" then
			if fields.ban then
				if selected == owner then
					fs.info = "you do not have permission to do that!"
				else
					create_ban_record(selected, name, ESC(fields.reason), '')
					if not check_ban(id) then
						fs.info = "Warning: failed to store ban for "..selected
					end
				end
			elseif fields.unban then
				update_ban_record(id, name, ESC(fields.reason), selected)
				fs.ban = list_ban(id)
			elseif fields.tban then
				if selected == owner then
					fs.info = "you do not have permission to do that!"
				else
					local  t = parse_time(ESC(fields.duration)) + os.time()
					create_ban_record(selected, name, ESC(fields.reason), t)
					if not check_ban(id) then
						fs.info = "Warning: failed to store ban for "..selected
					end
				end
			end
			fs.flag = false -- reset
			update_state(name, selected)
		else
			fs.info = "You must supply a reason!"
		end
		minetest.show_formspec(name, FORMNAME, getformspec(name))
	end
end)

--[[
###########################
###  Register Commands  ###
###########################
]]

minetest.override_chatcommand("ban", {
	description = "ban a player from the server",
	params = "<player> <reason>",
	privs = { ban = true },
	func = function(name, params)
		local player_name, reason = params:match("(%S+)%s+(.+)")

		if not (player_name and reason) then
			-- check params are present
			return false, "Usage: /ban <player> <reason>"
		end

		if player_name == owner then
			-- protect owner
			return false, "Insufficient privileges!"
		end

		local expires = ''
		local id = get_id(player_name)

		if id then
			-- check for existing ban
		   if ban_fetch(id) then
			   return true, ("%s is already banned!"):format(player_name)
		   end
			-- limit ban?
			if expiry then
				expires = parse_time(expiry) + os.time()
			end
			-- Params: name, source, reason, expires
			create_ban_record(player_name, name, reason, expires)
		else
			local privs = minetest.get_player_privs(name)
			-- ban_admin only
			if not privs.ban_admin then
				return false, "Player "..player_name.." doesn't exist!"
			end
			-- create entry & ban
			id = create_player_record(player_name, "0.0.0.0") -- arbritary ip
			create_ban_record(player_name, name, reason, expires)
		end
		-- check database
		if not check_ban(id) then
			minetest.log("info", "ban failed to store "..player_name)
			return false, ("ban failed to store %s"):format(player_name)
		else
			return true, ("Banned %s."):format(player_name)
		end
	end
})

minetest.register_chatcommand("ban_del", {
	description = "Deletes a player's sban records",
	params = "player",
	privs = {server = true},
	func = function(name, params)
		local player_name = params:match("%S+")
		if not player_name then
			return false, "Usage: /ban_del_record <player>"
		end
		local id = get_id(player_name)
		if not id then
			return false, player_name.." doesn't exist!"
		end
		del_ban_record(id)
		minetest.log("action",
		"ban records for "..player_name.." deleted by "..name)
		return true, player_name.." ban records deleted!"
	end
})

minetest.register_chatcommand("ban_record", {
	description = "Display player sban records",
	params = "<player_or_ip>",
	privs = { ban = true },
	func = function(name, params)
		local playername = params:match("%S+")
		if not playername or playername:find("*") then
			return false, "Useage: /ban_record <player_name>"
		end
		-- get target and source privs
		local id = get_id(playername)
		if not id then
			return false, "Unknown player!"
		end
		local target = find_records_by_id(id)
		local source = minetest.get_player_privs(name)
		local chk = false
		-- If the target has server privs on any account
		-- do NOT allow record to be shown unless source
		-- has server priv.
		for i, v in ipairs(target) do
			local privs = minetest.get_player_privs(v.name)
			if privs.server then chk = true break end
		end
		-- if source doesn't have sufficient privs deny
		if not source.server and chk then
			return false, "Insufficient privileges!"
		end
		display_record(name, playername)
		return true
	end
})

minetest.register_chatcommand("ban_wl", {
	description = "Manages the whitelist",
	params = "(add|del|list) <name_or_ip>",
	privs = {server = true},
	func = function(name, params)
		local helper = ("Usage: /ban_wl (add|del) "
		.."<name_or_ip> \nor /ban_wl list")
		local param = {}
		local i = 1
		for word in params:gmatch("%S+") do
			param[i] = word
			i = i + 1
		end
		if #param < 1 then
			return false, helper
		end
		if param[1] == "list" then
			local str = ""
			for k, v in pairs(WL) do
				str = str..k.."\n"
			end
			if str ~= "" then
				return true, str
			end
			return true, "Whitelist empty!"
		end
		if param[2] then
			if param[1] == "add" then
				if not WL[param[2]] then
					add_whitelist_record(name, param[2])
					WL[param[2]] = true
					minetest.log("action",
					("%s added %s to whitelist"):format(name, param[2]))
					return true, param[2].." added to whitelist!"
				else
					return false, param[2].." is already whitelisted!"
				end
			elseif param[1] == "del" then
				if WL[param[2]] then
					del_whitelist(param[2])
					WL[param[2]] = nil
					minetest.log("action", ("%s removed %s from whitelist"
					):format(name, param[2]))
					return true, param[2].." removed from whitelist!"
				else
					return false, param[2].." isn't on the whitelist"
				end
			end
		end
		return false, helper
	end
})

minetest.register_chatcommand("tempban", {
	description = "Ban a player temporarily with sban",
	params = "<player> <time> <reason>",
	privs = { ban = true },
	func = function(name, params)
		local player_name, time, reason = params:match("(%S+)%s+(%S+)%s+(.+)")

		if not (player_name and time and reason) then
			-- correct params?
			return false, "Usage: /tempban <player> <time> <reason>"
		end

		if player_name == owner then
			-- protect owner account
			return false, "Insufficient privileges!"
		end

		time = parse_time(time)
		if time < 60 then
			return false, "You must ban for at least 60 seconds."
		end
		local expires = os.time() + time

		-- is player already banned?
		local id = get_id(player_name)
		if id then
			if ban_fetch(id) then
				return true, ("%s is already banned!"):format(player_name)
			end
			create_ban_record(player_name, name, reason, expires)
		else
			local privs = minetest.get_player_privs(name)
			-- assert normal behaviour without server priv
			if not privs.ban_admin then
				return false, "Player doesn't exist!"
			end
			-- create entry before ban
			id = create_player_record(player_name, "0.0.0.0")
			create_ban_record(player_name, name, reason, expires)
		end
		if not check_ban(id) then
			minetest.log("info", "tempban failed to store "..player_name)
			return false, ("tempban failed to store %s"):format(player_name)
		else
			return true, ("Banned %s until %s."):format(
			player_name, os.date("%c", expires))
		end
	end,
})

minetest.override_chatcommand("unban", {
	description = "Unban a player or ip banned with sban",
	params = "<player_or_ip> <reason>",
	privs = { ban = true },
	func = function(name, params)
		local player_name, reason = params:match("(%S+)%s+(.+)")
		if not (player_name and reason) then
		return false, "Usage: /unban <player_or_ip> <reason>"
		end
		-- look for records by id
		local id = get_id(player_name)
		if id then
			if not ban_fetch(id) then
				return false, ("No active ban record for "..player_name)
			end
			update_ban_record(id, name, reason, player_name)
			if check_ban(id) then
				minetest.log("error", "[sban] Failed to unban "..player_name)
				return false
			else
				return true, ("Unbanned %s."):format(v.name)
			end
		end
	end,
})

minetest.register_chatcommand("bang", {
	description = "Launch sban gui",
	privs = {ban = true},
	func = function(name)
		state[name] = nil
		local fs = get_state(name)
		fs.list = hotlist
		for i,v in ipairs(fs.list) do
			fs.hlist[i] = v
		end
		minetest.show_formspec(name, FORMNAME, getformspec(name))
	end
})

minetest.override_chatcommand("kick", {
	params = "<name> [reason]",
	description = "Kick a player",
	privs = {kick=true},
	func = function(name, param)
		local tokick, reason = param:match("([^ ]+) (.+)")
		tokick = tokick or param
		local player = minetest.get_player_by_name(tokick)
		if not player then
			return false, "Player " .. tokick .. " not in game!"
		end
		if not minetest.kick_player(tokick, reason) then
			player:set_detach()
			if not minetest.kick_player(tokick, reason) then
				return false, "Failed to kick player " .. tokick ..
				" after detaching!"
			end
		end
		local log_reason = ""
		if reason then
			log_reason = " with reason \"" .. reason .. "\""
		end
		minetest.log("action", name .. " kicks " .. tokick .. log_reason)
		return true, "Kicked " .. tokick
  end,
})

minetest.register_chatcommand("/whois", {
	description = "Returns info on a player",
	privs = {ban_admin = true},
	func = function(name, param)
		if not param then
			return false, "Useage: /whois <player>"
		end
		local id = get_id(param)
		if not id then
			return false, "The player \"" .. param .. "\" did not join yet."
		end
		local data = account_names(id)
		local msg1, msg2 = "", ""
		for i, v in ipairs(data) do
			if msg1 ~= "" then
				msg1 = msg1 .. ", " .. v.name
			else
				msg1 = msg1 .. " " .. v.name
			end
		end
		msg1 = msg1 .. "\nIP Adresses: "
		data = account_ips(id)
		for i, v in ipairs(data) do
			if msg2 ~= "" then
				msg2 = msg2 .. ", " .. v.ip
			else
				msg2 = msg2 .. " " .. v.ip
			end
		end
		return false, "Player info for " .. param .. ": " .. msg1 .. msg2
	end,
})

--[[
#######################
###  API Functions  ###
#######################
]]

sban.ban = function(name, source, reason, expires)
	-- check params are valid
	assert(type(name) == 'string')
	assert(type(source) == 'string')
	assert(type(reason) == 'string')
	if expires then
		assert(type(expires) == 'string')
		expires = parse_time(expires)
	end
	local id = get_id(name)
	if id and ban_fetch(id) then
		return false, name.." is already banned!"
	elseif not id then
		return false, name.." doesn't exist!"
	end
	
	create_ban_record(player_name, name, reason, expires)
	-- check database
	if not check_ban(id) then
		minetest.log("info", "sban failed to store "..name)
		return false, ("sban failed to store %s"):format(name)
	else
		return true, ("Banned %s."):format(name)
	end
end

sban.unban = function(name, source, reason)
	-- check params are valid
	assert(type(name) == 'string')
	assert(type(source) == 'string')
	assert(type(reason) == 'string')
	-- look for records by id
	local id = get_id(name)
	if id then
		if not ban_fetch(id) then
			return false, ("No active ban record for "..name)
		end
		update_ban_record(id, name, reason, name)
		if check_ban(id) then
			minetest.log("error", "[sban] Failed to unban "..name)
			return false, ("Failed to unban "..name)
		else
			return true, ("Unbanned %s."):format(name)
		end
	end
end

sban.ban_status = function(name)
	assert(type(name) == 'string')
	local id = get_id(name)
	return id ~= nil
end

sban.player_bans = function(name)
	assert(type(name) == 'string')
	local id = get_id(name)
	if id then
		return list_ban(id)
	else
		return false, (name.." doesn't exist!")
	end
end

--[[
############################
###  Register callbacks  ###
############################
]]

minetest.register_on_shutdown(function()
	db:close_vm()
	db:close()
end)

minetest.register_on_prejoinplayer(function(name, ip)
	-- whitelist bypass
	if WL[name] or WL[ip] then
		minetest.log("action", "[sban] "..
		name.." whitelisted entry permits login")
		return
	end
	-- known player?
	local id = get_id(name) or get_id(ip)

	if not id then return end -- unknown
	if owner_id and owner_id == id then return end -- owner bypass

	local data = ban_fetch(id)
	if not data then
		-- check names per id?
		if names_per_id then
			-- names per id
			local names = account_names(id)
			-- allow existing
			for i,v in ipairs(names) do
				if v.name == name then return end
			end
			-- check player isn't exceeding account limit
			if #names >= names_per_id then
				-- create string list
				local msg = ""
				for i,v in ipairs(names) do
					msg = msg..v.name..", "
				end
				msg = msg:sub(1, msg:len() - 2) -- trim trailing ','
				return ("\nYou exceeded the limit of accounts ("..
				names_per_id..").\nYou already have the following accounts:\n"
				..msg)
			end
		end

		return
	else
		-- check for ban expiry
		local date

		if type(data.expires) == "number" and data.expires ~= 0 then
			-- temp ban
			if os.time() > data.expires then
				-- clear temp ban
				update_ban_record(data.id, "sban", "ban expired", name)
				return
			end
			date = hrdf(data.expires)
		else
			date = "the end of time"
		end
		return ("Banned: Expires: %s, Reason: %s"):format(date, data.reason)
	end
end)

minetest.register_on_joinplayer(function(player)

	local name = player:get_player_name()
	local ip = minetest.get_player_ip(name)
	if not ip then return end
	local id = get_id(name)

	hotlistp(name)

	-- check for player name entry
	if not id then
		-- no records, check for ip
		id = get_id(ip)
		if not id then
			-- new entry
			create_player_record(name, ip)
			if not owner_id and name == owner then
				owner_id = id -- initialise
			end
			return
		else
			-- new name
			add_player_record(id, name, ip)
			return
		end
	else
		-- check ip record
		if not get_id(ip) then
			-- new ip
			add_player_record(id, name, ip)
			return
		end
		-- update record timestamp
		update_login(name)
	end
end)
