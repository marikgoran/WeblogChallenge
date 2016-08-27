REGISTER /usr/local/Cellar/pig/0.16.0/libexec/lib/piggybank.jar;
REGISTER /usr/local/Cellar/pig/0.16.0/libexec/lib/datafu.jar;
DEFINE Sessionize datafu.pig.sessions.Sessionize('900s');

input_data = load 'small-tz-ip.log' USING org.apache.pig.piggybank.storage.CSVExcelStorage(' ', 'NO_MULTILINE', 'WINDOWS') as (timestamp:chararray,  client_socket:chararray);

-- Clean up the data from fields that will not be used
clean_data = FOREACH input_data GENERATE timestamp as timestamp, SUBSTRING(client_socket,0,INDEXOF(client_socket,':',1)) as ipaddress;

-- Sessionize the data, courtesy of datafu
-- This will append a GUID session tag to each row. The data needs to be sorted by timestamp
tagged_sessions = FOREACH (GROUP clean_data BY ipaddress) {
     sorted_data = ORDER clean_data BY timestamp ;
     GENERATE FLATTEN(Sessionize(sorted_data)) AS (timestamp, ipaddress, session_id);
};

-- Group all entries under same session guid
session = GROUP tagged_sessions  BY  session_id;

-- -- 1) sessionizing data

-- Isolate the client IP, session length and the number of (non-unique for now) hits
session_stats = FOREACH session GENERATE FLATTEN(TOP(1,0,tagged_sessions.ipaddress)) as ipaddress, SecondsBetween(ToDate(MAX(tagged_sessions.timestamp)),ToDate(MIN(tagged_sessions.timestamp))) as length, COUNT(tagged_sessions.ipaddress) as url_hits, group as session_id;

-- -- 2) Determine the average session time 

-- AVG per user, need to group all sessions for each one
session_avg = GROUP session_stats BY ipaddress;
user_avg = FOREACH session_avg GENERATE group, AVG(session_stats.length);

-- global AVG for all sessions and all users
global_avg = FOREACH (GROUP session_stats ALL) GENERATE AVG(session_stats.length);


-- -- 3) Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
-- work in progress, needs to refactor the code to include the urls in the cleaned data
-- for non unique hits:
hit_stats = FOREACH session_stats GENERATE session_id, url_hits;

-- -- 4) Find the most engaged users, ie the IPs with the longest session times
engaged_users = ORDER session_stats BY length;

-- limit the answers to 10 entries each, for speed and simplicity
answer1 = LIMIT session_stats 10;
answer2 = LIMIT global_avg 10;
answer3 = LIMIT hit_stats 10;
answer4 = LIMIT engaged_users 10;


