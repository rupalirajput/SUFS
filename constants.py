import humanfriendly

BLOCKSIZE = humanfriendly.parse_size("64MiB")
HEARTBEAT_TIMEOUT = humanfriendly.parse_timespan("10s")
HEARTBEAT_INTERVAL = humanfriendly.parse_timespan("5s")
BLOCKREPORT_INTERVAL = humanfriendly.parse_timespan("5s")
REPLICATION_FACTIOR = 3
BUCKETNAME = 'sufsloh'
