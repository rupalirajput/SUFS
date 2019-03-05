import humanfriendly

BLOCKSIZE = humanfriendly.parse_size("64MiB")
HEARTBEAT_TIMEOUT = humanfriendly.parse_timespan("10s")
HEARTBEAT_INTERVAL = humanfriendly.parse_timespan("1s")
BLOCKREPORT_INTERVAL = humanfriendly.parse_timespan("1s")
REPLICATION_FACTIOR = 3
