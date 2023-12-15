GET_EVENT_BY_DAY_QUERY ="""
SELECT * FROM `githubarchive.day.%s`
WHERE type = @event_type
LIMIT 1000"""

GET_ALL_DATA_QUERY = """
SELECT * FROM `githubarchive.%s`
"""