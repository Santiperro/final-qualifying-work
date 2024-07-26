CLICKHOUSE_DATA_QUERY = """
    SELECT 
        {select_clause_str}
    FROM
        (SELECT DISTINCT repo_name
        FROM github_events 
        WHERE '{start_date}' <= created_at 
            AND created_at < '{end_date}' 
        {new_repos_filter}
        {min_watches_filter}
        {min_members_filter}
        ) as outerQuery
    {join_clauses_str}
    ORDER BY RAND()
    LIMIT {limit};
    """
    

SELECT_CLAUSES = {
    'pushes': "pushEvents.pushEventCount as pushes",
    'avg_push_size': "pushEvents.avgPushSize as avg_push_size",
    'pull_requests': "pullRequestEvents.pullRequestEventCount as pull_requests",
    'merged_pull_requests_ratio': "pullRequestEvents.mergedPullRequestEventRatio as merged_pull_requests_ratio",
    'issues': "issuesEvents.issuesEventCount as issues",
    'closed_issues_ratio': "issuesEvents.closedIssuesRatio as closed_issues_ratio",
    'watches': "watchEvents.watchEventCount as watches",
    'forks': "forkEvents.forkEventCount as forks",
    'new_members': "newMemberEvents.memberEventCount as new_members"
}

    
JOIN_CLAUSES = {
    "PUSH_EVENTS_QUERY": """
    LEFT JOIN
        (SELECT repo_name, COUNT(*) as pushEventCount, AVG(push_size) as avgPushSize
        FROM github_events 
        WHERE '{start_date}' <= created_at 
            AND created_at < '{end_date}' 
            AND event_type = 'PushEvent' 
        GROUP BY repo_name) as pushEvents
    ON outerQuery.repo_name = pushEvents.repo_name
    """,
    "PULL_REQUEST_EVENTS_QUERY": """
    LEFT JOIN
        (SELECT 
            repo_name, COUNT(*) as pullRequestEventCount, 
            SUM(CASE WHEN merged = 1 THEN 1 ELSE 0 END) / COUNT(*) as mergedPullRequestEventRatio
        FROM github_events 
        WHERE '{start_date}' <= created_at 
            AND created_at < '{end_date}' 
            AND event_type = 'PullRequestEvent'
        GROUP BY repo_name) as pullRequestEvents
    ON outerQuery.repo_name = pullRequestEvents.repo_name
    """,
    "ISSUES_EVENTS_QUERY": """
    LEFT JOIN
        (SELECT 
            repo_name, COUNT(*) as issuesEventCount, 
            SUM(CASE WHEN state = 'closed' THEN 1 ELSE 0 END) / COUNT(*) as closedIssuesRatio
        FROM github_events 
        WHERE '{start_date}' <= created_at 
            AND created_at < '{end_date}' 
            AND event_type = 'IssuesEvent' 
        GROUP BY repo_name) as issuesEvents
    ON outerQuery.repo_name = issuesEvents.repo_name
    """,
    "WATCH_EVENTS_QUERY": """
    LEFT JOIN
        (SELECT repo_name, COUNT(*) as watchEventCount
        FROM github_events 
        WHERE '{start_date}' <= created_at 
            AND created_at < '{end_date}' 
            AND event_type = 'WatchEvent' 
        GROUP BY repo_name) as watchEvents
    ON outerQuery.repo_name = watchEvents.repo_name
    """,
    "FORK_EVENTS_QUERY": """
    LEFT JOIN
        (SELECT repo_name, COUNT(*) as forkEventCount
        FROM github_events 
        WHERE '{start_date}' <= created_at 
            AND created_at < '{end_date}' 
            AND event_type = 'ForkEvent' 
        GROUP BY repo_name) as forkEvents
    ON outerQuery.repo_name = forkEvents.repo_name
    """,
    "NEW_MEMBER_EVENTS_QUERY": """
    LEFT JOIN
        (SELECT repo_name, COUNT(*) as memberEventCount
        FROM github_events 
        WHERE '{start_date}' <= created_at 
            AND created_at < '{end_date}' 
            AND event_type = 'MemberEvent' 
        GROUP BY repo_name) as newMemberEvents
    ON outerQuery.repo_name = newMemberEvents.repo_name
    """
}

NEW_REPOS_QUERY_FILTER = """
    AND event_type = 'CreateEvent' 
    AND ref_type = 'repository'
"""

MIN_WATCHES_QUERY_FILTER = """
    AND repo_name IN (
                    SELECT repo_name
                    FROM github_events 
                    WHERE '{start_date}' <= created_at 
                        AND created_at < '{end_date}' 
                        AND event_type = 'WatchEvent'
                    GROUP BY repo_name
                    HAVING COUNT(*) >= {watch_event_count}
                )
"""

MIN_MEMBERS_QUERY_FILTER = """
    AND repo_name IN (
                    SELECT repo_name
                    FROM github_events 
                    WHERE '{start_date}' <= created_at 
                        AND created_at < '{end_date}' 
                        AND event_type = 'MemberEvent'
                        AND action = 'added'
                    GROUP BY repo_name
                    HAVING COUNT(*) >= {new_members_count}
                )
"""