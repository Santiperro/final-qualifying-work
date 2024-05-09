# This Python code snippet defines a multi-part SQL query stored in the
# `CLICKHOUSE_DATA_QUERY` variable. The query retrieves various metrics related
# to GitHub events for repositories within a specified time range.
CLICKHOUSE_DATA_QUERY = """
    SELECT 
        outerQuery.repo_name as repo,
        pushEvents.pushEventCount as pushes,
        pushEvents.avgPushSize as avg_push_size,
        pullRequestEvents.pullRequestEventCount as pull_requests,
        pullRequestEvents.mergedPullRequestEventRatio as merged_pull_requests_ratio,
        issuesEvents.issuesEventCount as issues,
        issuesEvents.closedIssuesRatio as closed_issues_ratio,
        watchEvents.watchEventCount as watches,
        forkEvents.forkEventCount as forks,
        newMemberEvents.memberEventCount as new_members
    FROM
        (SELECT DISTINCT repo_name
        FROM github_events 
        WHERE '{start_date}' <= created_at 
            AND created_at < '{end_date}' 
        {new_repos_filter}
        {min_watches_filter}
        {min_members_filter}
        ) as outerQuery
    LEFT JOIN
        (SELECT repo_name, COUNT(*) as pushEventCount, AVG(push_size) as avgPushSize
        FROM github_events 
        WHERE '{start_date}' <= created_at 
            AND created_at < '{end_date}' 
            AND event_type = 'PushEvent' 
        GROUP BY repo_name) as pushEvents
    ON outerQuery.repo_name = pushEvents.repo_name
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
    LEFT JOIN
        (SELECT repo_name, COUNT(*) as watchEventCount
        FROM github_events 
        WHERE '{start_date}' <= created_at 
            AND created_at < '{end_date}' 
            AND event_type = 'WatchEvent' 
        GROUP BY repo_name) as watchEvents
    ON outerQuery.repo_name = watchEvents.repo_name
    LEFT JOIN
        (SELECT repo_name, COUNT(*) as forkEventCount
        FROM github_events 
        WHERE '{start_date}' <= created_at 
            AND created_at < '{end_date}' 
            AND event_type = 'ForkEvent' 
        GROUP BY repo_name) as forkEvents
    ON outerQuery.repo_name = forkEvents.repo_name
    LEFT JOIN
        (SELECT repo_name, COUNT(*) as memberEventCount
        FROM github_events 
        WHERE '{start_date}' <= created_at 
            AND created_at < '{end_date}' 
            AND event_type = 'MemberEvent' 
        GROUP BY repo_name) as newMemberEvents
    ON outerQuery.repo_name = newMemberEvents.repo_name
    ORDER BY RAND()
    LIMIT {limit};
    """

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