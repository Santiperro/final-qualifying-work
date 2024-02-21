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
        forkEvents.forkEventCount as forks
    FROM
        (SELECT repo_name
        FROM github_events 
        WHERE '{start_date}' <= created_at 
            AND created_at < '{end_date}' 
            AND event_type = 'CreateEvent' 
            AND ref_type = 'repository') as outerQuery
    INNER JOIN
        (SELECT repo_name
        FROM github_events 
        WHERE '{start_date}' <= created_at 
            AND created_at < '{end_date}' 
            AND event_type = 'WatchEvent'
        GROUP BY repo_name
        HAVING COUNT(*) >= {watch_event_count}) as reposWithWatchEvent
    ON outerQuery.repo_name = reposWithWatchEvent.repo_name
    ANY LEFT JOIN
        (SELECT repo_name, COUNT(*) as pushEventCount, AVG(push_size) as avgPushSize
        FROM github_events 
        WHERE '{start_date}' <= created_at 
            AND created_at < '{end_date}' 
            AND event_type = 'PushEvent' 
        GROUP BY repo_name) as pushEvents
    ON outerQuery.repo_name = pushEvents.repo_name
    ANY LEFT JOIN
        (SELECT 
            repo_name, COUNT(*) as pullRequestEventCount, 
            SUM(CASE WHEN merged = 1 THEN 1 ELSE 0 END) / COUNT(*) as mergedPullRequestEventRatio
        FROM github_events 
        WHERE '{start_date}' <= created_at 
            AND created_at < '{end_date}' 
            AND event_type = 'PullRequestEvent'
        GROUP BY repo_name) as pullRequestEvents
    ON outerQuery.repo_name = pullRequestEvents.repo_name
    ANY LEFT JOIN
        (SELECT 
            repo_name, COUNT(*) as issuesEventCount, 
            SUM(CASE WHEN state = 'closed' THEN 1 ELSE 0 END) / COUNT(*) as closedIssuesRatio
        FROM github_events 
        WHERE '{start_date}' <= created_at 
            AND created_at < '{end_date}' 
            AND event_type = 'IssuesEvent' 
        GROUP BY repo_name) as issuesEvents
    ON outerQuery.repo_name = issuesEvents.repo_name 
    ANY LEFT JOIN
        (SELECT repo_name, COUNT(*) as watchEventCount
        FROM github_events 
        WHERE '{start_date}' <= created_at 
            AND created_at < '{end_date}' 
            AND event_type = 'WatchEvent' 
        GROUP BY repo_name) as watchEvents
    ON outerQuery.repo_name = watchEvents.repo_name
    ANY LEFT JOIN
        (SELECT repo_name, COUNT(*) as forkEventCount
        FROM github_events 
        WHERE '{start_date}' <= created_at 
            AND created_at < '{end_date}' 
            AND event_type = 'ForkEvent' 
        GROUP BY repo_name) as forkEvents
    ON outerQuery.repo_name = forkEvents.repo_name
    ORDER BY RAND()
    LIMIT {limit};
    """
    
FORKS_QUANTILES_QUERY = """
    SELECT 
        quantile({quantile1})(forkEventCount) as quantile1_fork_events,
        quantile({quantile2})(forkEventCount) as quantile2_fork_events,
        quantile({quantile3})(forkEventCount) as quantile3_fork_events,
        quantile({quantile4})(forkEventCount) as quantile4_fork_events,
        quantile({quantile5})(forkEventCount) as quantile5_fork_events
    FROM
        (SELECT repo_name, COUNT(*) as forkEventCount
        FROM github_events 
        WHERE '{start_date}' <= created_at 
            AND created_at < '{end_date}' 
            AND event_type = 'ForkEvent' 
        GROUP BY repo_name)
    """
        
PUSHES_QUANTILES_QUERY = """
SELECT 
    quantile({quantile1})(pushEventCount) as quantile1_push_events,
    quantile({quantile2})(pushEventCount) as quantile2_push_events,
    quantile({quantile3})(pushEventCount) as quantile3_push_events,
    quantile({quantile4})(pushEventCount) as quantile4_push_events,
    quantile({quantile5})(pushEventCount) as quantile5_push_events
FROM
    (SELECT repo_name, COUNT(*) as pushEventCount
    FROM github_events 
    WHERE '{start_date}' <= created_at 
        AND created_at < '{end_date}' 
        AND event_type = 'PushEvent' 
    GROUP BY repo_name)
"""
AVG_PUSH_SIZE_QUANTILES_QUERY = """
SELECT 
    quantile({quantile1})(avgPushSize) as quantile1_avg_push_size,
    quantile({quantile2})(avgPushSize) as quantile2_avg_push_size,
    quantile({quantile3})(avgPushSize) as quantile3_avg_push_size,
    quantile({quantile4})(avgPushSize) as quantile4_avg_push_size,
    quantile({quantile5})(avgPushSize) as quantile5_avg_push_size
FROM
    (SELECT repo_name, AVG(push_size) as avgPushSize
    FROM github_events 
    WHERE '{start_date}' <= created_at 
        AND created_at < '{end_date}' 
        AND event_type = 'PushEvent' 
    GROUP BY repo_name)
"""

PULL_REQUESTS_QUANTILES_QUERY = """
SELECT 
    quantile({quantile1})(pullRequestEventCount) as quantile1_pull_requests,
    quantile({quantile2})(pullRequestEventCount) as quantile2_pull_requests,
    quantile({quantile3})(pullRequestEventCount) as quantile3_pull_requests,
    quantile({quantile4})(pullRequestEventCount) as quantile4_pull_requests,
    quantile({quantile5})(pullRequestEventCount) as quantile5_pull_requests
FROM
    (SELECT 
        repo_name, COUNT(*) as pullRequestEventCount
    FROM github_events 
    WHERE '{start_date}' <= created_at 
        AND created_at < '{end_date}' 
        AND event_type = 'PullRequestEvent'
    GROUP BY repo_name)
"""

MERGED_PULL_REQUESTS_RATIO_QUANTILES_QUERY = """
SELECT 
    quantile({quantile1})(mergedPullRequestEventRatio) as quantile1_merged_pull_request_ratio,
    quantile({quantile2})(mergedPullRequestEventRatio) as quantile2_merged_pull_request_ratio,
    quantile({quantile3})(mergedPullRequestEventRatio) as quantile3_merged_pull_request_ratio,
    quantile({quantile4})(mergedPullRequestEventRatio) as quantile4_merged_pull_request_ratio,
    quantile({quantile5})(mergedPullRequestEventRatio) as quantile5_merged_pull_request_ratio
FROM
    (SELECT 
        repo_name, SUM(CASE WHEN merged = 1 THEN 1 ELSE 0 END) / COUNT(*) as mergedPullRequestEventRatio
    FROM github_events 
    WHERE '{start_date}' <= created_at 
        AND created_at < '{end_date}' 
        AND event_type = 'PullRequestEvent'
    GROUP BY repo_name)
"""

ISSUES_QUANTILES_QUERY = """
SELECT 
    quantile({quantile1})(issuesEventCount) as quantile1_issues,
    quantile({quantile2})(issuesEventCount) as quantile2_issues,
    quantile({quantile3})(issuesEventCount) as quantile3_issues,
    quantile({quantile4})(issuesEventCount) as quantile4_issues,
    quantile({quantile5})(issuesEventCount) as quantile5_issues
FROM
    (SELECT 
        repo_name, COUNT(*) as issuesEventCount
    FROM github_events 
    WHERE '{start_date}' <= created_at 
        AND created_at < '{end_date}' 
        AND event_type = 'IssuesEvent' 
    GROUP BY repo_name)
"""

CLOSED_ISSUES_RATIO_QUANTILES_QUERY = """
SELECT 
    quantile({quantile1})(closedIssuesRatio) as quantile1_closed_issues_ratio,
    quantile({quantile2})(closedIssuesRatio) as quantile2_closed_issues_ratio,
    quantile({quantile3})(closedIssuesRatio) as quantile3_closed_issues_ratio,
    quantile({quantile4})(closedIssuesRatio) as quantile4_closed_issues_ratio,
    quantile({quantile5})(closedIssuesRatio) as quantile5_closed_issues_ratio
FROM
    (SELECT 
        repo_name, SUM(CASE WHEN state = 'closed' THEN 1 ELSE 0 END) / COUNT(*) as closedIssuesRatio
    FROM github_events 
    WHERE '{start_date}' <= created_at 
        AND created_at < '{end_date}' 
        AND event_type = 'IssuesEvent' 
    GROUP BY repo_name)
"""

WATCHES_QUANTILES_QUERY = """
SELECT 
    quantile({quantile1})(watchEventCount) as quantile1_watch_events,
    quantile({quantile2})(watchEventCount) as quantile2_watch_events,
    quantile({quantile3})(watchEventCount) as quantile3_watch_events,
    quantile({quantile4})(watchEventCount) as quantile4_watch_events,
    quantile({quantile5})(watchEventCount) as quantile5_watch_events
FROM
    (SELECT repo_name, COUNT(*) as watchEventCount
    FROM github_events 
    WHERE '{start_date}' <= created_at 
        AND created_at < '{end_date}' 
        AND event_type = 'WatchEvent' 
    GROUP BY repo_name)
"""

