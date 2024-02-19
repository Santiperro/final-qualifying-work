SELECT 
    outerQuery.repo_name as repo,
    pushEvents.pushEventCount as push_events,
    pushEvents.avgPushSize as avg_push_size,
    pullRequestEvents.pullRequestEventCount as pull_requests,
    pullRequestEvents.mergedPullRequestEventRatio as merged_pull_request_ratio,
    issuesEvents.issuesEventCount as issues,
    issuesEvents.closedIssuesRatio as closed_issues_ratio,
    watchEvents.watchEventCount as watch_events,
    forkEvents.forkEventCount as fork_events
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