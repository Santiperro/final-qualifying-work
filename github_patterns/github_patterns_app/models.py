from django.db import models
    
class GithubDataParams(models.Model):
    repos_count = models.IntegerField()
    is_new_repos = models.BooleanField()
    min_members_count = models.IntegerField()
    min_watch_count = models.IntegerField()


class GithubData(models.Model):
    data_params_id = models.OneToOneField(GithubDataParams, on_delete=models.CASCADE)
    repo_name = models.CharField(max_lenght=200, null=True)
    pushes = models.IntegerField(null=True)
    avg_push_size = models.FloatField(null=True)
    pull_requests = models.IntegerField(null=True)
    merged_pull_requests_ratio = models.FloatField(null=True)
    issues = models.IntegerField(null=True)
    closed_issues_ratio = models.FloatField(null=True)
    watches = models.IntegerField(null=True)
    forks = models.IntegerField(null=True)
    language = models.CharField(max_length=200, null=True)
    license_name = models.CharField(max_length=200, null=True)
    is_deleted_or_private = models.BooleanField(null=True)
    