from django.db import models
    
    
class SampleParams(models.Model):
    STATUS_CHOICES = [
        ('ON', 'On'),
        ('OFF', 'Off')
    ]
    DIVISION = [
        ('DEC', 'Dec'),
        ('QUA', 'Qua'),
        ('NONE', 'None')
    ]
    
    save_time = models.DateTimeField()
    start_date = models.DateField()
    end_date = models.DateField()
    repos_count = models.IntegerField()
    min_members_count = models.IntegerField()
    min_watch_count = models.IntegerField()
    is_new_repos = models.BooleanField()
    note = models.CharField(max_length=200, null=True)
    
    # Sample attributes params:
    pushes_duration_status = models.CharField(
        max_length=3,
        choices=STATUS_CHOICES
    )
    pushes_duration_division = models.CharField(
        max_length=4,
        choices=DIVISION
    )
    avg_push_size_status = models.CharField(
        max_length=3,
        choices=STATUS_CHOICES
    )
    avg_push_size_division = models.CharField(
        max_length=4,
        choices=DIVISION
    )
    pull_requests_status = models.CharField(
        max_length=3,
        choices=STATUS_CHOICES
    )
    pull_requests_division = models.CharField(
        max_length=4,
        choices=DIVISION
    )
    merged_pull_requests_ratio_status = models.CharField(
        max_length=3,
        choices=STATUS_CHOICES
    )
    merged_pull_requests_ratio_division = models.CharField(
        max_length=4,
        choices=DIVISION
    )
    issues_status = models.CharField(
        max_length=3,
        choices=STATUS_CHOICES
    )
    issues_division = models.CharField(
        max_length=4,
        choices=DIVISION
    )
    closed_issues_ratio_status = models.CharField(
        max_length=3,
        choices=STATUS_CHOICES
    )
    closed_issues_ratio_division = models.CharField(
        max_length=4,
        choices=DIVISION
    )
    watches_status = models.CharField(
        max_length=3,
        choices=STATUS_CHOICES
    )
    watches_division = models.CharField(
        max_length=4,
        choices=DIVISION
    )
    forks_status = models.CharField(
        max_length=3,
        choices=STATUS_CHOICES
    )
    forks_division = models.CharField(
        max_length=4,
        choices=DIVISION
    )
    new_members_status = models.CharField(
        max_length=3,
        choices=STATUS_CHOICES
    )
    new_members_division = models.CharField(
        max_length=4,
        choices=DIVISION
    )
    language_status = models.CharField(
        max_length=3,
        choices=STATUS_CHOICES
    )
    license_name_status = models.CharField(
        max_length=3,
        choices=STATUS_CHOICES
    )
    is_deleted_or_private_status = models.CharField(
        max_length=3,
        choices=STATUS_CHOICES
    )
    
    
class RepositoryData(models.Model):
    data_params_id = models.ForeignKey(SampleParams, 
                                          on_delete=models.CASCADE, 
                                          related_name='githubdata_set')
    repo_name = models.CharField(max_length=200, null=True)
    pushes = models.IntegerField(null=True)
    avg_push_size = models.FloatField(null=True)
    pull_requests = models.IntegerField(null=True)
    merged_pull_requests_ratio = models.FloatField(null=True)
    issues = models.IntegerField(null=True)
    closed_issues_ratio = models.FloatField(null=True)
    watches = models.IntegerField(null=True)
    forks = models.IntegerField(null=True)
    new_members = models.IntegerField(null=True)
    language = models.CharField(max_length=200, null=True)
    license_name = models.CharField(max_length=200, null=True)
    is_deleted_or_private = models.BooleanField(null=True)
