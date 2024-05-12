from django.db import models


class AttributeInfo(models.Model):
    STATUS_CHOICES = [
        ('ON', 'On'),
        ('OFF', 'Off')
    ]
    DIVISION = [
        ('DEC', 'Dec'),
        ('QUA', 'Qua'),
        ('NONE', 'None')
    ]
    status = models.CharField(
        max_length=3,
        choices=STATUS_CHOICES
    )
    division = models.CharField(
        max_length=4,
        choices=DIVISION
    )
    
    def __str__(self):
        return f"Status: {self.get_status_display()}, Division: {self.get_division_display()}"
    
class SampleParams(models.Model):
    save_time = models.DateTimeField()
    start_date = models.DateField()
    end_date = models.DateField()
    repos_count = models.IntegerField()
    min_members_count = models.IntegerField()
    min_watch_count = models.IntegerField()
    is_new_repos = models.BooleanField()
    note = models.CharField(max_length=200, null=True)
    
    # Sample attributes params:
    pushes_duration_info = models.OneToOneField(AttributeInfo, 
                                             on_delete=models.CASCADE, 
                                             related_name='pushes_info')
    avg_push_size_info = models.OneToOneField(AttributeInfo, 
                                           on_delete=models.CASCADE, 
                                           related_name='avg_push_size_info')
    pull_requests_info = models.OneToOneField(AttributeInfo, 
                                           on_delete=models.CASCADE, 
                                           related_name='pull_requests_info')
    merged_pull_requests_ratio_info = models.OneToOneField(AttributeInfo, 
                                                        on_delete=models.CASCADE, 
                                                        related_name='merged_pull_requests_ratio_info')
    issues_info = models.OneToOneField(AttributeInfo, 
                                    on_delete=models.CASCADE, 
                                    related_name='issues_info')
    closed_issues_ratio_info = models.OneToOneField(AttributeInfo, 
                                                 on_delete=models.CASCADE, 
                                                 related_name='closed_issues_ratio_info')
    watches_info = models.OneToOneField(AttributeInfo, 
                                     on_delete=models.CASCADE, 
                                     related_name='watches_info')
    forks_info = models.OneToOneField(AttributeInfo, 
                                   on_delete=models.CASCADE, 
                                   related_name='forks_info')
    new_members_info = models.OneToOneField(AttributeInfo, 
                                         on_delete=models.CASCADE, 
                                         related_name='new_members_info')
    language_info = models.OneToOneField(AttributeInfo, 
                                         on_delete=models.CASCADE, 
                                         related_name='language_info')
    license_name_info = models.OneToOneField(AttributeInfo, 
                                         on_delete=models.CASCADE, 
                                         related_name='license_name_info')
    is_deleted_or_private_info = models.OneToOneField(AttributeInfo, 
                                         on_delete=models.CASCADE, 
                                         related_name='is_deleted_or_private_info')

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
