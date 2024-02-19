import aiohttp
import asyncio
import pandas as pd
import requests
import getpass


class GitHubAPI():
    def __init__(self):
        # self.token = getpass.getpass('Введите ваш GitHub токен: ')
        self.token = "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"
        self.headers = {'Authorization': f'token {self.token}'}
        
    async def fetch(self, session, url):
        async with session.get(url, headers=self.headers) as response:
            return await response.json(), response.status
            
    async def get_data(self, repo_names):
        REQUEST_URL = 'https://api.github.com/repos/{repo_name}'
        tasks = []
        data = {'repo': [],'language':[], 'license_name': [], 'is_deleted_or_private': []}

        async with aiohttp.ClientSession() as session:
            for repo_name in repo_names:
                url = REQUEST_URL.format(repo_name=repo_name)
                tasks.append(self.fetch(session, url))

            responses = await asyncio.gather(*tasks)

            for repo_name, (response, status) in zip(repo_names, responses):
                data['repo'].append(repo_name)
                data['license_name'].append(response.get('license', {}).get('name', None) if response.get('license') else None)
                data['is_deleted_or_private'].append(True if status == 404 else False)
                data['language'].append(response.get('language', None))

        df = pd.DataFrame(data)
        return df
        