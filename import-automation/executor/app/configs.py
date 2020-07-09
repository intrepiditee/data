from google.cloud import datastore

PROJECT_ID = 'google.com:datcom-data'
CONFIGS_NAMESPACE = 'configs'
CONFIGS_KIND = 'config'
MANIFEST_FILENAME = 'manifest.json'
REQUIREMENTS_FILENAME = 'requirements.txt'
REPO_OWNER_USERNAME = 'intrepiditee'
GITHUB_AUTH_USERNAME = 'intrepiditee'


def _get_config(entity_id):
    client = datastore.Client(project=PROJECT_ID, namespace=CONFIGS_NAMESPACE)
    key = client.key(CONFIGS_KIND, entity_id)
    return client.get(key)[entity_id]


def get_dashboard_oauth_client_id(version_id='latest'):
    return _get_config('DASHBOARD_OAUTH_CLIENT_ID')


def get_github_auth_access_token(version_id='latest'):
    return _get_config('GITHUB_AUTH_ACCESS_TOKEN')