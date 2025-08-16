from utils.api_client import BaseApiClient

class MlbApiClient(BaseApiClient):

    TEAMS_ENDPOINT = "/v1/teams"

    def __init__(self):
        super().__init__(base_url='https://statsapi.mlb.com/api')

    def get_teams(self) -> dict:
        """
        Returns a list of all teams from the MLB API.

        Returns:
            dict: A dictionary containing the list of teams.
        """
        fields = [
            'teams',
            'id',
            'abbreviation',
            'teamName',
            'locationName',
            'division',
            'name',
            'sport',
            'league'
        ]
        params = {
            'sportId': 1,
            'fields': ','.join(fields)
        }
        response = self.get(endpoint=self.TEAMS_ENDPOINT, params=params)
        return response.json()['teams']
    
if __name__ == '__main__':
    client = MlbApiClient()
    teams = client.get_teams()
    print(teams)
