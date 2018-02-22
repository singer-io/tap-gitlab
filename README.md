# tap-gitlab

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from GitLab's [REST API](https://docs.gitlab.com/ee/api/README.html)
- Extracts the following resources from GitLab:
  - [Branches](https://docs.gitlab.com/ee/api/branches.html)
  - [Commits](https://docs.gitlab.com/ee/api/commits.html)
  - [Issues](https://docs.gitlab.com/ee/api/issues.html)
  - [Milestones](https://docs.gitlab.com/ee/api/milestones.html)
  - [Projects](https://docs.gitlab.com/ee/api/projects.html)
  - [Users](https://docs.gitlab.com/ee/api/users.html)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state


## Quick start

1. Install

    ```bash
    > pip install tap-gitlab
    ```

2. Get your GitLab access token

    - Login to your GitLab account
    - Navigate to your profile page
    - Create an access token

3. Create the config file

    Create a JSON file called `config.json` containing:
    - Access token you just created
    - API URL for your GitLab account. If you are using the public gitlab.com this will be `https://gitlab.com/api/v3`
    - Projects to track (space separated)

    ```json
    {"api_url": "https://gitlab.com/api/v3",
     "private_token": "your-access-token",
     "projects": "myorg/repo-a myorg/repo-b",
     "start_date": "2018-01-01T00:00:00Z"}
    ```

4. [Optional] Create the initial state file

    You can provide JSON file that contains a date for the API endpoints
    to force the application to only fetch data newer than those dates.
    If you omit the file it will fetch all GitLab data

    ```json
    {"branches": "2017-01-17T00:00:00Z",
    "commits": "2017-01-17T00:00:00Z",
    "issues": "2017-01-17T00:00:00Z",
    "milestones": "2017-01-17T00:00:00Z",
    "projects": "2017-01-17T00:00:00Z",
    "users": "2017-01-17T00:00:00Z"}
    ```

5. Run the application

    `tap-gitlab` can be run with:

    ```bash
    tap-gitlab --config config.json [--state state.json]
    ```

---

Copyright &copy; 2018 Stitch
