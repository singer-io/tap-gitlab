# tap-gitlab

This is a [Singer](https://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:
- Pulls raw data from GitLab's [REST API](https://docs.gitlab.com/ee/api/README.html)
- Extracts the following resources from GitLab:
  - [Branches](https://docs.gitlab.com/api/branches/#list-repository-branches)
  - [Commits](https://docs.gitlab.com/api/commits/#list-repository-commits)
  - [Issues](https://docs.gitlab.com/api/issues/#list-issues)
  - [Projects](https://docs.gitlab.com/api/projects/#list-all-projects)
  - [Project milestones](https://docs.gitlab.com/api/milestones/#list-project-milestones)
  - [Users](https://docs.gitlab.com/api/projects/#list-users)
  - [Groups](https://docs.gitlab.com/api/groups/#list-all-groups)
  - [Group Milestones](https://docs.gitlab.com/api/group_milestones/#list-group-milestones)
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
    - Groups to track (space separated)
    - Projects to track (space separated)

    Notes:
    - either groups or projects need to be provided
    - filling in 'groups' but leaving 'projects' empty will sync all group projects.
    - filling in 'projects' but leaving 'groups' empty will sync selected projects.
    - filling in 'groups' and 'groups' will sync selected projects of those groups.
    - filling in 'projects' and 'groups' both will sync all selected projects and all group projects.
    - 'groups' contains space separated list of groups id.
    - 'projects' contains space separated list of projects id.

    ```json
    {
        "private_token": "your-access-token",
        "groups": "myorg mygroup",
        "projects": "myorg/repo-a myorg/repo-b",
        "start_date": "2018-01-01T00:00:00Z"
    }
    ```

4. [Optional] Create the initial state file

    You can provide JSON file that contains a date for the API endpoints
    to force the application to only fetch data newer than those dates.
    If you omit the file it will fetch all GitLab data

    ```json
    {
        "branches": "2017-01-17T00:00:00Z",
        "commits": "2017-01-17T00:00:00Z",
        "issues": "2017-01-17T00:00:00Z",
        "projects": "2017-01-17T00:00:00Z",
        "users": "2017-01-17T00:00:00Z",
        "group_milestones": "2017-01-17T00:00:00Z"
    }
    ```

    Note:
    - currently, groups don't have a date field which can be tracked

5. Run the application

    `tap-gitlab` can be run with:

    For Sync mode:
    ```bash
    > tap-gitlab --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-gitlab --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-gitlab --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

---

Copyright &copy; 2022 Stitch
