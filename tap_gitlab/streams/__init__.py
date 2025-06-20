from tap_gitlab.streams.projects import Projects
from tap_gitlab.streams.branches import Branches
from tap_gitlab.streams.commits import Commits
from tap_gitlab.streams.issues import Issues
from tap_gitlab.streams.project_milestones import ProjectMilestones
from tap_gitlab.streams.group_milestones import GroupMilestones
from tap_gitlab.streams.users import Users
from tap_gitlab.streams.groups import Groups

STREAMS = {
    "projects": Projects,
    "branches": Branches,
    "commits": Commits,
    "issues": Issues,
    "project_milestones": ProjectMilestones,
    "group_milestones": GroupMilestones,
    "users": Users,
    "groups": Groups,
}
